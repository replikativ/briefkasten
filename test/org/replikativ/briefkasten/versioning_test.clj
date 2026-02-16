(ns org.replikativ.briefkasten.versioning-test
  "Integration tests for yggdrasil pullback over datahike × scriptum."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [org.replikativ.briefkasten.schema :as schema]
            [org.replikativ.briefkasten.index :as index]
            [org.replikativ.briefkasten.versioning :as ver]
            [yggdrasil.protocols :as p]
            [yggdrasil.composite :as yc]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; ============================================================================
;; Test fixtures (same pattern as sync_test)
;; ============================================================================

(def ^:dynamic *conn* nil)
(def ^:dynamic *writer* nil)
(def ^:dynamic *data-path* nil)
(def ^:dynamic *composite* nil)

(defn- temp-dir []
  (str (Files/createTempDirectory "versioning-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- with-db-and-index [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}
        data-path (temp-dir)
        index-path (str data-path "/scriptum")]
    (d/create-database cfg)
    (let [conn (d/connect cfg)
          writer (index/create-mail-index! index-path)]
      (try
        (schema/install-schema! conn)
        (d/transact conn [(schema/account-entity
                           {:id :test
                            :email "test@example.com"
                            :imap-host "imap.example.com"
                            :imap-port 993})])
        (let [composite (ver/wrap-account {:id :test
                                           :conn conn
                                           :writer writer
                                           :data-path data-path})]
          (binding [*conn* conn
                    *writer* writer
                    *data-path* data-path
                    *composite* composite]
            (f)))
        (finally
          (sc/close! writer)
          (delete-dir-recursive data-path)
          (d/release conn)
          (d/delete-database cfg))))))

(use-fixtures :each with-db-and-index)

;; ============================================================================
;; Tests
;; ============================================================================

(deftest test-composite-wraps-correctly
  (testing "system-id reflects account name"
    (is (= "briefkasten-test" (p/system-id *composite*))))
  (testing "system-type is :composite"
    (is (= :composite (p/system-type *composite*))))
  (testing "logical branch is :main"
    (is (= :main (p/current-branch *composite*))))
  (testing "two sub-systems present"
    (is (= 2 (count (:systems *composite*))))))

(deftest test-sub-systems-accessible
  (testing "datahike sub-system"
    (let [dh (yc/get-subsystem *composite* "briefkasten-dh-test")]
      (is (some? dh))
      (is (= :datahike (p/system-type dh)))))
  (testing "scriptum sub-system"
    (let [sc (yc/get-subsystem *composite* "briefkasten-sc-test")]
      (is (some? sc))
      (is (= :scriptum (p/system-type sc))))))

(deftest test-snapshot-id-deterministic
  (testing "snapshot-id is a string"
    (is (string? (p/snapshot-id *composite*))))
  (testing "snapshot-id is stable across calls"
    (is (= (p/snapshot-id *composite*)
           (p/snapshot-id *composite*)))))

(deftest test-commit-updates-composite
  (testing "commit! changes composite snapshot-id and records parentage"
    (let [snap-before (p/snapshot-id *composite*)
          c2 (p/commit! *composite* "test commit")
          snap-after (p/snapshot-id c2)]
      (is (not= snap-before snap-after))
      (is (= #{snap-before} (p/parent-ids c2))))))

(deftest test-history-chain
  (testing "history tracks commits through composite parent chain"
    (let [c1 (p/commit! *composite* "first")
          c2 (p/commit! c1 "second")
          hist (p/history c2)]
      ;; initial + 2 commits = 3 entries
      (is (= 3 (count hist)))
      (is (= (p/snapshot-id c2) (first hist))))))

(deftest test-snapshot-meta
  (testing "snapshot-meta includes sub-snapshots map"
    (let [c2 (p/commit! *composite* "meta test")
          snap (p/snapshot-id c2)
          m (p/snapshot-meta c2 snap)]
      (is (= "meta test" (:message m)))
      (is (map? (:sub-snapshots m)))
      (is (= 2 (count (:sub-snapshots m)))))))
