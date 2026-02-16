(ns org.replikativ.briefkasten.sync-test
  "Tests for the mail sync engine with mocked IMAP."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [org.replikativ.briefkasten.schema :as schema]
            [org.replikativ.briefkasten.store :as store]
            [org.replikativ.briefkasten.index :as index]
            [org.replikativ.briefkasten.sync :as sync]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; ============================================================================
;; Helpers
;; ============================================================================

(def ^:dynamic *conn* nil)
(def ^:dynamic *writer* nil)
(def ^:dynamic *index-path* nil)

(defn- temp-dir []
  (str (Files/createTempDirectory "mail-sync-test-"
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
        index-path (temp-dir)]
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
        (binding [*conn* conn
                  *writer* writer
                  *index-path* index-path]
          (f))
        (finally
          (sc/close! writer)
          (delete-dir-recursive index-path)
          (d/release conn)
          (d/delete-database cfg))))))

(use-fixtures :each with-db-and-index)

;; ============================================================================
;; Mock IMAP
;; ============================================================================

(defn- make-mock-imap
  "Create a mock IMAP 'store' backed by atoms.
   messages is {folder-name {uid message-map}}
   folder-state is {folder-name {:uidvalidity :uidnext :message-count}}"
  [messages-atom folder-state-atom]
  ;; We override the imap functions via with-redefs in tests
  {:messages messages-atom
   :folder-state folder-state-atom})

(defmacro with-mock-imap
  "Execute body with IMAP functions redirected to mock data."
  [mock & body]
  `(let [mock# ~mock
         msgs# (:messages mock#)
         fstate# (:folder-state mock#)]
     (with-redefs [org.replikativ.briefkasten.imap/fetch-folder-state
                   (fn [~'_store folder-name#]
                     (get @fstate# folder-name#))

                   org.replikativ.briefkasten.imap/fetch-uids
                   (fn [~'_store folder-name#]
                     (set (keys (get @msgs# folder-name#))))

                   org.replikativ.briefkasten.imap/fetch-messages
                   (fn [~'_store folder-name# uids# & {:keys [~'data-path]}]
                     (let [folder-msgs# (get @msgs# folder-name#)]
                       (mapv #(get folder-msgs# %) uids#)))

                   org.replikativ.briefkasten.imap/fetch-all-messages
                   (fn [~'_store folder-name# & {:keys [~'data-path ~'batch-fn]}]
                     (let [all-msgs# (vec (vals (get @msgs# folder-name#)))]
                       (when ~'batch-fn (~'batch-fn all-msgs#))
                       (count all-msgs#)))

                   org.replikativ.briefkasten.imap/fetch-flags
                   (fn [~'_store folder-name# uids#]
                     (let [folder-msgs# (get @msgs# folder-name#)]
                       (into {}
                             (map (fn [uid#]
                                    [uid# (or (:flags (get folder-msgs# uid#)) #{})]))
                             uids#)))

                   org.replikativ.briefkasten.imap/list-folders
                   (fn [~'_store]
                     (vec (keys @msgs#)))]
       ~@body)))

;; ============================================================================
;; Pure function tests
;; ============================================================================

(deftest test-detect-changes
  (testing "detects new, deleted, and existing UIDs"
    (let [result (sync/detect-changes #{1 2 3 4} #{2 3 5})]
      (is (= #{1 4} (:new result)))
      (is (= #{5} (:deleted result)))
      (is (= #{2 3} (:existing result)))))

  (testing "empty sets"
    (let [result (sync/detect-changes #{} #{})]
      (is (empty? (:new result)))
      (is (empty? (:deleted result)))
      (is (empty? (:existing result)))))

  (testing "all new"
    (let [result (sync/detect-changes #{1 2 3} #{})]
      (is (= #{1 2 3} (:new result)))
      (is (empty? (:deleted result))))))

;; ============================================================================
;; Integration tests with mock IMAP
;; ============================================================================

(defn- make-msg [uid subject body & {:keys [flags] :or {flags #{}}}]
  {:uid uid :message-id (str "<msg-" uid ">") :subject subject
   :from "sender@test.com" :to "test@example.com"
   :date (java.util.Date.) :flags flags :body-text body})

(deftest test-initial-sync
  (let [mock (make-mock-imap
              (atom {"INBOX" {1 (make-msg 1 "First" "First message")
                              2 (make-msg 2 "Second" "Second message")}})
              (atom {"INBOX" {:uidvalidity 100 :uidnext 3 :message-count 2}}))]
    (with-mock-imap mock
      (let [result (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)]
        (testing "initial sync (first time, single pass)"
          (is (= :initial (:type result)))
          (is (= 2 (:stored result))))

        (testing "messages stored in datahike"
          (let [db (d/db *conn*)
                feid (store/get-folder-eid db :test "INBOX")]
            (is (= 2 (store/message-count db feid)))
            (is (= #{1 2} (store/get-local-uids db feid)))))

        (testing "messages indexed in scriptum"
          (let [results (index/search *writer* "first" :account :test)]
            (is (= 1 (count results)))
            (is (= 1 (:uid (first results))))))

        (testing "sync state updated"
          (let [state (store/get-folder-sync-state (d/db *conn*) :test "INBOX")]
            (is (= 100 (:uidvalidity state)))
            (is (= 3 (:uidnext state)))))))))

(deftest test-incremental-new-messages
  (let [msgs (atom {"INBOX" {1 (make-msg 1 "Existing" "Existing msg")}})
        fstate (atom {"INBOX" {:uidvalidity 100 :uidnext 2 :message-count 1}})
        mock (make-mock-imap msgs fstate)]
    (with-mock-imap mock
      ;; Initial sync
      (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)

      ;; Add new messages on "server"
      (swap! msgs assoc-in ["INBOX" 2] (make-msg 2 "New" "A new message"))
      (swap! msgs assoc-in ["INBOX" 3] (make-msg 3 "Also new" "Another new one"))
      (swap! fstate assoc "INBOX" {:uidvalidity 100 :uidnext 4 :message-count 3})

      ;; Second sync
      (let [result (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)]
        (testing "only new messages fetched"
          (is (= :incremental (:type result)))
          (is (= 2 (:new result)))
          (is (= 0 (:deleted result))))

        (testing "all 3 messages in cache"
          (let [db (d/db *conn*)
                feid (store/get-folder-eid db :test "INBOX")]
            (is (= 3 (store/message-count db feid)))))))))

(deftest test-incremental-deletions
  (let [msgs (atom {"INBOX" {1 (make-msg 1 "One" "one")
                             2 (make-msg 2 "Two" "two")
                             3 (make-msg 3 "Three" "three")}})
        fstate (atom {"INBOX" {:uidvalidity 100 :uidnext 4 :message-count 3}})
        mock (make-mock-imap msgs fstate)]
    (with-mock-imap mock
      ;; Initial sync
      (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)

      ;; Delete messages on "server"
      (swap! msgs update "INBOX" dissoc 2)
      (swap! fstate assoc "INBOX" {:uidvalidity 100 :uidnext 4 :message-count 2})

      ;; Second sync
      (let [result (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)]
        (testing "deletion detected"
          (is (= 1 (:deleted result)))
          (is (= 0 (:new result))))

        (testing "message removed from datahike"
          (let [db (d/db *conn*)
                feid (store/get-folder-eid db :test "INBOX")]
            (is (= #{1 3} (store/get-local-uids db feid)))))

        (testing "message removed from scriptum"
          (let [results (index/search *writer* "two" :account :test)]
            (is (empty? results))))))))

(deftest test-flag-sync
  (let [msgs (atom {"INBOX" {1 (make-msg 1 "Test" "test" :flags #{:seen})}})
        fstate (atom {"INBOX" {:uidvalidity 100 :uidnext 2 :message-count 1}})
        mock (make-mock-imap msgs fstate)]
    (with-mock-imap mock
      ;; Initial sync
      (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)

      ;; Change flags on "server"
      (swap! msgs assoc-in ["INBOX" 1 :flags] #{:seen :flagged})

      ;; Second sync
      (let [result (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)]
        (testing "flag change detected"
          (is (= 1 (:flags-updated result))))

        (testing "flags updated in datahike"
          (let [db (d/db *conn*)
                feid (store/get-folder-eid db :test "INBOX")
                flags (store/get-local-flags db feid)]
            (is (= #{:seen :flagged} (get flags 1)))))))))

(deftest test-uidvalidity-change-triggers-full-resync
  (let [msgs (atom {"INBOX" {1 (make-msg 1 "Old" "old message")}})
        fstate (atom {"INBOX" {:uidvalidity 100 :uidnext 2 :message-count 1}})
        mock (make-mock-imap msgs fstate)]
    (with-mock-imap mock
      ;; Initial sync
      (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)

      ;; UIDVALIDITY changes (server rebuilt mailbox)
      (reset! msgs {"INBOX" {1 (make-msg 1 "New UID 1" "completely new")
                             2 (make-msg 2 "New UID 2" "also new")}})
      (reset! fstate {"INBOX" {:uidvalidity 200 :uidnext 3 :message-count 2}})

      ;; Sync again - should detect UIDVALIDITY change
      (let [result (sync/sync-folder! nil *conn* *writer* :test "INBOX" nil)]
        (testing "full resync triggered"
          (is (= :full-resync (:type result)))
          (is (= 2 (:stored result))))

        (testing "new UIDVALIDITY stored"
          (let [state (store/get-folder-sync-state (d/db *conn*) :test "INBOX")]
            (is (= 200 (:uidvalidity state)))))))))

(deftest test-sync-account
  (let [msgs (atom {"INBOX" {1 (make-msg 1 "Inbox" "inbox msg")}
                    "Sent"  {1 (make-msg 1 "Sent" "sent msg")}})
        fstate (atom {"INBOX" {:uidvalidity 100 :uidnext 2 :message-count 1}
                      "Sent"  {:uidvalidity 200 :uidnext 2 :message-count 1}})
        mock (make-mock-imap msgs fstate)]
    (with-mock-imap mock
      (let [results (sync/sync-account! nil *conn* *writer* :test :data-path nil)]
        (testing "both folders synced"
          (is (contains? results "INBOX"))
          (is (contains? results "Sent"))
          (is (= 1 (:stored (get results "INBOX"))))
          (is (= 1 (:stored (get results "Sent")))))))))

(deftest test-sync-specific-folders
  (let [msgs (atom {"INBOX" {1 (make-msg 1 "Inbox" "inbox msg")}
                    "Sent"  {1 (make-msg 1 "Sent" "sent msg")}
                    "Trash" {1 (make-msg 1 "Trash" "trash msg")}})
        fstate (atom {"INBOX" {:uidvalidity 100 :uidnext 2 :message-count 1}
                      "Sent"  {:uidvalidity 200 :uidnext 2 :message-count 1}
                      "Trash" {:uidvalidity 300 :uidnext 2 :message-count 1}})
        mock (make-mock-imap msgs fstate)]
    (with-mock-imap mock
      (let [results (sync/sync-account! nil *conn* *writer* :test
                                        :folders ["INBOX" "Sent"]
                                        :data-path nil)]
        (testing "only requested folders synced"
          (is (= 2 (count results)))
          (is (contains? results "INBOX"))
          (is (contains? results "Sent"))
          (is (not (contains? results "Trash"))))))))
