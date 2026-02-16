(ns org.replikativ.briefkasten.index-test
  "Tests for scriptum fulltext indexing of mail."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [org.replikativ.briefkasten.index :as index]
            [scriptum.core :as sc])
  (:import [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

;; ============================================================================
;; Fixtures
;; ============================================================================

(def ^:dynamic *writer* nil)
(def ^:dynamic *index-path* nil)

(defn- temp-dir []
  (str (Files/createTempDirectory "mail-index-test-"
                                  (make-array FileAttribute 0))))

(defn- delete-dir-recursive [path]
  (let [dir (java.io.File. path)]
    (when (.exists dir)
      (doseq [f (reverse (file-seq dir))]
        (.delete f)))))

(defn- with-index [f]
  (let [path (temp-dir)
        writer (index/create-mail-index! path)]
    (binding [*writer* writer
              *index-path* path]
      (try
        (f)
        (finally
          (sc/close! writer)
          (delete-dir-recursive path))))))

(use-fixtures :each with-index)

;; ============================================================================
;; Tests
;; ============================================================================

(deftest test-index-and-search
  (let [messages [{:uid 1 :message-id "<msg-1>" :subject "Build failure in datahike"
                   :from "ci@example.com" :to "dev@example.com"
                   :body-text "The datahike build failed on commit abc123"
                   :date (java.util.Date.)}
                  {:uid 2 :message-id "<msg-2>" :subject "Conference deadline reminder"
                   :from "events@conf.org" :to "christian@example.com"
                   :body-text "Don't forget the paper submission deadline is Friday"
                   :date (java.util.Date.)}
                  {:uid 3 :message-id "<msg-3>" :subject "Meeting notes"
                   :from "alice@example.com" :to "team@example.com"
                   :body-text "Here are the notes from today's standup meeting"
                   :date (java.util.Date.)}]]
    (index/index-messages! *writer* :testaccount "INBOX" messages)
    (index/commit! *writer* "test commit")

    (testing "search by subject keyword"
      (let [results (index/search *writer* "datahike" :account :testaccount)]
        (is (= 1 (count results)))
        (is (= 1 (:uid (first results))))
        (is (= "INBOX" (:folder (first results))))))

    (testing "search by body content"
      (let [results (index/search *writer* "deadline" :account :testaccount)]
        (is (= 1 (count results)))
        (is (= 2 (:uid (first results))))))

    (testing "search by sender"
      (let [results (index/search *writer* "alice" :account :testaccount)]
        (is (= 1 (count results)))
        (is (= 3 (:uid (first results))))))

    (testing "search returns score"
      (let [results (index/search *writer* "meeting" :account :testaccount)]
        (is (every? #(pos? (:score %)) results))))

    (testing "search with folder filter"
      (let [results (index/search *writer* "datahike"
                                  :account :testaccount :folder "INBOX")]
        (is (= 1 (count results))))
      (let [results (index/search *writer* "datahike"
                                  :account :testaccount :folder "Sent")]
        (is (empty? results))))

    (testing "search with limit"
      (let [results (index/search *writer* "example" :account :testaccount :limit 2)]
        (is (<= (count results) 2))))

    (testing "empty query returns nil"
      (is (nil? (index/search *writer* "" :account :testaccount)))
      (is (nil? (index/search *writer* nil :account :testaccount))))))

(deftest test-delete-messages
  (index/index-messages! *writer* :test "INBOX"
                         [{:uid 1 :subject "Keep" :body-text "keep this"}
                          {:uid 2 :subject "Delete" :body-text "delete this"}])
  (index/commit! *writer*)

  (testing "delete specific messages"
    (index/delete-messages! *writer* :test "INBOX" [2])
    (index/commit! *writer*)

    (is (= 1 (sc/num-docs *writer*)))
    (let [results (index/search *writer* "keep" :account :test)]
      (is (= 1 (count results))))
    (let [results (index/search *writer* "delete" :account :test)]
      (is (empty? results)))))

(deftest test-delete-folder
  (index/index-messages! *writer* :test "INBOX"
                         [{:uid 1 :subject "Inbox msg" :body-text "inbox"}])
  (index/index-messages! *writer* :test "Sent"
                         [{:uid 1 :subject "Sent msg" :body-text "sent"}])
  (index/commit! *writer*)

  (testing "delete all docs for a folder"
    (index/delete-folder! *writer* :test "INBOX")
    (index/commit! *writer*)

    (is (= 1 (sc/num-docs *writer*)))
    (let [results (index/search *writer* "sent" :account :test)]
      (is (= 1 (count results))))))

(deftest test-commit-with-metadata
  (index/index-messages! *writer* :test "INBOX"
                         [{:uid 1 :subject "Test" :body-text "test"}])

  (testing "commit with datahike tx metadata"
    (let [gen (index/commit! *writer* "sync" 536870915)]
      (is (some? gen))
      (let [found (sc/find-generation *writer* "datahike/tx" "536870915")]
        (is (some? found))
        (is (contains? found :generation))))))

(deftest test-reindex-updates-doc
  (index/index-messages! *writer* :test "INBOX"
                         [{:uid 1 :subject "Original" :body-text "original text"}])
  (index/commit! *writer*)

  (testing "re-indexing same UID replaces the document"
    (index/index-messages! *writer* :test "INBOX"
                           [{:uid 1 :subject "Updated" :body-text "updated text"}])
    (index/commit! *writer*)

    (is (= 1 (sc/num-docs *writer*)))
    (let [results (index/search *writer* "updated" :account :test)]
      (is (= 1 (count results)))
      (is (= "Updated" (:subject (first results)))))))
