(ns org.replikativ.briefkasten.store-test
  "Tests for datahike mail store operations."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [datahike.api :as d]
            [org.replikativ.briefkasten.schema :as schema]
            [org.replikativ.briefkasten.store :as store]))

;; ============================================================================
;; Fixtures
;; ============================================================================

(def ^:dynamic *conn* nil)

(defn- with-db [f]
  (let [cfg {:store {:backend :memory :id (random-uuid)}
             :schema-flexibility :write
             :keep-history? true}]
    (d/create-database cfg)
    (let [conn (d/connect cfg)]
      (try
        (schema/install-schema! conn)
        ;; Create a test account
        (d/transact conn [(schema/account-entity
                           {:id :test-account
                            :email "test@example.com"
                            :imap-host "imap.example.com"
                            :imap-port 993})])
        (binding [*conn* conn]
          (f))
        (finally
          (d/release conn)
          (d/delete-database cfg))))))

(use-fixtures :each with-db)

;; ============================================================================
;; Folder Tests
;; ============================================================================

(deftest test-get-or-create-folder
  (testing "creates a folder when it doesn't exist"
    (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
      (is (pos? feid))
      (testing "returns same eid on second call"
        (is (= feid (store/get-or-create-folder! *conn* :test-account "INBOX"))))))

  (testing "creates different folders"
    (let [inbox (store/get-or-create-folder! *conn* :test-account "INBOX")
          sent  (store/get-or-create-folder! *conn* :test-account "Sent")]
      (is (not= inbox sent)))))

(deftest test-folder-sync-state
  (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
    (testing "nil sync state before first sync"
      (let [state (store/get-folder-sync-state (d/db *conn*) :test-account "INBOX")]
        (is (nil? (:uidvalidity state)))))

    (testing "update and retrieve sync state"
      (store/update-folder-sync-state! *conn* feid {:uidvalidity 12345 :uidnext 100})
      (let [state (store/get-folder-sync-state (d/db *conn*) :test-account "INBOX")]
        (is (= 12345 (:uidvalidity state)))
        (is (= 100 (:uidnext state)))
        (is (some? (:last-sync state)))))))

;; ============================================================================
;; Message Tests
;; ============================================================================

(deftest test-store-and-query-messages
  (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
    (testing "store messages"
      (store/store-messages! *conn* feid
                             [{:uid 1 :message-id "<msg-1>" :subject "Hello"
                               :from "alice@test.com" :date (java.util.Date.)
                               :flags #{:seen} :body-text "Hello world"}
                              {:uid 2 :message-id "<msg-2>" :subject "Meeting"
                               :from "bob@test.com" :date (java.util.Date.)
                               :flags #{} :body-text "Let's meet"}])
      (is (= #{1 2} (store/get-local-uids (d/db *conn*) feid))))

    (testing "list messages"
      (let [msgs (store/list-messages (d/db *conn*) feid)]
        (is (= 2 (count msgs)))
        (is (every? :mail.message/subject msgs))))

    (testing "message count"
      (is (= 2 (store/message-count (d/db *conn*) feid))))

    (testing "read message by uid"
      (let [msg (store/read-message (d/db *conn*) feid {:uid 1})]
        (is (= "Hello" (:mail.message/subject msg)))
        (is (= "<msg-1>" (:mail.message/message-id msg)))))

    (testing "read message by message-id"
      (let [msg (store/read-message (d/db *conn*) feid {:message-id "<msg-2>"})]
        (is (= "Meeting" (:mail.message/subject msg)))))))

(deftest test-retract-messages
  (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
    (store/store-messages! *conn* feid
                           [{:uid 1 :subject "Keep" :body-text "keep"}
                            {:uid 2 :subject "Delete" :body-text "delete"}
                            {:uid 3 :subject "Also delete" :body-text "also"}])

    (testing "retract specific UIDs"
      (let [n (store/retract-messages! *conn* feid #{2 3})]
        (is (= 2 n))
        (is (= #{1} (store/get-local-uids (d/db *conn*) feid)))))))

(deftest test-retract-folder-messages
  (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
    (store/store-messages! *conn* feid
                           [{:uid 1 :subject "A" :body-text "a"}
                            {:uid 2 :subject "B" :body-text "b"}])

    (testing "retract all folder messages"
      (let [n (store/retract-folder-messages! *conn* feid)]
        (is (= 2 n))
        (is (empty? (store/get-local-uids (d/db *conn*) feid)))))))

(deftest test-flag-operations
  (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
    (store/store-messages! *conn* feid
                           [{:uid 1 :subject "Test" :flags #{:seen}
                             :body-text "test"}])

    (testing "get local flags"
      (let [flags (store/get-local-flags (d/db *conn*) feid)]
        (is (= #{:seen} (get flags 1)))))

    (testing "update flags"
      (store/update-flags! *conn* feid {1 #{:seen :flagged}})
      (let [flags (store/get-local-flags (d/db *conn*) feid)]
        (is (= #{:seen :flagged} (get flags 1)))))

    (testing "remove flags"
      (store/update-flags! *conn* feid {1 #{:flagged}})
      (let [flags (store/get-local-flags (d/db *conn*) feid)]
        (is (= #{:flagged} (get flags 1)))))))

(deftest test-attachment-operations
  (let [feid (store/get-or-create-folder! *conn* :test-account "INBOX")]
    (store/store-messages! *conn* feid
                           [{:uid 1 :subject "With attachment"
                             :body-text "See attached" :has-attachments true
                             :eml-path "eml/INBOX/1.eml"}])
    (let [db (d/db *conn*)
          msg-eid (d/q '[:find ?m .
                         :in $ ?folder ?uid
                         :where
                         [?m :mail.message/folder ?folder]
                         [?m :mail.message/uid ?uid]]
                       db feid 1)]

      (testing "store and query attachments"
        (store/store-attachments! *conn* msg-eid
                                  [{:filename "report.pdf"
                                    :content-type "application/pdf"
                                    :size 12345
                                    :path "attachments/INBOX/1/report.pdf"}
                                   {:filename "image.png"
                                    :content-type "image/png"
                                    :size 6789
                                    :path "attachments/INBOX/1/image.png"}])
        (let [atts (store/get-attachments (d/db *conn*) msg-eid)]
          (is (= 2 (count atts)))
          (is (= #{"report.pdf" "image.png"}
                 (set (map :mail.attachment/filename atts))))))

      (testing "retracting message also retracts attachments"
        (store/retract-messages! *conn* feid #{1})
        (let [db (d/db *conn*)
              atts (d/q '[:find [?a ...]
                          :where [?a :mail.attachment/filename _]]
                        db)]
          (is (empty? atts)))))))

(deftest test-list-folders
  (let [_ (store/get-or-create-folder! *conn* :test-account "INBOX")
        _ (store/get-or-create-folder! *conn* :test-account "Sent")]
    (let [folders (store/list-folders (d/db *conn*) :test-account)]
      (is (= 2 (count folders)))
      (is (= #{"INBOX" "Sent"} (set (map :name folders)))))))
