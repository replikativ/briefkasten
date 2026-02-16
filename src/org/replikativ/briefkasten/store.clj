(ns org.replikativ.briefkasten.store
  "Datahike CRUD operations for local mail cache.

   All queries operate on the datahike DB value (immutable snapshot).
   All mutations take a datahike connection."
  (:require [datahike.api :as d]
            [clojure.set]
            [org.replikativ.briefkasten.schema :as schema]))

;; ============================================================================
;; Folder Operations
;; ============================================================================

(defn get-or-create-folder!
  "Get the entity ID for a folder, creating it if necessary.
   Returns the folder entity ID."
  [conn account-id folder-name]
  (let [db (d/db conn)
        account-ref [:mail.account/id account-id]
        folder-eid (d/q '[:find ?f .
                          :in $ ?acct ?fname
                          :where
                          [?a :mail.account/id ?acct]
                          [?f :mail.folder/account ?a]
                          [?f :mail.folder/name ?fname]]
                        db account-id folder-name)]
    (or folder-eid
        (let [tempid "new-folder"
              tx-data [(assoc (schema/folder-entity
                               {:account-ref account-ref
                                :name folder-name})
                              :db/id tempid)]
              result (d/transact conn tx-data)]
          (get (:tempids result) tempid)))))

(defn get-folder-eid
  "Get the entity ID for a folder. Returns nil if not found."
  [db account-id folder-name]
  (d/q '[:find ?f .
         :in $ ?acct ?fname
         :where
         [?a :mail.account/id ?acct]
         [?f :mail.folder/account ?a]
         [?f :mail.folder/name ?fname]]
       db account-id folder-name))

(defn get-folder-sync-state
  "Returns {:uidvalidity :uidnext :last-sync} for a folder, or nil."
  [db account-id folder-name]
  (let [result (d/q '[:find (pull ?f [:mail.folder/uidvalidity
                                      :mail.folder/uidnext
                                      :mail.folder/last-sync]) .
                      :in $ ?acct ?fname
                      :where
                      [?a :mail.account/id ?acct]
                      [?f :mail.folder/account ?a]
                      [?f :mail.folder/name ?fname]]
                    db account-id folder-name)]
    (when result
      {:uidvalidity (:mail.folder/uidvalidity result)
       :uidnext     (:mail.folder/uidnext result)
       :last-sync   (:mail.folder/last-sync result)})))

(defn update-folder-sync-state!
  "Update sync state for a folder."
  [conn folder-eid {:keys [uidvalidity uidnext]}]
  (d/transact conn
              [(cond-> {:db/id folder-eid
                        :mail.folder/last-sync (java.util.Date.)}
                 uidvalidity (assoc :mail.folder/uidvalidity uidvalidity)
                 uidnext     (assoc :mail.folder/uidnext uidnext))]))

;; ============================================================================
;; Message Operations
;; ============================================================================

(defn store-messages!
  "Batch transact messages for a folder.
   messages is a seq of maps matching schema/message-entity args."
  [conn folder-eid messages]
  (when (seq messages)
    (let [entities (mapv #(schema/message-entity (assoc % :folder-eid folder-eid))
                         messages)]
      (d/transact conn entities))))

(defn store-messages-with-attachments!
  "Transact messages and their attachments in a single transaction.
   Uses tempids to link attachments to their parent messages."
  [conn folder-eid messages]
  (when (seq messages)
    (let [tx-data (into []
                        (mapcat
                         (fn [msg]
                           (let [tempid (str "msg-" (:uid msg))
                                 msg-entity (assoc (schema/message-entity
                                                    (assoc msg :folder-eid folder-eid))
                                                   :db/id tempid)
                                 att-entities (when (seq (:attachments msg))
                                                (mapv #(schema/attachment-entity
                                                        (assoc % :message-eid tempid))
                                                      (:attachments msg)))]
                             (cons msg-entity att-entities))))
                        messages)]
      (d/transact conn tx-data))))

(defn retract-messages!
  "Retract messages by UIDs (for server-side deletions).
   Also retracts any child attachment entities.
   Returns the count of retracted message entities."
  [conn folder-eid uids]
  (when (seq uids)
    (let [db (d/db conn)
          msg-eids (d/q '[:find [?m ...]
                          :in $ ?folder [?uid ...]
                          :where
                          [?m :mail.message/folder ?folder]
                          [?m :mail.message/uid ?uid]]
                        db folder-eid (vec uids))]
      (when (seq msg-eids)
        ;; Find child attachments
        (let [att-eids (d/q '[:find [?a ...]
                              :in $ [?m ...]
                              :where
                              [?a :mail.attachment/message ?m]]
                            db (vec msg-eids))
              all-retracts (into (mapv (fn [e] [:db/retractEntity e]) att-eids)
                                 (mapv (fn [e] [:db/retractEntity e]) msg-eids))]
          (d/transact conn all-retracts)
          (count msg-eids))))))

(defn retract-folder-messages!
  "Retract ALL messages in a folder (for UIDVALIDITY change / full resync).
   Also retracts child attachment entities.
   Returns count of retracted message entities."
  [conn folder-eid]
  (let [db (d/db conn)
        msg-eids (d/q '[:find [?m ...]
                        :in $ ?folder
                        :where
                        [?m :mail.message/folder ?folder]]
                      db folder-eid)]
    (when (seq msg-eids)
      (let [att-eids (d/q '[:find [?a ...]
                            :in $ [?m ...]
                            :where
                            [?a :mail.attachment/message ?m]]
                          db (vec msg-eids))
            all-retracts (into (mapv (fn [e] [:db/retractEntity e]) att-eids)
                               (mapv (fn [e] [:db/retractEntity e]) msg-eids))]
        (d/transact conn all-retracts)
        (count msg-eids)))))

;; ============================================================================
;; Attachment Operations
;; ============================================================================

(defn store-attachments!
  "Batch transact attachment entities linked to a message.
   message-eid is the datahike entity ID of the parent message.
   attachments is a seq of {:filename :content-type :size :path}."
  [conn message-eid attachments]
  (when (seq attachments)
    (let [entities (mapv #(schema/attachment-entity (assoc % :message-eid message-eid))
                         attachments)]
      (d/transact conn entities))))

(defn get-attachments
  "Query attachments for a message entity ID.
   Returns a vec of {:filename :content-type :size :path}."
  [db message-eid]
  (let [results (d/q '[:find [(pull ?a [:mail.attachment/filename
                                        :mail.attachment/content-type
                                        :mail.attachment/size
                                        :mail.attachment/path]) ...]
                       :in $ ?msg
                       :where
                       [?a :mail.attachment/message ?msg]]
                     db message-eid)]
    (vec results)))

(defn get-local-uids
  "Query all UIDs for a folder. Returns a set of longs."
  [db folder-eid]
  (set (d/q '[:find [?uid ...]
              :in $ ?folder
              :where
              [?m :mail.message/folder ?folder]
              [?m :mail.message/uid ?uid]]
            db folder-eid)))

(defn get-local-flags
  "Query UID→flags map for a folder.
   Returns {uid #{:seen :flagged ...}}."
  [db folder-eid]
  (let [results (d/q '[:find ?uid ?flags
                       :in $ ?folder
                       :where
                       [?m :mail.message/folder ?folder]
                       [?m :mail.message/uid ?uid]
                       [?m :mail.message/flags ?flags]]
                     db folder-eid)]
    (reduce (fn [acc [uid flag]]
              (update acc uid (fnil conj #{}) flag))
            {}
            results)))

(defn update-flags!
  "Update flags for specific UIDs.
   uid-flags is a map of {uid #{:seen :flagged ...}}."
  [conn folder-eid uid-flags]
  (when (seq uid-flags)
    (let [db (d/db conn)
          txs (for [[uid new-flags] uid-flags
                    :let [eid (d/q '[:find ?m .
                                     :in $ ?folder ?uid
                                     :where
                                     [?m :mail.message/folder ?folder]
                                     [?m :mail.message/uid ?uid]]
                                   db folder-eid uid)]
                    :when eid
                    :let [old-flags (set (d/q '[:find [?f ...]
                                                :in $ ?m
                                                :where [?m :mail.message/flags ?f]]
                                              db eid))
                          to-retract (clojure.set/difference old-flags new-flags)
                          to-add (clojure.set/difference new-flags old-flags)]]
                (concat
                 (map (fn [f] [:db/retract eid :mail.message/flags f]) to-retract)
                 (map (fn [f] [:db/add eid :mail.message/flags f]) to-add)))]
      (let [flat-txs (vec (mapcat identity txs))]
        (when (seq flat-txs)
          (d/transact conn flat-txs))))))

;; ============================================================================
;; Query Operations
;; ============================================================================

(defn list-messages
  "List messages in a folder from the local cache.
   Options: :limit :offset :order (default newest-first)
   Returns vector of message maps."
  [db folder-eid & {:keys [limit] :or {limit 50}}]
  (let [results (d/q '[:find [(pull ?m [:mail.message/uid
                                        :mail.message/message-id
                                        :mail.message/subject
                                        :mail.message/from
                                        :mail.message/date
                                        :mail.message/flags
                                        :mail.message/size]) ...]
                       :in $ ?folder
                       :where
                       [?m :mail.message/folder ?folder]]
                     db folder-eid)]
    (->> results
         (sort-by #(or (:mail.message/date %) (java.util.Date. 0))
                  #(compare %2 %1))
         (take limit)
         vec)))

(defn read-message
  "Get a full message by UID or Message-ID.
   lookup is {:uid 42} or {:message-id \"<...>\"}."
  [db folder-eid {:keys [uid message-id]}]
  (let [eid (cond
              uid
              (d/q '[:find ?m .
                     :in $ ?folder ?uid
                     :where
                     [?m :mail.message/folder ?folder]
                     [?m :mail.message/uid ?uid]]
                   db folder-eid uid)

              message-id
              (d/q '[:find ?m .
                     :in $ ?folder ?mid
                     :where
                     [?m :mail.message/folder ?folder]
                     [?m :mail.message/message-id ?mid]]
                   db folder-eid message-id))]
    (when eid
      (d/pull db '[*] eid))))

(defn message-count
  "Count messages in a folder."
  [db folder-eid]
  (or (d/q '[:find (count ?m) .
             :in $ ?folder
             :where
             [?m :mail.message/folder ?folder]]
           db folder-eid)
      0))

(defn list-folders
  "List all synced folders for an account with message counts."
  [db account-id]
  (let [folders (d/q '[:find ?f ?name ?uidvalidity ?last-sync
                       :in $ ?acct
                       :where
                       [?a :mail.account/id ?acct]
                       [?f :mail.folder/account ?a]
                       [?f :mail.folder/name ?name]
                       [(get-else $ ?f :mail.folder/uidvalidity -1) ?uidvalidity]
                       [(get-else $ ?f :mail.folder/last-sync #inst "1970-01-01") ?last-sync]]
                     db account-id)]
    (mapv (fn [[feid name uidvalidity last-sync]]
            {:folder-eid   feid
             :name         name
             :uidvalidity  (when (pos? uidvalidity) uidvalidity)
             :last-sync    (when (not= last-sync #inst "1970-01-01") last-sync)
             :message-count (message-count db feid)})
          folders)))
