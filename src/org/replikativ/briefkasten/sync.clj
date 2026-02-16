(ns org.replikativ.briefkasten.sync
  "Sync engine: keeps local datahike + scriptum mirror consistent with IMAP.

   Server-authoritative: IMAP is source of truth. All reads go through local
   cache, all mutations go through IMAP first."
  (:require [org.replikativ.briefkasten.store :as store]
            [org.replikativ.briefkasten.imap :as imap]
            [org.replikativ.briefkasten.index :as index]
            [datahike.api :as d]
            [clojure.set :as set]
            [taoensso.trove :as trove]))

;; ============================================================================
;; Change Detection (pure function)
;; ============================================================================

(defn detect-changes
  "Compute the diff between remote and local UID sets.
   Returns {:new #{uids} :deleted #{uids} :existing #{uids}}."
  [remote-uids local-uids]
  {:new      (set/difference remote-uids local-uids)
   :deleted  (set/difference local-uids remote-uids)
   :existing (set/intersection remote-uids local-uids)})

;; ============================================================================
;; Batch Store+Index Helper
;; ============================================================================

(defn- store-and-index-batch!
  "Store a batch of parsed messages in datahike + scriptum.
   Messages and their attachments are transacted together in a single
   transaction to reduce overhead."
  [conn folder-eid writer account-id folder-name messages]
  (let [valid (vec (remove :error messages))
        errors (count (filter :error messages))]
    (when (seq valid)
      (store/store-messages-with-attachments! conn folder-eid valid)
      (index/index-messages! writer account-id folder-name valid)
      (index/commit! writer (str "batch " folder-name)))
    {:stored (count valid) :errors errors}))

;; ============================================================================
;; Apply Changes (side-effecting)
;; ============================================================================

(defn- apply-new-messages!
  "Fetch and store new messages by UID, in batches.
   Fetches and stores one batch at a time to bound memory usage."
  [imap-store conn folder-eid writer account-id folder-name new-uids data-path]
  (when (seq new-uids)
    (let [total (count new-uids)
          batch-size 50
          uid-batches (partition-all batch-size (sort new-uids))]
      (trove/log! {:level :info, :id ::fetch-new
                    :data {:folder folder-name, :count total}
                    :msg (format "[%s] %d new messages to fetch by UID" folder-name total)})
      (loop [remaining uid-batches
             total-stored 0
             total-errors 0]
        (if-let [uid-batch (first remaining)]
          (let [messages (imap/fetch-messages imap-store folder-name
                                              (set uid-batch) :data-path data-path)
                {:keys [stored errors]} (store-and-index-batch!
                                         conn folder-eid writer
                                         account-id folder-name messages)
                done (+ total-stored stored)
                errs (+ total-errors errors)]
            (trove/log! {:level :info, :id ::store-progress
                          :data {:folder folder-name, :done done, :total total}
                          :msg (format "[%s] stored %d/%d (%.0f%%)"
                                       folder-name done total
                                       (* 100.0 (/ done (max 1 total))))})
            (recur (rest remaining) done errs))
          {:fetched total :stored total-stored :errors total-errors})))))

(defn- apply-deletions!
  "Remove locally-cached messages that were deleted on the server."
  [conn folder-eid writer account-id folder-name deleted-uids]
  (when (seq deleted-uids)
    (let [retracted (store/retract-messages! conn folder-eid deleted-uids)]
      (index/delete-messages! writer account-id folder-name deleted-uids)
      {:retracted (or retracted 0)})))

(defn- apply-flag-updates!
  "Sync flag changes from server to local cache."
  [imap-store conn folder-eid folder-name existing-uids]
  (when (seq existing-uids)
    (let [db (d/db conn)
          local-flags (store/get-local-flags db folder-eid)
          remote-flags (imap/fetch-flags imap-store folder-name existing-uids)
          changed (into {}
                        (filter (fn [[uid rf]]
                                  (not= rf (get local-flags uid))))
                        remote-flags)]
      (when (seq changed)
        (store/update-flags! conn folder-eid changed))
      {:flags-updated (count changed)})))

;; ============================================================================
;; Sync Operations
;; ============================================================================

(defn initial-sync!
  "Single-pass sync for empty local state: iterate all messages once,
   parse and store in batches. No separate UID fetch needed.
   Downloads full MIME content, saves .eml + attachments to disk.
   Streams batches directly to datahike+scriptum for visible progress."
  [imap-store conn folder-eid writer account-id folder-name data-path]
  (trove/log! {:level :info, :id ::initial-sync
                :data {:folder folder-name}
                :msg (format "[%s] initial sync (full MIME, streaming)..." folder-name)})
  (let [total-stored (atom 0)
        total-errors (atom 0)
        batch-fn (fn [parsed-batch]
                   (let [{:keys [stored errors]} (store-and-index-batch!
                                                  conn folder-eid writer
                                                  account-id folder-name
                                                  (vec parsed-batch))]
                     (swap! total-stored + stored)
                     (swap! total-errors + errors)
                     (trove/log! {:level :info, :id ::batch-progress
                                   :data {:folder folder-name, :total-stored @total-stored
                                          :batch-stored stored, :batch-errors errors}
                                   :msg (format "[%s] stored %d (batch: %d stored, %d errors)"
                                                folder-name @total-stored stored errors)})))
        fetched (imap/fetch-all-messages imap-store folder-name
                                         :data-path data-path
                                         :batch-fn batch-fn)]
    {:type    :initial
     :stored  @total-stored
     :errors  @total-errors
     :fetched fetched}))

(defn full-resync!
  "Full resync: retract all local, re-fetch everything in single pass.
   Called when UIDVALIDITY changes."
  [imap-store conn folder-eid writer account-id folder-name data-path]
  (let [retracted (store/retract-folder-messages! conn folder-eid)]
    (index/delete-folder! writer account-id folder-name)
    (let [result (initial-sync! imap-store conn folder-eid writer
                                account-id folder-name data-path)]
      (assoc result
             :type :full-resync
             :retracted (or retracted 0)))))

(defn incremental-sync!
  "Incremental sync: two-pass — fetch UIDs to diff, then only fetch changes."
  [imap-store conn folder-eid writer account-id folder-name data-path]
  (let [db (d/db conn)
        local-uids (store/get-local-uids db folder-eid)]
    ;; If local is empty, use single-pass initial sync instead
    (if (empty? local-uids)
      (initial-sync! imap-store conn folder-eid writer account-id folder-name data-path)
      (let [remote-uids (imap/fetch-uids imap-store folder-name)
            {:keys [new deleted existing]} (detect-changes remote-uids local-uids)
            _ (trove/log! {:level :info, :id ::incremental-diff
                            :data {:folder folder-name, :new (count new)
                                   :deleted (count deleted), :existing (count existing)}
                            :msg (format "[%s] diff: %d new, %d deleted, %d existing"
                                         folder-name (count new) (count deleted) (count existing))})
            new-result (apply-new-messages! imap-store conn folder-eid writer
                                            account-id folder-name new data-path)
            del-result (apply-deletions! conn folder-eid writer
                                         account-id folder-name deleted)
            flag-result (apply-flag-updates! imap-store conn folder-eid
                                             folder-name existing)]
        {:type          :incremental
         :new           (or (:stored new-result) 0)
         :deleted       (or (:retracted del-result) 0)
         :flags-updated (or (:flags-updated flag-result) 0)
         :fetch-errors  (or (:errors new-result) 0)}))))

(defn sync-folder!
  "Sync a single IMAP folder.
   Compares UIDVALIDITY to decide between incremental and full resync."
  [imap-store conn writer account-id folder-name data-path]
  (let [folder-eid (store/get-or-create-folder! conn account-id folder-name)
        remote-state (imap/fetch-folder-state imap-store folder-name)
        db (d/db conn)
        local-state (store/get-folder-sync-state db account-id folder-name)
        needs-full-resync? (and (:uidvalidity local-state)
                                (not= (:uidvalidity local-state)
                                      (:uidvalidity remote-state)))
        result (if needs-full-resync?
                 (full-resync! imap-store conn folder-eid writer
                               account-id folder-name data-path)
                 (incremental-sync! imap-store conn folder-eid writer
                                    account-id folder-name data-path))]
    ;; Update sync state
    (store/update-folder-sync-state! conn folder-eid
                                     {:uidvalidity (:uidvalidity remote-state)
                                      :uidnext     (:uidnext remote-state)})
    ;; Commit scriptum index
    (index/commit! writer (str "sync " folder-name))
    (assoc result :folder folder-name)))

(defn sync-account!
  "Sync all folders (or specific folders) for an account.
   Returns a map of folder-name → sync result."
  [imap-store conn writer account-id & {:keys [folders data-path]}]
  (let [folder-names (or folders (imap/list-folders imap-store))]
    (into {}
          (map (fn [fname]
                 (try
                   [fname (sync-folder! imap-store conn writer account-id fname data-path)]
                   (catch Exception e
                     [fname {:type :error :error (.getMessage e)}]))))
          folder-names)))
