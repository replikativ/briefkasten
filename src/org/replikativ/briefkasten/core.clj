(ns org.replikativ.briefkasten.core
  "Public API for the local mail sync library.

   Configure accounts in ~/.config/briefkasten/config.edn
   (or set BRIEFKASTEN_CONFIG env var to override path):

     {:accounts
      {:myaccount {:email     \"user@example.com\"
                   :imap {:host \"imap.example.com\" :port 993
                          :user \"user\" :pass \"secret\"}
                   :data-path \"/home/user/.local/share/briefkasten/myaccount\"}}}

   Usage:
     (def acct (create-account! :myaccount))
     (sync! acct)
     (search acct \"datahike build failure\")
     (list-messages acct \"INBOX\" {:limit 20})
     (read-message acct {:folder \"INBOX\" :uid 42})
     (close! acct)"
  (:require [org.replikativ.briefkasten.schema :as schema]
            [org.replikativ.briefkasten.store :as store]
            [org.replikativ.briefkasten.imap :as imap]
            [org.replikativ.briefkasten.index :as index]
            [org.replikativ.briefkasten.sync :as sync]
            [org.replikativ.briefkasten.versioning :as ver]
            [yggdrasil.composite :as yc]
            [scriptum.core :as sc]
            [datahike.api :as d]
            [clojure.java.io :as io]
            [clojure.edn :as edn]))

;; ============================================================================
;; Configuration
;; ============================================================================

(def ^:private default-config-path
  "Default path for the briefkasten config file."
  (str (System/getProperty "user.home") "/.config/briefkasten/config.edn"))

(defn config-path
  "Return the config file path.
   Uses BRIEFKASTEN_CONFIG env var if set, otherwise ~/.config/briefkasten/config.edn."
  []
  (or (System/getenv "BRIEFKASTEN_CONFIG")
      default-config-path))

(defn load-config
  "Load configuration from the config file.
   Returns the parsed EDN map, or nil if file doesn't exist."
  ([] (load-config (config-path)))
  ([path]
   (let [f (io/file path)]
     (when (.exists f)
       (edn/read-string (slurp f))))))

;; ============================================================================
;; Account Lifecycle
;; ============================================================================

(defn create-account!
  "Create a mail account with local datahike DB + scriptum index.

   Can be called with:
   - A keyword: loads full config from config file by account ID
   - A map: uses the provided config directly

   Config map keys:
     :id        — keyword identifier (e.g. :myaccount)
     :email     — email address
     :imap      — {:host :port :user :pass :insecure? :ssl-trust}
     :smtp      — {:host :port :user :pass} (optional)
     :data-path — directory for local storage

   Returns an account handle map with :conn :writer :imap-config :data-path :config."
  [id-or-config]
  (let [config (cond
                 (keyword? id-or-config)
                 (let [cfg (get-in (load-config) [:accounts id-or-config])]
                   (when-not cfg
                     (throw (ex-info (str "No account " id-or-config
                                          " found in config file " (config-path))
                                     {:account id-or-config
                                      :config-path (config-path)})))
                   (assoc cfg :id id-or-config))

                 (map? id-or-config)
                 id-or-config

                 :else
                 (throw (ex-info "create-account! expects a keyword or config map"
                                 {:argument id-or-config})))
        {:keys [id email imap smtp data-path]} config
        _ (do (assert (keyword? id) "config :id must be a keyword")
              (assert (string? email) "config :email must be a string")
              (assert (map? imap) "config :imap must be a map")
              (assert (string? data-path) "config :data-path must be a string")
              (assert (string? (:host imap)) "config :imap :host must be a string")
              (assert (number? (:port imap)) "config :imap :port must be a number")
              (assert (string? (:user imap)) "config :imap :user must be a string")
              (assert (string? (:pass imap)) "config :imap :pass must be a string"))]
    (let [db-path    (str data-path "/datahike")
          index-path (str data-path "/scriptum")
          ;; Only create parent of data-path; datahike/scriptum create their own dirs
          _          (io/make-parents (str data-path "/dummy"))
          ;; Datahike — deterministic UUID from account id for stable reopening
          db-id      (java.util.UUID/nameUUIDFromBytes (.getBytes (str "briefkasten/" (name id))))
          db-config  {:store {:backend :file :path db-path :id db-id}
                      :schema-flexibility :write
                      :keep-history? true}
          _          (when-not (d/database-exists? db-config)
                       (d/create-database db-config))
          conn       (d/connect db-config)
          _          (schema/install-schema! conn)
          ;; Scriptum — open existing or create new
          writer     (if (.exists (io/file index-path "main"))
                       (index/open-mail-index index-path)
                       (index/create-mail-index! index-path))
          ;; Store account entity
          _          (d/transact conn [(schema/account-entity
                                        {:id        id
                                         :email     email
                                         :imap-host (:host imap)
                                         :imap-port (:port imap)
                                         :smtp-host (:host smtp)
                                         :smtp-port (:port smtp)})])]
      (let [handle {:id          id
                    :conn        conn
                    :writer      writer
                    :imap-config imap
                    :smtp-config smtp
                    :data-path   data-path
                    :config      config}]
        (assoc handle :composite (ver/wrap-account handle))))))

(defn close!
  "Close all resources for an account."
  [{:keys [conn writer composite]}]
  (when composite (yc/close! composite))
  (when writer (index/commit! writer "close") (sc/close! writer))
  (when conn (d/release conn)))

;; ============================================================================
;; Sync
;; ============================================================================

(defn sync!
  "Sync mail for an account (all folders or specific ones).
   Connects to IMAP, syncs, then disconnects.
   Downloads full MIME content and saves .eml + attachments to data-path.

   Options:
     :folders — seq of folder names to sync (default: all)"
  [{:keys [id conn writer imap-config data-path]} & {:keys [folders]}]
  (let [imap-store (imap/connect! imap-config)]
    (try
      (sync/sync-account! imap-store conn writer id
                          :folders folders
                          :data-path data-path)
      (finally
        (imap/disconnect! imap-store)))))

;; ============================================================================
;; Search
;; ============================================================================

(defn search
  "Fulltext search across all synced mail.
   Returns [{:score :uid :folder :subject :from :date}].

   Options:
     :limit — max results (default 20)"
  [{:keys [writer id]} query & {:keys [limit] :or {limit 20}}]
  (index/search writer query :account id :limit limit))

(defn search-folder
  "Fulltext search within a specific folder."
  [{:keys [writer id]} folder-name query & {:keys [limit] :or {limit 20}}]
  (index/search writer query :account id :folder folder-name :limit limit))

;; ============================================================================
;; Read Operations
;; ============================================================================

(defn list-folders
  "List synced folders with message counts."
  [{:keys [conn id]}]
  (store/list-folders (d/db conn) id))

(defn list-messages
  "List messages in a folder from local cache.
   Options: :limit (default 50)"
  [{:keys [conn id]} folder-name & {:keys [limit] :or {limit 50}}]
  (let [db (d/db conn)
        folder-eid (store/get-folder-eid db id folder-name)]
    (when folder-eid
      (store/list-messages db folder-eid :limit limit))))

(defn read-message
  "Read a full message by UID or Message-ID.
   lookup: {:folder \"INBOX\" :uid 42} or {:folder \"INBOX\" :message-id \"<...>\"}."
  [{:keys [conn id]} {:keys [folder uid message-id] :as lookup}]
  (let [db (d/db conn)
        folder-eid (store/get-folder-eid db id (or folder "INBOX"))]
    (when folder-eid
      (store/read-message db folder-eid
                          (select-keys lookup [:uid :message-id])))))

(defn message-count
  "Count messages in a folder."
  [{:keys [conn id]} folder-name]
  (let [db (d/db conn)
        folder-eid (store/get-folder-eid db id folder-name)]
    (when folder-eid
      (store/message-count db folder-eid))))
