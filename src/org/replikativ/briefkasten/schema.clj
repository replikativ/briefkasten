(ns org.replikativ.briefkasten.schema
  "Datahike schema for local mail cache.

   Entities:
   - :mail.account/* — IMAP/SMTP account configuration
   - :mail.folder/*  — IMAP folder sync state
   - :mail.message/* — Cached email messages
   - :mail.attachment/* — File attachments"
  (:require [datahike.api :as d]))

;; ============================================================================
;; Schema Definition
;; ============================================================================

(def account-schema
  [{:db/ident       :mail.account/id
    :db/valueType   :db.type/keyword
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one
    :db/doc         "Unique account identifier (e.g. :christian)"}

   {:db/ident       :mail.account/email
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.account/imap-host
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.account/imap-port
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.account/smtp-host
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.account/smtp-port
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}])

(def folder-schema
  [{:db/ident       :mail.folder/account
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the parent account"}

   {:db/ident       :mail.folder/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "IMAP folder name (INBOX, Sent, etc.)"}

   {:db/ident       :mail.folder/uidvalidity
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.folder/uidnext
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.folder/last-sync
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}])

(def message-schema
  [{:db/ident       :mail.message/folder
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the parent folder entity"}

   {:db/ident       :mail.message/uid
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one
    :db/index       true
    :db/doc         "IMAP UID (unique within folder+uidvalidity)"}

   {:db/ident       :mail.message/message-id
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index       true
    :db/doc         "RFC Message-ID header"}

   {:db/ident       :mail.message/subject
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.message/from
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Formatted 'Name <addr>'"}

   {:db/ident       :mail.message/to
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.message/cc
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.message/date
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/index       true}

   {:db/ident       :mail.message/flags
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/many
    :db/doc         "IMAP flags: :seen :flagged :answered :draft :deleted"}

   {:db/ident       :mail.message/size
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.message/in-reply-to
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "In-Reply-To header for threading"}

   {:db/ident       :mail.message/references
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "References header (space-separated Message-IDs)"}

   {:db/ident       :mail.message/eml-path
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Relative path to raw .eml file from data-path"}

   {:db/ident       :mail.message/has-attachments
    :db/valueType   :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/doc         "Whether message has file attachments"}])

(def attachment-schema
  [{:db/ident       :mail.attachment/message
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc         "Reference to the parent message entity"}

   {:db/ident       :mail.attachment/filename
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.attachment/content-type
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.attachment/size
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}

   {:db/ident       :mail.attachment/path
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "Relative path from data-path to attachment file"}])

(def mail-schema
  "Complete mail schema for datahike."
  (vec (concat account-schema folder-schema message-schema attachment-schema)))

;; ============================================================================
;; Schema Installation
;; ============================================================================

(defn install-schema!
  "Transact the mail schema into a datahike connection."
  [conn]
  (d/transact conn mail-schema))

;; ============================================================================
;; Entity Constructor Helpers
;; ============================================================================

(defn account-entity
  "Build an account entity map for transacting."
  [{:keys [id email imap-host imap-port smtp-host smtp-port]}]
  (cond-> {:mail.account/id id}
    email     (assoc :mail.account/email email)
    imap-host (assoc :mail.account/imap-host imap-host)
    imap-port (assoc :mail.account/imap-port imap-port)
    smtp-host (assoc :mail.account/smtp-host smtp-host)
    smtp-port (assoc :mail.account/smtp-port smtp-port)))

(defn folder-entity
  "Build a folder entity map for transacting.
   account-ref should be a lookup ref like [:mail.account/id :christian]."
  [{:keys [account-ref name uidvalidity uidnext last-sync]}]
  (cond-> {:mail.folder/account account-ref
           :mail.folder/name    name}
    uidvalidity (assoc :mail.folder/uidvalidity uidvalidity)
    uidnext     (assoc :mail.folder/uidnext uidnext)
    last-sync   (assoc :mail.folder/last-sync last-sync)))

(defn message-entity
  "Build a message entity map for transacting.
   folder-eid is the datahike entity ID of the parent folder."
  [{:keys [folder-eid uid message-id subject from to cc date
           flags size in-reply-to references
           eml-path has-attachments]}]
  (cond-> {:mail.message/folder folder-eid
           :mail.message/uid    uid}
    message-id      (assoc :mail.message/message-id message-id)
    subject         (assoc :mail.message/subject subject)
    from            (assoc :mail.message/from from)
    to              (assoc :mail.message/to to)
    cc              (assoc :mail.message/cc cc)
    date            (assoc :mail.message/date date)
    (seq flags)     (assoc :mail.message/flags (set flags))
    size            (assoc :mail.message/size size)
    in-reply-to     (assoc :mail.message/in-reply-to in-reply-to)
    references      (assoc :mail.message/references references)
    eml-path        (assoc :mail.message/eml-path eml-path)
    (some? has-attachments) (assoc :mail.message/has-attachments has-attachments)))

(defn attachment-entity
  "Build an attachment entity map for transacting.
   message-eid is the datahike entity ID of the parent message."
  [{:keys [message-eid filename content-type size path]}]
  (cond-> {:mail.attachment/message message-eid}
    filename     (assoc :mail.attachment/filename filename)
    content-type (assoc :mail.attachment/content-type content-type)
    size         (assoc :mail.attachment/size size)
    path         (assoc :mail.attachment/path path)))
