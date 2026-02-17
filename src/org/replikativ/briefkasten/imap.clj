(ns org.replikativ.briefkasten.imap
  "Thin IMAP wrapper over clojure-mail with SSL trust support."
  (:require [clojure-mail.core :as mail]
            [clojure-mail.message :as msg]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.trove :as trove])
  (:import [javax.mail Folder Flags Flags$Flag FetchProfile FetchProfile$Item
            UIDFolder$FetchProfileItem Part Multipart]
           [com.sun.mail.imap IMAPFolder IMAPStore IMAPFolder$FetchProfileItem]))

;; ============================================================================
;; Connection
;; ============================================================================

(defn connect!
  "Create an IMAP store connection.
   config: {:host \"imap.example.com\" :port 993 :user \"...\" :pass \"...\"
            :insecure? true}   ; opt-in to skip certificate verification
   By default, standard Java SSL verification is used.
   Set :insecure? true to trust all certificates (e.g. self-signed).
   For fine-grained control, set :ssl-trust to a specific hostname."
  [{:keys [host port user pass insecure? ssl-trust]}]
  (let [base-props {"mail.store.protocol"          "imaps"
                    "mail.imaps.usesocketchannels" "true"
                    "mail.imaps.ssl.enable"        "true"}
        ssl-props (cond
                    insecure?
                    {"mail.imaps.ssl.trust"               "*"
                     "mail.imaps.ssl.checkserveridentity" "false"}

                    ssl-trust
                    {"mail.imaps.ssl.trust" ssl-trust}

                    :else {})
        props (mail/as-properties (merge base-props ssl-props))
        session (javax.mail.Session/getInstance props)
        server (if port [host port] host)]
    (mail/store "imaps" session server user pass)))

(defn disconnect!
  "Close an IMAP store connection."
  [^IMAPStore store]
  (when (and store (.isConnected store))
    (.close store)))

;; ============================================================================
;; Folder Operations
;; ============================================================================

(defn list-folders
  "List all IMAP folders. Returns seq of folder name strings."
  [^IMAPStore store]
  (let [default (.getDefaultFolder store)]
    (->> (.list default "*")
         (map #(.getFullName ^Folder %))
         vec)))

(defn open-folder
  "Open a folder in read-only mode. Returns the IMAPFolder."
  ^IMAPFolder [^IMAPStore store ^String folder-name]
  (let [folder (.getFolder store folder-name)]
    (.open folder Folder/READ_ONLY)
    folder))

(defn fetch-folder-state
  "Get folder sync state: {:uidvalidity :uidnext :message-count}."
  [^IMAPStore store ^String folder-name]
  (let [folder (open-folder store folder-name)]
    (try
      {:uidvalidity (.getUIDValidity ^IMAPFolder folder)
       :uidnext     (.getUIDNext ^IMAPFolder folder)
       :message-count (.getMessageCount folder)}
      (finally
        (.close folder false)))))

;; ============================================================================
;; Message Fetching
;; ============================================================================

(defn- format-addresses
  "Format address list to a string."
  [addrs]
  (when (seq addrs)
    (str/join ", " (map (fn [a]
                          (if (map? a)
                            (let [{:keys [name address]} a]
                              (if name (str name " <" address ">") address))
                            (str a)))
                        addrs))))

(defn- ->string
  "Coerce a value to a string, handling InputStreams from clojure-mail."
  [v]
  (cond
    (string? v) v
    (instance? java.io.InputStream v) (slurp v)
    (nil? v) nil
    :else (str v)))

(defn- extract-text-body
  "Extract text/plain from message body."
  [body]
  (->string
   (cond
     (string? body) body
     (instance? java.io.InputStream body) body
     (sequential? body)
     (or (some (fn [part]
                 (when (and (map? part)
                            (str/starts-with? (str (:content-type part)) "text/plain"))
                   (:body part)))
               body)
         (some (fn [part]
                 (when (and (map? part)
                            (str/starts-with? (str (:content-type part)) "text/html"))
                   (:body part)))
               body)
         (:body (first body)))
     (map? body) (:body body)
     :else body)))

(defn- extract-html-body
  "Extract text/html from message body."
  [body]
  (->string
   (when (sequential? body)
     (some (fn [part]
             (when (and (map? part)
                        (str/starts-with? (str (:content-type part)) "text/html"))
               (:body part)))
           body))))

(defn- imap-flags->keywords
  "Convert javax.mail Flags to a set of keywords."
  [^javax.mail.Message message]
  (let [flags (.getFlags message)]
    (cond-> #{}
      (.contains flags Flags$Flag/SEEN)     (conj :seen)
      (.contains flags Flags$Flag/FLAGGED)  (conj :flagged)
      (.contains flags Flags$Flag/ANSWERED) (conj :answered)
      (.contains flags Flags$Flag/DRAFT)    (conj :draft)
      (.contains flags Flags$Flag/DELETED)  (conj :deleted))))

(defn- save-eml!
  "Write raw RFC822 message to disk. Returns the relative path from data-path."
  [^javax.mail.Message message ^String eml-dir uid]
  (let [path (str eml-dir "/" uid ".eml")]
    (io/make-parents path)
    (with-open [os (java.io.FileOutputStream. path)]
      (.writeTo message os))
    path))

(defn- walk-multipart
  "Recursively walk a Multipart, collecting all leaf BodyParts."
  [^Multipart mp]
  (loop [i 0, parts []]
    (if (>= i (.getCount mp))
      parts
      (let [bp (.getBodyPart mp i)
            content (try (.getContent bp) (catch Exception _ nil))]
        (recur (inc i)
               (if (instance? Multipart content)
                 (into parts (walk-multipart content))
                 (conj parts bp)))))))

(defn- extract-attachments!
  "Extract file attachments from a message to disk.
   Returns a vec of {:filename :content-type :size :path} or nil."
  [^javax.mail.Message message ^String attach-dir uid]
  (try
    (let [content (.getContent message)]
      (when (instance? Multipart content)
        (let [dir (str attach-dir "/" uid)
              attachments (->> (walk-multipart content)
                               (filter (fn [^Part part]
                                         (let [disp (.getDisposition part)]
                                           (or (= Part/ATTACHMENT disp)
                                               ;; Some mailers use INLINE for real files
                                               (and (= Part/INLINE disp)
                                                    (.getFileName part))))))
                               (mapv (fn [^Part part]
                                       (let [fname (or (.getFileName part) "unnamed")
                                             fpath (str dir "/" fname)]
                                         (io/make-parents fpath)
                                         (io/copy (.getInputStream part) (io/file fpath))
                                         {:filename     fname
                                          :content-type (.getContentType part)
                                          :size         (let [s (.getSize part)]
                                                          (if (pos? s) (long s) -1))
                                          :path         fpath}))))]
          (when (seq attachments)
            attachments))))
    (catch Exception _
      ;; Content may not be multipart or may fail to parse; skip attachments
      nil)))

(defn- parse-message
  "Parse a javax.mail.Message into our message map format.
   When data-path and folder-name are provided, saves .eml and extracts attachments."
  [^IMAPFolder folder message & {:keys [data-path folder-name]}]
  (try
    (let [uid (.getUID folder message)
          read (msg/read-message message)
          body-raw (:body read)
          ;; Save EML + extract attachments if data-path provided
          eml-path (when data-path
                     (save-eml! message
                                (str data-path "/eml/" folder-name)
                                uid))
          attachments (when data-path
                        (extract-attachments! message
                                              (str data-path "/attachments/" folder-name)
                                              uid))]
      {:uid             uid
       :message-id      (:id read)
       :subject         (:subject read)
       :from            (format-addresses (:from read))
       :to              (format-addresses (:to read))
       :cc              (format-addresses (:cc read))
       :date            (:date-sent read)
       :flags           (imap-flags->keywords message)
       :body-text       (extract-text-body body-raw)
       :body-html       (extract-html-body body-raw)
       :size            (long (.getSize message))
       :in-reply-to     (msg/in-reply-to message)
       :references      (when-let [hdrs (.getHeader message "References")]
                          (first hdrs))
       :eml-path        eml-path
       :attachments     attachments
       :has-attachments (boolean (seq attachments))})
    (catch Exception e
      (let [uid (try (.getUID folder message) (catch Exception _ -1))]
        {:uid uid :error (.getMessage e)}))))

(defn fetch-uids
  "Get all UIDs in a folder. Returns a set of longs.
   Uses FetchProfile to bulk-prefetch UIDs in a single IMAP command."
  [^IMAPStore store ^String folder-name]
  (let [folder (open-folder store folder-name)]
    (try
      (let [msgs (.getMessages folder)
            total (alength msgs)
            _ (trove/log! {:level :info, :id ::prefetch-uids
                           :data {:folder folder-name, :count total}
                           :msg (format "[%s] prefetching UIDs for %d messages..." folder-name total)})
            ;; Bulk-prefetch UIDs — single IMAP FETCH command instead of N round-trips
            fp (FetchProfile.)]
        (.add fp UIDFolder$FetchProfileItem/UID)
        (.fetch ^IMAPFolder folder msgs fp)
        (let [uids (set (map #(.getUID ^IMAPFolder folder %) msgs))]
          (trove/log! {:level :info, :id ::uids-fetched
                       :data {:folder folder-name, :count (count uids)}
                       :msg (format "[%s] got %d UIDs" folder-name (count uids))})
          uids))
      (finally
        (.close folder false)))))

(defn- prefetch-full!
  "Prefetch envelope + flags + full MIME content in bulk."
  [^IMAPFolder folder ^"[Ljavax.mail.Message;" msg-array]
  (let [fp (FetchProfile.)]
    (.add fp FetchProfile$Item/ENVELOPE)
    (.add fp FetchProfile$Item/CONTENT_INFO)
    (.add fp FetchProfile$Item/FLAGS)
    (.add fp UIDFolder$FetchProfileItem/UID)
    (.add fp IMAPFolder$FetchProfileItem/MESSAGE)
    (.fetch folder msg-array fp)))

(defn- fetch-message-batch
  "Fetch a single batch of messages by UIDs. Opens/closes folder per batch."
  [^IMAPStore store ^String folder-name uid-batch & {:keys [data-path]}]
  (let [folder (open-folder store folder-name)]
    (try
      (let [uid-array (long-array uid-batch)
            msgs (.getMessagesByUID ^IMAPFolder folder uid-array)
            non-nil (into-array javax.mail.Message (remove nil? msgs))]
        (when (pos? (alength non-nil))
          (prefetch-full! folder non-nil))
        (mapv #(parse-message folder %
                              :data-path data-path
                              :folder-name folder-name)
              non-nil))
      (finally
        (.close folder false)))))

(defn fetch-messages
  "Fetch full messages for a set of UIDs.
   Returns a seq of parsed message maps.
   For large UID sets, processes in batches with progress logging.
   Options:
     :data-path — base directory for .eml + attachment storage"
  [^IMAPStore store ^String folder-name uids & {:keys [data-path]}]
  (when (seq uids)
    (let [total (count uids)
          batch-size 50
          batches (partition-all batch-size (sort uids))]
      (loop [remaining batches
             result []]
        (if-let [batch (first remaining)]
          (let [parsed (fetch-message-batch store folder-name batch
                                            :data-path data-path)
                done (+ (count result) (count parsed))]
            (when (> total batch-size)
              (trove/log! {:level :info, :id ::fetch-progress
                           :data {:folder folder-name, :done done, :total total}
                           :msg (format "[%s] fetched %d/%d messages (%.0f%%)"
                                        folder-name done total
                                        (* 100.0 (/ done total)))}))

            (recur (rest remaining) (into result parsed)))
          result)))))

(defn fetch-all-messages
  "Fetch all messages in a folder, in batches with IMAP prefetch.
   Downloads full MIME content and saves .eml + attachments to disk.
   Reopens the folder every :reopen-interval batches to release cached
   MIME content and bound memory usage.
   Options:
     :data-path — base directory for .eml + attachment storage
     :batch-fn  — if provided, called with each batch of parsed messages
                   instead of accumulating. Returns total count."
  [^IMAPStore store ^String folder-name & {:keys [data-path batch-fn]}]
  (let [batch-size 50
        reopen-interval 20  ;; reopen folder every 1000 messages
        folder (open-folder store folder-name)
        total (.getMessageCount folder)]
    (.close folder false)
    (trove/log! {:level :info, :id ::fetch-all
                 :data {:folder folder-name, :count total}
                 :msg (format "[%s] %d messages to fetch (full MIME)" folder-name total)})
    (loop [offset 0
           processed 0
           ^IMAPFolder folder (open-folder store folder-name)
           batches-since-reopen 0]
      (if (>= offset total)
        (do (.close folder false) processed)
        (let [;; Reopen folder periodically to release cached MIME content
              [^IMAPFolder cur-folder reopen-count]
              (if (>= batches-since-reopen reopen-interval)
                (do (.close folder false)
                    [(open-folder store folder-name) 0])
                [folder batches-since-reopen])
              all-msgs (.getMessages cur-folder)
              end (min (+ offset batch-size) total)
              batch-array (java.util.Arrays/copyOfRange all-msgs offset end)]
          (prefetch-full! cur-folder batch-array)
          (let [parsed (mapv #(parse-message cur-folder %
                                             :data-path data-path
                                             :folder-name folder-name)
                             batch-array)
                done (+ processed (count parsed))]
            (trove/log! {:level :info, :id ::fetch-all-progress
                         :data {:folder folder-name, :done done, :total total}
                         :msg (format "[%s] fetched %d/%d (%.0f%%)"
                                      folder-name done total
                                      (* 100.0 (/ done (max 1 total))))})
            (when batch-fn (batch-fn parsed))
            (recur end done cur-folder (inc reopen-count))))))))

(defn fetch-flags
  "Fetch only flags for UIDs (lighter than full message fetch).
   Returns {uid #{:seen :flagged ...}}."
  [^IMAPStore store ^String folder-name uids]
  (when (seq uids)
    (let [folder (open-folder store folder-name)]
      (try
        (let [uid-array (long-array (sort uids))
              msgs (.getMessagesByUID ^IMAPFolder folder uid-array)]
          (into {}
                (comp (remove nil?)
                      (map (fn [m]
                             [(.getUID ^IMAPFolder folder m)
                              (imap-flags->keywords m)])))
                msgs))
        (finally
          (.close folder false))))))

;; ============================================================================
;; IMAP Mutations
;; ============================================================================

(defn set-flags!
  "Set flags on messages by UID.
   flag-changes is a seq of {:uid :add #{:seen} :remove #{:flagged}}."
  [^IMAPStore store ^String folder-name flag-changes]
  (when (seq flag-changes)
    (let [folder (.getFolder store folder-name)]
      (.open folder Folder/READ_WRITE)
      (try
        (doseq [{:keys [uid add remove]} flag-changes]
          (let [msg (.getMessageByUID ^IMAPFolder folder uid)]
            (when msg
              (when (seq add)
                (doseq [flag add]
                  (case flag
                    :seen     (.setFlag msg Flags$Flag/SEEN true)
                    :flagged  (.setFlag msg Flags$Flag/FLAGGED true)
                    :answered (.setFlag msg Flags$Flag/ANSWERED true)
                    :draft    (.setFlag msg Flags$Flag/DRAFT true)
                    :deleted  (.setFlag msg Flags$Flag/DELETED true)
                    nil)))
              (when (seq remove)
                (doseq [flag remove]
                  (case flag
                    :seen     (.setFlag msg Flags$Flag/SEEN false)
                    :flagged  (.setFlag msg Flags$Flag/FLAGGED false)
                    :answered (.setFlag msg Flags$Flag/ANSWERED false)
                    :draft    (.setFlag msg Flags$Flag/DRAFT false)
                    :deleted  (.setFlag msg Flags$Flag/DELETED false)
                    nil))))))
        (finally
          (.close folder false))))))

(defn expunge!
  "Permanently remove messages flagged as deleted."
  [^IMAPStore store ^String folder-name]
  (let [folder (.getFolder store folder-name)]
    (.open folder Folder/READ_WRITE)
    (try
      (.expunge folder)
      (finally
        (.close folder false)))))
