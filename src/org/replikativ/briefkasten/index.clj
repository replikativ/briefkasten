(ns org.replikativ.briefkasten.index
  "Scriptum fulltext index for mail messages.

   Each message is indexed as a Lucene document with analyzed text fields
   (subject, from, to, body) for fulltext search, plus string fields
   (id, folder, account) for exact filtering."
  (:require [scriptum.core :as sc]
            [clojure.string :as str])
  (:import [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.analysis TokenStream]
           [org.apache.lucene.analysis.tokenattributes CharTermAttribute]
           [org.apache.lucene.index Term]
           [org.apache.lucene.search BooleanQuery BooleanQuery$Builder
            BooleanClause$Occur TermQuery IndexSearcher]
           [org.apache.lucene.document Document]))

;; ============================================================================
;; Index Lifecycle
;; ============================================================================

(defn create-mail-index!
  "Create a scriptum index for mail at the given path.
   Returns a BranchIndexWriter."
  [^String path]
  (sc/create-index path "main"))

(defn open-mail-index
  "Open an existing mail index.
   Returns a BranchIndexWriter."
  [^String path]
  (sc/open-branch path "main"))

;; ============================================================================
;; Document Helpers
;; ============================================================================

(defn- sync-key
  "Build a unique document ID: account/folder/uid."
  [account-id folder-name uid]
  (str (name account-id) "/" folder-name "/" uid))

(defn- message->doc
  "Convert a message map to a scriptum document."
  [account-id folder-name {:keys [uid message-id subject from to body-text date]}]
  {:id      {:value (sync-key account-id folder-name uid) :type :string :stored? true}
   :msg-id  {:value (or message-id "") :type :string :stored? true}
   :subject {:value (or subject "") :type :text :stored? true}
   :from    {:value (or from "") :type :text :stored? true}
   :to      {:value (or to "") :type :text :stored? true}
   :body    {:value (or body-text "") :type :text :stored? false}
   :date    {:value (if date (str date) "") :type :string :stored? true}
   :folder  {:value (or folder-name "") :type :string :stored? true}
   :account {:value (name account-id) :type :string :stored? true}
   :uid     {:value (str uid) :type :string :stored? true}})

;; ============================================================================
;; Indexing Operations
;; ============================================================================

(defn index-messages!
  "Add/update message documents in the scriptum index.
   messages is a seq of message maps (as from imap/parse-message)."
  [writer account-id folder-name messages]
  (doseq [msg messages]
    (when-not (:error msg)
      (let [key (sync-key account-id folder-name (:uid msg))]
        ;; Delete existing doc if any, then add new
        (sc/delete-docs writer "id" key)
        (sc/add-doc writer (message->doc account-id folder-name msg))))))

(defn delete-messages!
  "Remove documents from the index by UIDs."
  [writer account-id folder-name uids]
  (doseq [uid uids]
    (sc/delete-docs writer "id" (sync-key account-id folder-name uid))))

(defn delete-folder!
  "Remove all documents for a folder from the index."
  [writer account-id folder-name]
  (let [results (sc/search writer {:term [:folder folder-name]}
                           {:limit Integer/MAX_VALUE})]
    (doseq [doc results
            :when (= (get doc "account") (name account-id))]
      (sc/delete-docs writer "id" (get doc "id")))))

(defn commit!
  "Commit the index with optional datahike tx metadata."
  ([writer]
   (sc/commit! writer))
  ([writer message]
   (sc/commit! writer message))
  ([writer message datahike-tx]
   (sc/commit! writer message
               (when datahike-tx
                 {"datahike/tx" (str datahike-tx)}))))

;; ============================================================================
;; Search
;; ============================================================================

(defn- tokenize
  "Tokenize a string using StandardAnalyzer. Returns a seq of token strings."
  [^String text]
  (let [analyzer (StandardAnalyzer.)
        tokens (atom [])]
    (with-open [^TokenStream ts (.tokenStream analyzer "default" text)]
      (let [^CharTermAttribute attr (.addAttribute ts CharTermAttribute)]
        (.reset ts)
        (while (.incrementToken ts)
          (swap! tokens conj (.toString attr)))
        (.end ts)))
    @tokens))

(defn- build-query
  "Build a Lucene BooleanQuery from a query string.
   For each token, creates term queries across subject/from/to/body (SHOULD),
   then combines all token groups with MUST (AND semantics)."
  [^String query-str & {:keys [folder account]}]
  (let [tokens (tokenize query-str)
        builder (BooleanQuery$Builder.)]
    ;; Each token MUST match in at least one field
    (doseq [token tokens]
      (let [field-builder (BooleanQuery$Builder.)]
        (doseq [field ["subject" "from" "to" "body"]]
          (.add field-builder
                (TermQuery. (Term. field token))
                BooleanClause$Occur/SHOULD))
        (.add builder (.build field-builder) BooleanClause$Occur/MUST)))
    ;; Optional folder filter
    (when folder
      (.add builder
            (TermQuery. (Term. "folder" folder))
            BooleanClause$Occur/MUST))
    ;; Optional account filter
    (when account
      (.add builder
            (TermQuery. (Term. "account" (name account)))
            BooleanClause$Occur/MUST))
    (.build builder)))

(defn search
  "Fulltext search across indexed mail.
   Returns [{:score :uid :folder :subject :from :date :account :msg-id}].

   Options:
     :folder  — restrict to a specific folder
     :account — restrict to a specific account
     :limit   — max results (default 20)"
  [writer query-str & {:keys [folder account limit] :or {limit 20}}]
  (when (and query-str (not (str/blank? query-str)))
    (let [q (build-query query-str :folder folder :account account)
          results (sc/search writer q {:limit limit})]
      (mapv (fn [r]
              {:score   (:score r)
               :uid     (when-let [u (get r "uid")] (parse-long u))
               :folder  (get r "folder")
               :subject (get r "subject")
               :from    (get r "from")
               :date    (get r "date")
               :account (get r "account")
               :msg-id  (get r "msg-id")})
            results))))
