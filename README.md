# briefkasten

[![CircleCI](https://circleci.com/gh/replikativ/briefkasten.svg?style=shield)](https://circleci.com/gh/replikativ/briefkasten)
[![Clojars](https://img.shields.io/clojars/v/org.replikativ/briefkasten.svg)](https://clojars.org/org.replikativ/briefkasten)
[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)

*This is an early beta. Feedback welcome!*

Local IMAP mirror with structured metadata in [datahike](https://github.com/replikativ/datahike), fulltext search in [scriptum](https://github.com/replikativ/scriptum) (Lucene), and atomic versioning via [yggdrasil](https://github.com/replikativ/yggdrasil).

briefkasten syncs your IMAP mailbox to a local store where you can run datalog queries across structured metadata, fulltext search, and raw `.eml` files — all joinable in a single query. Yggdrasil provides atomic copy-on-write snapshots over both datahike and scriptum, enabling time-travel queries across the full mail archive.

## Architecture

```
IMAP Server (source of truth)
    │
    ▼
┌─────────────────────────────────────────────┐
│  briefkasten sync engine                    │
│  - batch fetch (50 msgs/batch)              │
│  - periodic IMAP folder reopen (memory)     │
│  - incremental sync via UID diffing         │
└────┬──────────┬──────────┬──────────────────┘
     │          │          │
     ▼          ▼          ▼
 datahike    scriptum    .eml files
 (metadata)  (fulltext)  (raw RFC822)
     │          │
     └────┬─────┘
          ▼
    yggdrasil composite
    (atomic CoW snapshots)
```

- **datahike** — structured metadata: subject, from, to, date, flags, UIDs, threading headers. No body text (kept lean for performance).
- **scriptum** — Lucene fulltext index over subject, from, to, and body text.
- **.eml files** — raw RFC822 messages and extracted attachments on disk.
- **yggdrasil** — atomic composite versioning (datahike + scriptum branch/commit/snapshot together).

## Configuration

Create `~/.config/briefkasten/config.edn` (or set `BRIEFKASTEN_CONFIG` env var):

```edn
{:accounts
 {:myaccount {:email     "user@example.com"
              :imap      {:host "imap.example.com" :port 993
                          :user "user" :pass "secret"}
              :data-path "/home/user/.local/share/briefkasten/myaccount"}}}
```

IMAP SSL options:
- Default: standard Java SSL verification
- `:insecure? true` — trust all certificates (self-signed servers)
- `:ssl-trust "imap.example.com"` — trust a specific hostname

## Usage

### Sync

```clojure
(require '[org.replikativ.briefkasten.core :as bk])

;; Create account (opens/creates datahike DB + scriptum index)
(def acct (bk/create-account! :myaccount))

;; Sync all folders from IMAP
(bk/sync! acct)

;; Or sync specific folders
(bk/sync! acct :folders ["INBOX"])
;; => {"INBOX" {:type :initial, :stored 95864, :errors 24, :fetched 95888}}
```

Initial sync of ~100k messages completes in ~90 minutes with <1.5GB RSS.

### Search

```clojure
;; Fulltext search across all synced mail
(bk/search acct "datahike build failure" :limit 5)
;; => [{:score 11.5, :uid 201031, :folder "INBOX",
;;      :subject "Re: [replikativ/datahike] Fix cljdoc build (#88)",
;;      :from "Timo Kramer <notifications@github.com>", ...} ...]

;; Search within a specific folder
(bk/search-folder acct "INBOX" "clojure" :limit 10)
```

### Browse

```clojure
;; List synced folders with message counts
(bk/list-folders acct)
;; => [{:name "INBOX", :message-count 95864, :uidvalidity 1, ...}]

;; List recent messages
(bk/list-messages acct "INBOX" :limit 20)

;; Read a full message by UID
(bk/read-message acct {:folder "INBOX" :uid 42})
```

### Joint Datalog Queries (scriptum + datahike + eml)

The real power is running datalog queries that join across all three data sources. Clojure functions can be called from datalog where clauses:

```clojure
(require '[datahike.api :as d]
         '[org.replikativ.briefkasten.index :as index])

;; Helper: search scriptum and return top hit UID
(defn search-uid [writer query]
  (:uid (first (index/search writer query :limit 1))))

;; Helper: read and decode body from .eml file
(defn decode-eml-body [eml-path]
  (let [raw (slurp eml-path)
        body-start (clojure.string/index-of raw "\r\n\r\n")
        body (when body-start (subs raw (+ body-start 4)))]
    (try
      (String. (.decode (java.util.Base64/getMimeDecoder) (.getBytes body)))
      (catch Exception _ body))))

;; Single query: fulltext search -> metadata join -> body read
(d/q '[:find ?subject ?from ?date ?body
       :in $ ?writer ?query
       :where
       [(my.ns/search-uid ?writer ?query) ?uid]
       [?m :mail.message/uid ?uid]
       [?m :mail.message/subject ?subject]
       [?m :mail.message/from ?from]
       [?m :mail.message/date ?date]
       [?m :mail.message/eml-path ?eml-path]
       [(my.ns/decode-eml-body ?eml-path) ?body]]
     (d/db (:conn acct))
     (:writer acct)
     "clojure")
;; => #{["{JOB} Funding Circle - Clojure - London"
;;       "'Angela Piergiovanni Grosso' via Clojure <clojure@googlegroups.com>"
;;       #inst "2022-06-15T11:09:16.000-00:00"
;;       "Hey everyone! My name is Angela and I work for Funding Circle..."]}
```

### Yggdrasil Snapshots and Time-Travel Queries

briefkasten wraps each account as a yggdrasil `CompositeSystem` — datahike and scriptum branch/commit/snapshot together atomically. The composite history is persisted to `<data-path>/composite/` using a PSS (persistent sorted set) backed by konserve, so `history`, `commit-graph`, and `as-of` survive process restarts:

```clojure
(require '[yggdrasil.protocols :as p])

;; Commit a snapshot after sync
(def c (p/commit! (:composite acct) "post-sync snapshot"))

(p/snapshot-id c)
;; => "122d36d5-887d-3580-9c5d-ec2829b0c908"

(p/history c)
;; => ["122d36d5-..." "0fe9a6fa-..." "8d29b338-..."]

;; History persists across restarts — reopen the account and query again
(def acct2 (bk/create-account! :myaccount))
(p/history (:composite acct2))
;; => same history chain as before
```

Use `p/as-of` to get both systems at a historical point, then run the same joint queries against the snapshot:

```clojure
(import '[org.apache.lucene.search IndexSearcher TermQuery BooleanQuery
          BooleanQuery$Builder BooleanClause$Occur]
        '[org.apache.lucene.index Term]
        '[org.apache.lucene.analysis.standard StandardAnalyzer])

;; Helper: search a historical scriptum DirectoryReader
(defn snapshot-search-uid [reader query-str]
  (let [searcher (IndexSearcher. reader)
        ;; ... build boolean query from tokens (see index.clj) ...
        top-docs (.search searcher query 1)
        hits (.-scoreDocs top-docs)]
    (when (pos? (alength hits))
      (let [doc (.document (.storedFields searcher) (.-doc (aget hits 0)))]
        (parse-long (.get doc "uid"))))))

;; Query a yggdrasil snapshot: datahike db + scriptum reader + eml body
(let [snap    (p/snapshot-id c)
      views   (p/as-of c snap)
      dh-db   (get views "briefkasten-dh-myaccount")      ;; datahike DB value
      sc-rdr  (get views "briefkasten-sc-myaccount")]      ;; Lucene DirectoryReader
  (try
    (d/q '[:find ?subject ?from ?date ?body
           :in $ ?reader ?query
           :where
           [(my.ns/snapshot-search-uid ?reader ?query) ?uid]
           [?m :mail.message/uid ?uid]
           [?m :mail.message/subject ?subject]
           [?m :mail.message/from ?from]
           [?m :mail.message/date ?date]
           [?m :mail.message/eml-path ?eml-path]
           [(my.ns/decode-eml-body ?eml-path) ?body]]
         dh-db
         sc-rdr
         "clojure")
    (finally
      (.close sc-rdr))))
;; => #{["{JOB} Funding Circle - Clojure - London"
;;       "'Angela Piergiovanni Grosso' via Clojure <clojure@googlegroups.com>"
;;       #inst "2022-06-15T11:09:16.000-00:00"
;;       "Hey everyone! My name is Angela and I work for Funding Circle..."]}
```

### Cleanup

```clojure
(bk/close! acct)
```

## Dependencies

- [datahike](https://github.com/replikativ/datahike) — immutable datalog database
- [scriptum](https://github.com/replikativ/scriptum) — copy-on-write Lucene indices
- [yggdrasil](https://github.com/replikativ/yggdrasil) — unified CoW versioning
- [clojure-mail](https://github.com/forward-it/clojure-mail) — IMAP access
- [trove](https://github.com/taoensso/trove) — logging facade

## License

Apache-2.0. See [LICENSE](LICENSE).
