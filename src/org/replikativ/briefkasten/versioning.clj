(ns org.replikativ.briefkasten.versioning
  "Yggdrasil integration for briefkasten accounts.

   Wraps each account's datahike conn + scriptum writer as a CompositeSystem
   (fiber product / pullback over the shared branch space). This ensures the
   metadata DB and fulltext index branch/commit/merge together atomically."
  (:require [yggdrasil.adapters.datahike :as ydh]
            [yggdrasil.composite :as yc]
            [yggdrasil.protocols :as p]
            [scriptum.yggdrasil :as sy]))

(defn wrap-account
  "Wrap an account's datahike conn + scriptum writer as a CompositeSystem.

   Constructs ScriptumSystem directly from the existing writer to avoid
   violating Lucene's single-writer constraint.

   Returns the CompositeSystem (pullback of datahike × scriptum)."
  [{:keys [id conn writer data-path]}]
  (let [index-path (str data-path "/scriptum")
        dh-sys (ydh/create conn {:system-name (str "briefkasten-dh-" (name id))})
        sc-sys (sy/->ScriptumSystem
                index-path
                {"main" writer}
                "main"
                (str "briefkasten-sc-" (name id)))]
    ;; Use composite (not pullback) because datahike's default branch
    ;; is :db while scriptum uses "main" — different naming conventions.
    (yc/composite [dh-sys sc-sys]
                  :name (str "briefkasten-" (name id))
                  :branch :main
                  :store-path (str data-path "/composite"))))

(defn branch!
  "Create a branch on the composite system. Returns new CompositeSystem."
  [composite branch-name]
  (p/branch! composite branch-name))

(defn checkout
  "Switch to a branch. Returns new CompositeSystem."
  [composite branch-name]
  (p/checkout composite branch-name))

(defn commit!
  "Commit the composite system. Returns new CompositeSystem."
  [composite message]
  (p/commit! composite message))
