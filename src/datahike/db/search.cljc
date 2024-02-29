(ns datahike.db.search
  (:require
   [clojure.core.cache.wrapped :as cw]
   [datahike.array :refer [a=]]
   [datahike.constants :refer [e0 tx0 emax txmax]]
   [datahike.datom :refer [datom datom-tx datom-added]]
   [datahike.db.utils :as dbu]
   [datahike.index :as di]
   [datahike.lru :refer [lru-datom-cache-factory]]
   [datahike.tools :refer [case-tree raise match-vector]]
   [environ.core :refer [env]])
  #?(:cljs (:require-macros [datahike.datom :refer [datom]]
                            [datahike.tools :refer [case-tree raise]]))
  #?(:clj (:import [datahike.datom Datom])))

(def db-caches (cw/lru-cache-factory {} :threshold (:datahike-max-db-caches env 5)))

(defn memoize-for [db key f]
  (if (or (zero? (or (:cache-size (:config db)) 0))
          (zero? (:hash db))) ;; empty db
    (f)
    (let [db-cache (cw/lookup-or-miss db-caches
                                      (:hash db)
                                      (fn [_] (lru-datom-cache-factory {} :threshold (:cache-size (:config db)))))]
      (cw/lookup-or-miss db-cache key (fn [_] (f))))))

(defn validate-pattern
  "Checks if database pattern is valid"
  [pattern]
  (let [[e a v tx added?] pattern]

    (when-not (or (number? e)
                  (nil? e)
                  (and (vector? e) (= 2 (count e))))
      (raise "Bad format for entity-id in pattern, must be a number, nil or vector of two elements."
             {:error :search/pattern :e e :pattern pattern}))

    (when-not (or (number? a)
                  (keyword? a)
                  (nil? a))
      (raise "Bad format for attribute in pattern, must be a number, nil or a keyword."
             {:error :search/pattern :a a :pattern pattern}))

    (when-not (or (not (vector? v))
                  (nil? v)
                  (and (vector? v) (= 2 (count v))))
      (raise "Bad format for value in pattern, must be a scalar, nil or a vector of two elements."
             {:error :search/pattern :v v :pattern pattern}))

    (when-not (or (nil? tx)
                  (number? tx))
      (raise "Bad format for transaction ID in pattern, must be a number or nil."
             {:error :search/pattern :tx tx :pattern pattern}))

    (when-not (or (nil? added?)
                  (boolean? added?))
      (raise "Bad format for added? in pattern, must be a boolean value or nil."
             {:error :search/pattern :added? added? :pattern pattern}))))





(defn datom-expr [[esym asym vsym tsym]
                  [e-strat a-strat v-strat t-strat]
                  e-bound
                  tx-bound]
  (let [subst (fn [expr strategy bound]
                (case strategy
                  1 expr
                  bound))]
    `(datom ~(subst esym e-strat e-bound)
            ~(subst asym a-strat nil)
            ~(subst vsym v-strat nil)
            ~(subst tsym t-strat tx-bound))))

(defn lookup-strategy-sub [eavt-symbols index-expr eavt-strats]
  {:pre [(symbol? index-expr)
         (nil? (namespace index-expr))]}
  (let [[_ _ v-strat t-strat] eavt-strats
        [_ _ v-sym t-sym] eavt-symbols
        strat-set (set eavt-strats)
        _ (assert (every? #{'_ 'f 1} strat-set))
        has-substitution (contains? strat-set 1)
        lookup-expr (if has-substitution
                      `(di/-slice ~index-expr
                                  ~(datom-expr eavt-symbols eavt-strats 'e0 'tx0)
                                  ~(datom-expr eavt-symbols eavt-strats 'emax 'txmax)
                                  ~(keyword index-expr))
                      `(di/-all ~index-expr))
        dexpr (vary-meta (gensym) assoc :tag `Datom) ;; <-- type hinted symbol
        equalities (remove nil? [(when (= 'f v-strat)
                                   `(a= ~v-sym (.-v ~dexpr)))
                                 (when (= 'f t-strat)
                                   `(= ~t-sym (datom-tx ~dexpr)))])]
    (if (seq equalities)
      `(filter (fn [~dexpr] (and ~@equalities)) ~lookup-expr)
      lookup-expr)))

(defmacro lookup-strategy [index-expr & eavt-strats]
  (lookup-strategy-sub '[e a v tx] index-expr eavt-strats))

(defn- search-indices
  "Assumes correct pattern form, i.e. refs for ref-database"
  [eavt aevt avet pattern indexed? temporal-db?]
  (validate-pattern pattern)
  (let [[e a v tx added?] pattern]
    (if (and (not temporal-db?) (false? added?))
      '()
      (match-vector [e a (some? v) tx indexed?]
        [e a v t *] (lookup-strategy eavt 1 1 1 1)
        [e a v _ *] (lookup-strategy eavt 1 1 1 _)
        [e a _ t *] (lookup-strategy eavt 1 1 _ f)
        [e a _ _ *] (lookup-strategy eavt 1 1 _ _)
        [e _ v t *] (lookup-strategy eavt 1 _ f f)
        [e _ v _ *] (lookup-strategy eavt 1 _ f _)
        [e _ _ t *] (lookup-strategy eavt 1 _ _ f)
        [e _ _ _ *] (lookup-strategy eavt 1 _ _ _)
        [_ a v t i] (lookup-strategy avet _ 1 1 f)
        [_ a v t _] (lookup-strategy aevt _ 1 f f)
        [_ a v _ i] (lookup-strategy avet _ 1 1 _)
        [_ a v _ _] (lookup-strategy aevt _ 1 f _)
        [_ a _ t *] (lookup-strategy aevt _ 1 _ f)
        [_ a _ _ *] (lookup-strategy aevt _ 1 _ _)
        [_ _ v t *] (lookup-strategy eavt _ _ f f)
        [_ _ v _ *] (lookup-strategy eavt _ _ f _)
        [_ _ _ t *] (lookup-strategy eavt _ _ _ f)
        [_ _ _ _ *] (lookup-strategy eavt _ _ _ _)))))

(defn search-current-indices [db pattern]
  (memoize-for db [:search pattern]
               #(let [[_ a _ _] pattern]
                  (search-indices (:eavt db)
                                  (:aevt db)
                                  (:avet db)
                                  pattern
                                  (dbu/indexing? db a)
                                  false))))

(defn search-temporal-indices [db pattern]
  (memoize-for db [:temporal-search pattern]
               #(let [[_ a _ _ added] pattern
                      result (search-indices (:temporal-eavt db)
                                             (:temporal-aevt db)
                                             (:temporal-avet db)
                                             pattern
                                             (dbu/indexing? db a)
                                             true)]
                  (case added
                    true (filter datom-added result)
                    false (remove datom-added result)
                    nil result))))

(defn temporal-search [db pattern]
  (dbu/distinct-datoms db
                       (search-current-indices db pattern)
                       (search-temporal-indices db pattern)))

(defn temporal-seek-datoms [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (dbu/components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (dbu/distinct-datoms db
                         (di/-slice index from to index-type)
                         (di/-slice temporal-index from to index-type))))

(defn temporal-rseek-datoms [db index-type cs]
  (let [index (get db index-type)
        temporal-index (get db (keyword (str "temporal-" (name index-type))))
        from (dbu/components->pattern db index-type cs e0 tx0)
        to (datom emax nil nil txmax)]
    (concat
     (-> (dbu/distinct-datoms db
                              (di/-slice index from to index-type)
                              (di/-slice temporal-index from to index-type))
         vec
         rseq))))

(defn temporal-index-range [db current-db attr start end]
  (when-not (dbu/indexing? db attr)
    (raise "Attribute" attr "should be marked as :db/index true" {}))
  (dbu/validate-attr attr (list '-index-range 'db attr start end) db)
  (let [from (dbu/resolve-datom current-db nil attr start nil e0 tx0)
        to (dbu/resolve-datom current-db nil attr end nil emax txmax)]
    (dbu/distinct-datoms db
                         (di/-slice (:avet db) from to :avet)
                         (di/-slice (:temporal-avet db) from to :avet))))


