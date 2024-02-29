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



(defn subst [expr strategy bound]
  (case strategy
    1 expr
    bound))

(defmacro lookup-strategy [index-expr e a v t]
  {:pre [(symbol? index-expr)
         (nil? (namespace index-expr))]}
  (let [eavt-set (set [e a v t])
        _ (assert (every? #{'_ 'f 1} eavt-set))
        has-substitution (contains? eavt-set 1)
        lookup-expr (if has-substitution
                      `(di/-slice ~index-expr
                                  (datom ~(subst 'e e 'e0)
                                         ~(subst 'a a nil)
                                         ~(subst 'v v nil)
                                         ~(subst 'tx t 'tx0))
                                  (datom ~(subst 'e e 'emax)
                                         ~(subst 'a a nil)
                                         ~(subst 'v v nil)
                                         ~(subst 'tx t 'txmax))
                                  ~(keyword index-expr))
                      `(di/-all ~index-expr))
        dexpr (vary-meta (gensym) assoc :tag `Datom) ;; <-- type hinted symbol
        equalities (remove nil? [(when (= 'f v)
                                   `(a= ~'v (.-v ~dexpr)))
                                 (when (= 'f t)
                                   `(= ~'tx (datom-tx ~dexpr)))])]
    (if (seq equalities)
      `(filter (fn [~dexpr] (and ~@equalities)) ~lookup-expr)
      lookup-expr)))

(defn- search-indices
  "Assumes correct pattern form, i.e. refs for ref-database"
  [eavt aevt avet pattern indexed? temporal-db?]
  (validate-pattern pattern)
  (let [[e a v tx added?] pattern]
    (if (and (not temporal-db?) (false? added?))
      '()
      (match-vector [e a (some? v) tx indexed?]
        [e a v t *] (lookup-strategy eavt 1 1 1 1)
        [e a v _ *] (lookup-strategy eavt 1 1 1 _) #_(di/-slice eavt (datom e a v tx0) (datom e a v txmax) :eavt)
        [e a _ t *] (lookup-strategy eavt 1 1 _ f) #_(->> (di/-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt)
                             (filter (fn [^Datom d] (= tx (datom-tx d)))))
        [e a _ _ *] (di/-slice eavt (datom e a nil tx0) (datom e a nil txmax) :eavt)
        [e _ v t *] (->> (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)
                         (filter (fn [^Datom d] (and (a= v (.-v d))
                                                     (= tx (datom-tx d))))))
        [e _ v _ *] (->> (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)
                         (filter (fn [^Datom d] (a= v (.-v d)))))
        [e _ _ t *] (->> (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)
                         (filter (fn [^Datom d] (= tx (datom-tx d)))))
        [e _ _ _ *] (di/-slice eavt (datom e nil nil tx0) (datom e nil nil txmax) :eavt)
        [_ a v t i] (->> (di/-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet)
                         (filter (fn [^Datom d] (= tx (datom-tx d)))))
        [_ a v t _] (->> (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (and (a= v (.-v d))
                                                     (= tx (datom-tx d))))))
        [_ a v _ i] (di/-slice avet (datom e0 a v tx0) (datom emax a v txmax) :avet)
        [_ a v _ _] (->> (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (a= v (.-v d)))))
        [_ a _ t *] (->> (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
                         (filter (fn [^Datom d] (= tx (datom-tx d)))))
        [_ a _ _ *] (di/-slice aevt (datom e0 a nil tx0) (datom emax a nil txmax) :aevt)
        [_ _ v t *] (filter (fn [^Datom d] (and (a= v (.-v d)) (= tx (datom-tx d)))) (di/-all eavt))
        [_ _ v _ *] (filter (fn [^Datom d] (a= v (.-v d))) (di/-all eavt))
        [_ _ _ t *] (filter (fn [^Datom d] (= tx (datom-tx d))) (di/-all eavt))
        [_ _ _ _ *] (di/-all eavt)))))

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


