(ns datahike.debug-query
  (:require [datahike.api :as d]
            [datahike.query :as dq]
            [clojure.spec.alpha :as spec]
            [clojure.pprint :as pp]
            [taoensso.nippy :as nippy]
            [clojure.edn :as edn]))

(defn default-data []
  (nippy/thaw-from-file "/home/jonas/prog/jobtech-taxonomy-api/resources/taxonomy.nippy"))

(def db-config {:keep-history? true, :keep-history true, :search-cache-size 10000, :index :datahike.index/persistent-set, :store {:id "in-mem-nippy-data2", :backend :mem}, :name "Nippy testing data", :store-cache-size 1000, :wanderung/type :datahike, :attribute-refs? true, :writer {:backend :self}, :crypto-hash? false, :schema-flexibility :write, :branch :db})

(defn with-connection-fn [db f]
  (let [conn (d/connect db)]
    (try
      (f conn)
      (finally
        (d/release conn)))))

(defmacro wrap-op [trace type-key end-data & body]
  `(do (swap! ~trace conj {:type [~type-key :begin]})
       (let [start# (System/nanoTime)
             result# (do ~@body)
             end# (System/nanoTime)]
         (swap! ~trace conj (assoc ~end-data
                                   :type [~type-key :end]
                                   :result result#
                                   :start-ns start#
                                   :elapsed-ns (- end# start#)))
         result#)))

(defn wrap-traced-fn [trace type-key f arg-ks]
  {:pre [(keyword? type-key)
         (ifn? f)
         (sequential? arg-ks)]}
  (fn [& args]
    (wrap-op trace type-key (assoc (zipmap arg-ks args) :args args) (apply f args))))

(defn summarize-trace-item [x]
  (select-keys x [:type :start-ns :elapsed-ns :path]))


(spec/def ::function-arglist-pairs
  (spec/* (spec/cat :fsym symbol?
                    :arglist (spec/spec (spec/* symbol?)))))

(defmacro with-trace-functions [function-arglist-pairs & body]
  (let [parsed (map #(assoc % :var (gensym))
                    (spec/conform ::function-arglist-pairs
                                  function-arglist-pairs))
        trace (gensym)]
    `(let [~trace (atom [])
           ~@(for [x parsed
                   y [(:var x) (:fsym x)]]
               y)
           result# (with-redefs
                    ~(into []
                           (mapcat (fn [x]
                                     [(:fsym x)
                                      `(wrap-traced-fn
                                        ~trace
                                        ~(keyword (:fsym x))
                                        ~(:var x)
                                        ~(mapv keyword (:arglist x)))]))
                           parsed)
                    ~@body)]
       [result# (deref ~trace)])))

(defn traced-q
  "Instrument important functions to record a trace of db operations"
  [& args]
  (with-trace-functions
      [
       ;dq/-resolve-clause* [context clause orig-clause]
       ;dq/lookup-pattern [context source pattern orig-pattern]
       ;dq/lookup-patterns [context clause p-before p-after]
       ;dq/simplify-rel [rel]
       ;dq/var-mapping [pattern indices]
       ;dq/sum-rel [a b]
       ;dq/lookup-and-sum-pattern-rels [context source patterns clause collect-stats]
       ;dq/lookup-pattern-db [context db pattern orig-pattern]
       ;dq/q []
       ]
    (time (apply dq/q args))))

(defmacro with-connection [[conn db] & body]
  {:pre [(symbol? conn)]}
  `(with-connection-fn
     ~db
     (fn [~conn]
       ~@body)))

(defn create-populated-database []
  (let [db (d/create-database db-config)]
    (with-connection [conn db]
      (deref (d/load-entities conn (default-data))))
    (println "Database created and populated.")
    db))

(defonce db (delay (create-populated-database)))

(defn set-db [query db]
  (def the-db db)
  (update query :args (fn [args] (into [db] args))))

(defn set-strategy [query strategy]
  (assoc-in query [:settings :search-strategy] strategy))

(defn query2 [conn strategy]
  (-> (dq/normalize-q-input
       '[:find
         (pull ?oc [:concept/id :concept/type :concept/preferred-label])
         (pull
          ?esco
          [:concept/id
           :concept/type
           :concept/preferred-label
           :concept.external-standard/esco-uri])
         :in
         $
         %
         :where
         [?oc :concept/type "occupation-name"]
         (edge ?oc "narrow-match" ?esco ?r1)]
       '[[[(-forward-edge ?from-concept ?type ?to-concept ?relation)
           [(ground
             ["broader"
              "related"
              "possible-combination"
              "unlikely-combination"
              "substitutability"
              "broad-match"
              "exact-match"
              "close-match"])
            [?type ...]]
           [?relation :relation/concept-1 ?from-concept]
           [?relation :relation/type ?type]
           [?relation :relation/concept-2 ?to-concept]]
          [(-reverse-edge ?from-concept ?type ?to-concept ?relation)
           [(ground
             {"narrower" "broader",
              "related" "related",
              "possible-combination" "possible-combination",
              "unlikely-combination" "unlikely-combination",
              "substituted-by" "substitutability",
              "narrow-match" "broad-match",
              "exact-match" "exact-match",
              "close-match" "close-match"})
            [[?type ?reverse-type]]]
           [?relation :relation/concept-2 ?from-concept]
           [?relation :relation/type ?reverse-type]
           [?relation :relation/concept-1 ?to-concept]]
          [(edge ?from-concept ?type ?to-concept ?relation)
           (or
            (-forward-edge ?from-concept ?type ?to-concept ?relation)
            (-reverse-edge ?from-concept ?type ?to-concept ?relation))]]])
      (set-db (deref conn))
      (set-strategy strategy)))

(defn trace-with-paths [trace]
  (loop [trace trace
         path []
         result []]
    (if (empty? trace)
      result
      (let [[x & trace] trace
            [k begin-or-end] (:type x)
            _ (assert (#{:begin :end} begin-or-end))
            [this-path next-path] (case begin-or-end
                                    :begin [path (conj path k)]
                                    :end (let [p (pop path)]
                                           [p p]))]
        (recur trace next-path (conj result (assoc x :path this-path)))))))

(defn openness [trace-item]
  {:post [(#{:begin :end} %)]}
  (-> trace-item :type second))

(defn end? [x]
  (= :end (openness x)))

(defn raw-acc-tree [trace]
  (let [trace (into [] (filter end?) (trace-with-paths trace))]
    (transduce (filter end?)
               (fn
                 ([dst] dst)
                 ([dst {:keys [path type elapsed-ns]}]
                  (let [[k _] type
                        path (->> k
                                  (conj path)
                                  (into [] (mapcat (fn [x] [:sub x])))
                                  rest)]
                    (update-in dst
                               path
                               (fn [x]
                                 (-> (merge {:elapsed-ns 0 :count 0} x)
                                     (update :count inc)
                                     (update :elapsed-ns + elapsed-ns)))))))
               {}
               trace)))

(declare clean-acc-nodes)

(defn clean-acc-nodes-sub [sub-map parent-time]
  (into {}
        (map (fn [[k v]] [k (clean-acc-nodes v parent-time)]))
        sub-map))

(defn clean-acc-nodes [node parent-time]
  (let [this-time (:elapsed-ns node)]
    (assert this-time)
    (-> node
        (assoc :time-s (* 1.0e-9 this-time)
               :time-rel (/ (double this-time)
                            parent-time))
        (dissoc :elapsed-ns)
        (update :sub clean-acc-nodes-sub this-time))))

(defn acc-tree [trace]
  (let [raw (raw-acc-tree trace)
        total-time (transduce (map :elapsed-ns) + (vals raw))]
    (clean-acc-nodes-sub raw total-time)))

(defn render-tree [tree]
  (let [add-sub (fn [stack indent sub]
                  (into stack
                        (map (fn [[k v]] [indent k v]))
                        sub))]
    (loop [stack (add-sub [] "" tree)]
      (if (empty? stack)
        nil
        (let [[[indent k {:keys [time-s time-rel count sub]}] & stack] stack
              inner-indent (str indent "  ")]
          (println (format "%s* %s (%d):" indent (name k) count))
          (println (format "%sabs: %.3f" inner-indent time-s))
          (println (format "%srel: %d%%" inner-indent (Math/round (* 100 time-rel))))
          (recur (add-sub stack inner-indent sub)))))))

(defn disp-examples [examples]
  (println "The examples:")
  (doseq [[i ex] (map-indexed vector examples)]
    (println (format "%d) %s -> %d %s"
                     i (str (:clause ex))
                     (count (:constrainted-patterns ex))
                     (-> ex :constrainted-patterns
                         first str)))))

(defn crop-rel [rel n]
  {:attrs (:attrs rel)
   :tuples (into [] (comp (take n) (map vec)) (:tuples rel))
   :tuple-count (count (:tuples rel))})

(defn crop-rels [rels n]
  (mapv #(crop-rel % n) rels))

(defn crop-context [context n]
  (-> context
      (dissoc :sources :stats :settings :rules)
      (update :rels crop-rels n)))

(defn crop-example [example n]
  (-> example
      (assoc :source {:max-tx (:max-tx (:source example))})
      (update :constrained-patterns #(into [] (take n) %))
      (assoc :constrained-pattern-count
             (-> example :constrained-patterns count))
      (update :context crop-context n)))

(defn run-example
  ([strategy query-builder]
   (with-connection [conn (deref db)]
     (let [examples (atom [])]
       (with-redefs [dq/log-example (fn [ex] (swap! examples conj ex))]
         (let [query (query-builder conn strategy)
               [result trace] (traced-q query)]
           (render-tree (acc-tree trace))
           (def the-examples (deref examples))
           (disp-examples the-examples)
           (spit "query_examples.edn"
                 (with-out-str
                   (pp/pprint (mapv #(crop-example % 10) the-examples))))
           (count result)))))))

(defn demo0 [] (run-example :new #_dq/expand-once query2))

(defn load-examples []
  (-> "query_examples.edn"
      slurp
      edn/read-string))


(defn exercise-many
  ([f n]
   (dotimes [_i n]
     (f)))
  ([f] (exercise-many f 100)))

(comment

  (def examples )
  
  (demo0)

  (def tr2 (-> the-trace trace-with-paths))

  )
