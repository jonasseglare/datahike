(ns datahike.debug-query
  (:require [datahike.api :as d]
            [datahike.query :as dq]
            [datahike.tools :as tools]
            [taoensso.nippy :as nippy]))

(defn default-data []
  (nippy/thaw-from-file "/home/jonas/prog/jobtech-taxonomy-api/resources/taxonomy.nippy"))

(def db-config {:keep-history? true, :keep-history true, :search-cache-size 10000, :index :datahike.index/persistent-set, :store {:id "in-mem-nippy-data2", :backend :mem}, :name "Nippy testing data", :store-cache-size 1000, :wanderung/type :datahike, :attribute-refs? true, :writer {:backend :self}, :crypto-hash? false, :schema-flexibility :write, :branch :db})

(defn with-connection-fn [db f]
  (let [conn (d/connect db)]
    (try
      (f conn)
      (finally
        (d/release conn)))))

(defn wrap-traced-fn [trace type-key f arg-ks]
  {:pre [(keyword? type-key)
         (ifn? f)
         (sequential? arg-ks)]}
  (fn [& args]
    (swap! trace conj (assoc (zipmap arg-ks args)
                             :type [type-key :begin]
                             :args args))
    (let [start (System/nanoTime)
          result (apply f args)
          end (System/nanoTime)]
      (swap! trace conj {:type [type-key :end]
                         :result result
                         :start-ns start
                         :elapsed-ns (- end start)})
      result)))

(defn summarize-trace-item [x]
  (select-keys x [:type :start-ns :elapsed-ns]))

(defn traced-q
  "Instrument important functions to record a trace of db operations"
  [& args]
  (let [trace (atom [])
        orig-resolve-clause* dq/-resolve-clause*
        orig-lookup-pattern dq/lookup-pattern
        tq (wrap-traced-fn trace
                           :q
                           d/q
                           [])
        result (with-redefs
                 [dq/-resolve-clause* (wrap-traced-fn trace
                                                      :resolve-clause
                                                      orig-resolve-clause*
                                                      [:context :clause :orig-clause])
                  dq/lookup-pattern (wrap-traced-fn trace
                                                    :lookup-pattern
                                                    orig-lookup-pattern
                                                    [:context :source :pattern :orig-pattern])]
                 (apply tq args))]
    [result (deref trace)]))

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
  (update query :args (fn [args] (into [db] args))))

(defn set-strategy [query strategy]
  (assoc-in query [:settings :relprod-strategy] strategy))

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

(defn run-example
  ([strategy query-builder]
   (with-connection [conn (deref db)]
     (let [query (query-builder conn strategy)
           [result trace] (traced-q query)]
       (def the-trace trace)
       (count result)))))

(defn demo0 [] (run-example dq/expand-once query2))

(comment

  


  )
