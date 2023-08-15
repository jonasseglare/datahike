(ns datahike.debug-query
  (:require [datahike.api :as d]
            [taoensso.nippy :as nippy]))

(def find-tx-datoms
  '[:find ?tx ?inst
    :in $ ?bf
    :where
    [?tx :db/txInstant ?inst]
    [(< ?bf ?inst)]])

(def find-datoms-in-tx
  '[:find ?e ?a ?v ?t ?added
    :in $ ?t
    :where
    [?e ?a ?v ?t ?added]
    (not [?e :db/txInstant ?v ?t ?added])])

(defn extract-datahike-data
  "Given a Datahike connection extracts datoms from indices."
  [conn]
  (let [db (d/db conn)
        txs (sort-by first (d/q find-tx-datoms db (java.util.Date. 70)))
        query {:query find-datoms-in-tx
               :args [(d/history db)]}]
    (println "NUMBER OF TXS" (count txs))
    (mapcat
     (fn [[tid tinst]]
       (->> (d/q (update-in query [:args] conj tid))
            (sort-by first)
            (into [[tid :db/txInstant tinst tid true]])))
     txs)))


(defonce datoms (nippy/thaw-from-file "/home/jonas/prog/jobtech-taxonomy-d/taxonomy-v20.nippy"))

(def db-config {:keep-history? true, :keep-history true, :search-cache-size 10000, :index :datahike.index/persistent-set, :store {:id "in-mem-nippy-data2", :backend :mem}, :name "Nippy testing data", :store-cache-size 1000, :wanderung/type :datahike, :attribute-refs? true, :writer {:backend :self}, :crypto-hash? false, :schema-flexibility :write, :branch :db})

(def broader-query
  '{:query
    {:find [?from-id ?id],
     :keys [from_id id],
     :in [$ %

          [?from-id ...] ;; Alla källobject
          ?relation-type ;; Vilket fält det är
          [?type ...]    ;; Typ av målbegrepp


          ],
     :where
     [
      ;; Det finns ett begrepp ?c med id from-id
      [?c :concept/id ?from-id]

      ;; ... en kant från detta begrepp pekar på ett annat begrepp ?related-c
      (edge ?c ?relation-type ?related-c ?r)

      ;; Detta andra begrepp related-c har id ?id
      ;; och typen ?type
      [?related-c :concept/id ?id]
      [?related-c :concept/type ?type]

      ;; Deprecated ska vara false
      ;; https://docs.datomic.com/pro/query/query.html#ground
      [(ground false) ?deprecated]

      
      [(get-else $ ?related-c :concept/deprecated false) ?deprecated]
      ]},
    :args
    [

     ;; Detta är reglerna
     [[(-forward-edge ?from-concept ?type ?to-concept ?relation)
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
        (-reverse-edge ?from-concept ?type ?to-concept ?relation))]]

     ;; Detta är argumenten som man skickar in
     #{"w6ud_quG_dgh"}
     "broader"
     ["occupation-field"]]})

(defn with-example-database [f]
  (let [db (d/create-database db-config)]
    (try
      (f db)
      (finally
        (d/delete-database db-config)))))

(defn with-connection [db f]
  (let [conn (d/connect db)]
    (try
      (f conn)
      (finally
        (d/release conn)))))

(defn run-example []
  (with-example-database
    (fn [dst]
      (with-connection dst
        (fn [conn]
          (deref (d/load-entities conn datoms))))
      (with-connection dst
        (fn [conn]
          (let [;;_ (def extracted (extract-datahike-data conn))
                ;;_ (assert (= extracted datoms))
                path [:args]
                db (d/as-of (deref conn) 536870932)
                query (update-in broader-query path (fn [args] (into [db] args)))
                _ (println "The query")
                _ (println query)
                _ (println "----")
                result (d/q query)]
            result))))))
