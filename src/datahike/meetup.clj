(ns datahike.meetup
  (:require [datahike.api :as datahike]
            [datahike.datom :as datom]
            [clojure.string :as str]))

(defn init-db []
  (let [cfg {:store {:backend :mem
                     :id (str (gensym))}
             :keep-history? true
             ;;:attribute-refs? true
             :schema-flexibility :write}
        _ (datahike/create-database cfg)
        conn (datahike/connect cfg)
        schema [{:db/ident :person/name
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident :person/parent
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many}]]
    (datahike/transact conn schema)
    conn))

(defn print-table [datoms]
  (let [datoms (->> datoms
                    (sort-by (fn [[e _a _v tx _added?]]
                               [tx e]))
                    (into [["ENTITY-ID"
                            "ATTRIBUTE"
                            "VALUE"
                            "TRANSACTION"
                            "OP"]]
                          (map (fn [[e a v tx added?]]
                                 (mapv pr-str [e a v tx
                                               (if added?
                                                 '+
                                                 '-)])))))
        col-widths (->> (apply map vector datoms)
                        (mapv #(transduce (map count) max 0 %)))]
    (println "---- DATOMS")
    (doseq [datom-group (partition-by #(nth % 3) datoms)]
      (doseq [datom datom-group]
        (println (str/join "    "
                           (mapv (fn [s n] (format (str "%" n "s") s))
                                 datom col-widths))))
      (println))))

(defn chronological-datoms [db]
  (->> (datahike/datoms db :eavt)
       ))

(defn demo0 []
  (let [conn (init-db)]
    (-> conn datahike/db chronological-datoms)))

(defn step1 [conn]
  (datahike/transact conn [[:db/add "x" :person/name "August"]])
  conn)

(defn demo1 []
  (let [conn (init-db)]
    (step1 conn)
    (chronological-datoms conn)))

(defn step2 [conn]
  (let [person-of-interest (some (fn [[e _a v]]
                                   (when (= v "August")
                                     e))
                                 (datahike/datoms (datahike/db conn)
                                                  {:index :aevt
                                                   :components [:person/name]}))]
    (datahike/transact conn [[:db/add person-of-interest :person/name "Augustin"]])))

(defn demo2 []
  (let [conn (init-db)]
    (step1 conn)
    (step2 conn)
    (-> conn
        datahike/db
        datahike/history
        chronological-datoms
        print-table)))
