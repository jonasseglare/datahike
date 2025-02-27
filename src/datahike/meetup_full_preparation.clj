(ns datahike.meetup-full-preparation
  (:require [datahike.api :as datahike]
            [datahike.datom :as datom]
            [datahike.meetup-helpers :as mh]
            [clojure.string :as str]))

(comment

  (mh/intro-slideshow)

  )



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

(defn chronological-datoms [db]
  (->> (datahike/datoms db :eavt)
       ))

(defn demo0 []
  (let [conn (init-db)]
    (-> conn
        datahike/db
        (datahike/datoms :eavt)
        mh/print-db-datoms)))

(defn step1 [conn]
  (datahike/transact conn [[:db/add "x" :person/name "August"]])
  conn)

(defn demo1 []
  (let [conn (init-db)]
    (step1 conn)
    (-> conn datahike/db (datahike/datoms :eavt) )))

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
        mh/print-db-datoms)))






