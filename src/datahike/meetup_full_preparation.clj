(ns datahike.meetup-full-preparation
  (:require [datahike.api :as datahike]
            [datahike.meetup-helpers :as mh]))

(comment

  (mh/intro-slideshow)

  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; P O P U L A T I N G   T H E   D A T A B A S E
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


;; First we will create a function that will create a new database
(defn init-db []
  (let [cfg {:store {:backend :mem
                     :id (str (gensym))}
             :keep-history? true
             :schema-flexibility :write}
        _ (datahike/create-database cfg)
        conn (datahike/connect cfg)

        ;; The schema describes how a database is stored
        schema [{:db/ident :person/id
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident :person/name
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one}
                {:db/ident :person/parent
                 :db/valueType :db.type/ref
                 :db/cardinality :db.cardinality/many}]]
    (datahike/transact conn schema)
    conn))

;; Now, let's write a function to see what the database contains
(defn demo0 []
  (let [conn (init-db)]
    (-> conn
        datahike/db
        (datahike/datoms :eavt)
        mh/display-datoms)))



;; Add some data to the database
(defn step1 [conn]
  (datahike/transact conn [[:db/add "tmp" :person/name "August"]
                           [:db/add "tmp" :person/id "001"]]))

(defn demo1 []
  (let [conn (init-db)]
    (step1 conn)
    (-> conn
        datahike/db
        (datahike/datoms :eavt)
        mh/display-datoms)))


(defn find-entity-id-by-person-id [conn person-id]
  {:post [(number? %)]}
  (some (fn [[e _a v]]
          (when (= v person-id)
            e))
        (datahike/datoms (datahike/db conn)
                         {:index :aevt
                          :components [:person/id]})))

;; Change the name
(defn step2 [conn]
  (let [entity-id (find-entity-id-by-person-id conn "001")]
    (datahike/transact conn [[:db/add entity-id :person/name "Augustin"]])))

(defn demo2 []
  (let [conn (init-db)]
    (step1 conn)
    (step2 conn)
    (-> conn
        datahike/db
        datahike/history
        (datahike/datoms :eavt)
        mh/display-datoms)))

;; Let's give August a parent

(defn step3 [conn]
  (let [entity-id (find-entity-id-by-person-id conn "001")]
    (datahike/transact conn [[:db/add entity-id :person/parent "tmp"]
                             [:db/add "tmp" :person/id "002"]
                             [:db/add "tmp" :person/name "Jonas"]])))

(defn demo3 []
  (let [conn (init-db)]
    (step1 conn)
    (step2 conn)
    (step3 conn)
    (-> conn
        datahike/db
        datahike/history
        (datahike/datoms :eavt)
        mh/display-datoms)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; Q U E R Y I N G   T H E   D A T A B A S E
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn prepare-db []
  (let [conn (init-db)]
    (step1 conn)
    (step2 conn)
    (step3 conn)
    conn))

(comment

  (def the-conn (prepare-db))

  )

;; Let's query all persons
(defn demo4 []
  (let [conn (prepare-db)]
    (mh/disp-q '[:find ?e ?id ?name ;; SQL Select
                 :in $              ;; SQL From 
                 :where             ;; SQL where

                 ;; Clauses:
                 [?e :person/name ?name]
                 [?e :person/id ?id]]

               ;; The database
               (datahike/db conn))))

(defn demo5 []
  (let [conn (prepare-db)]
    (mh/disp-traced-q '[:find ?e ?id ?name ;; SQL Select
                        :in $              ;; SQL From 
                        :where             ;; SQL where

                        ;; Clauses:
                        [?e :person/name ?name]
                        [?e :person/id ?id]]

                      ;; The database
                      (datahike/db conn))))

;; Let's try to find a person by id

(defn find-person-by-id [conn id]
  (mh/disp-traced-q '[:find ?e
                      :in $ ?id
                      :where
                      [?e :person/id ?id]]
                    (datahike/db conn)
                    id))

(defn find-parent-id [conn child-id]
  (mh/disp-traced-q '[:find ?parent-id
                      :in $ ?child-id
                      :where
                      [?child :person/id ?child-id]
                      [?child :person/parent ?parent]
                      [?parent :person/id ?parent-id]]
                    (datahike/db conn)
                    child-id))


(comment

  (mh/final-slides)

  )
