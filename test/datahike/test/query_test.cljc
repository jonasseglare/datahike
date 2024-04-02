(ns datahike.test.query-test
  (:require
   #?(:cljs [cljs.test    :as t :refer-macros [is deftest testing]]
      :clj  [clojure.test :as t :refer        [is deftest testing]])
   [datahike.api :as d]
   [datahike.db :as db]
   [datahike.test.utils :as utils]
   [datahike.query :as dq])
  #?(:clj
     (:import [clojure.lang ExceptionInfo])))

#?(:cljs (def Throwable js/Error))

(deftest test-joins
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                           {:db/id 2, :name  "Petr", :age   37}
                           {:db/id 3, :name  "Ivan", :age   37}
                           {:db/id 4, :age 15}]))]
    (is (= (d/q '[:find ?e
                  :where [?e :name]] db)
           #{[1] [2] [3]}))
    (is (= (d/q '[:find  ?e ?v
                  :where [?e :name "Ivan"]
                  [?e :age ?v]] db)
           #{[1 15] [3 37]}))
    (is (= (d/q '[:find  ?e1 ?e2
                  :where [?e1 :name ?n]
                  [?e2 :name ?n]] db)
           #{[1 1] [2 2] [3 3] [1 3] [3 1]}))
    (is (= (d/q '[:find  ?e ?e2 ?n
                  :where [?e :name "Ivan"]
                  [?e :age ?a]
                  [?e2 :age ?a]
                  [?e2 :name ?n]] db)
           #{[1 1 "Ivan"]
             [3 3 "Ivan"]
             [3 2 "Petr"]}))))

(deftest test-q-many
  (let [db (-> (db/empty-db {:aka {:db/cardinality :db.cardinality/many}})
               (d/db-with [[:db/add 1 :name "Ivan"]
                           [:db/add 1 :aka  "ivolga"]
                           [:db/add 1 :aka  "pi"]
                           [:db/add 2 :name "Petr"]
                           [:db/add 2 :aka  "porosenok"]
                           [:db/add 2 :aka  "pi"]]))]
    (is (= (d/q '[:find  ?n1 ?n2
                  :where [?e1 :aka ?x]
                  [?e2 :aka ?x]
                  [?e1 :name ?n1]
                  [?e2 :name ?n2]] db)
           #{["Ivan" "Ivan"]
             ["Petr" "Petr"]
             ["Ivan" "Petr"]
             ["Petr" "Ivan"]}))))

(deftest test-q-coll
  (let [db [[1 :name "Ivan"]
            [1 :age  19]
            [1 :aka  "dragon_killer_94"]
            [1 :aka  "-=autobot=-"]]]
    (is (= (d/q '[:find  ?n ?a
                  :where [?e :aka "dragon_killer_94"]
                  [?e :name ?n]
                  [?e :age  ?a]] db)
           #{["Ivan" 19]})))

  (testing "Query over long tuples"
    (let [db [[1 :name "Ivan" 945 :db/add]
              [1 :age  39     999 :db/retract]]]
      (is (= (d/q '[:find  ?e ?v
                    :where [?e :name ?v]] db)
             #{[1 "Ivan"]}))
      (is (= (d/q '[:find  ?e ?a ?v ?t
                    :where [?e ?a ?v ?t :db/retract]] db)
             #{[1 :age 39 999]})))))

(deftest test-q-in
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name "Ivan", :age 15}
                           {:db/id 2, :name "Petr", :age 37}
                           {:db/id 3, :name "Ivan", :age 37}]))
        query '{:find  [?e]
                :in    [$ ?attr ?value]
                :where [[?e ?attr ?value]]}]
    (is (= (d/q query db :name "Ivan")
           #{[1] [3]}))
    (is (= (d/q query db :age 37)
           #{[2] [3]}))

    (testing "Named DB"
      (is (= (d/q '[:find  ?a ?v
                    :in    $db ?e
                    :where [$db ?e ?a ?v]] db 1)
             #{[:name "Ivan"]
               [:age 15]})))

    (testing "DB join with collection"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ $b
                    :where [?e :name ?n]
                    [$b ?n ?email]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Query without DB"
      (is (= (d/q '[:find ?a ?b
                    :in   ?a ?b]
                  10 20)
             #{[10 20]})))))

(deftest test-bindings
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name "Ivan", :age 15}
                           {:db/id 2, :name "Petr", :age 37}
                           {:db/id 3, :name "Ivan", :age 37}]))]
    (testing "Relation binding"
      (is (= (d/q '[:find  ?e ?email
                    :in    $ [[?n ?email]]
                    :where [?e :name ?n]]
                  db
                  [["Ivan" "ivan@mail.ru"]
                   ["Petr" "petr@gmail.com"]])
             #{[1 "ivan@mail.ru"]
               [2 "petr@gmail.com"]
               [3 "ivan@mail.ru"]})))

    (testing "Tuple binding"
      (is (= (d/q '[:find  ?e
                    :in    $ [?name ?age]
                    :where [?e :name ?name]
                    [?e :age ?age]]
                  db ["Ivan" 37])
             #{[3]})))

    (testing "Collection binding"
      (is (= (d/q '[:find  ?attr ?value
                    :in    $ ?e [?attr ...]
                    :where [?e ?attr ?value]]
                  db 1 [:name :age])
             #{[:name "Ivan"] [:age 15]})))

    (testing "Empty coll handling"
      (is (= (d/q '[:find ?id
                    :in $ [?id ...]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{}))
      (is (= (d/q '[:find ?id
                    :in $ [[?id]]
                    :where [?id :age _]]
                  [[1 :name "Ivan"]
                   [2 :name "Petr"]]
                  [])
             #{})))

    (testing "Placeholders"
      (is (= (d/q '[:find ?x ?z
                    :in [?x _ ?z]]
                  [:x :y :z])
             #{[:x :z]}))
      (is (= (d/q '[:find ?x ?z
                    :in [[?x _ ?z]]]
                  [[:x :y :z] [:a :b :c]])
             #{[:x :z] [:a :c]})))

    (testing "Error reporting"
      (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to tuple \[\?a \?b\]"
                            (d/q '[:find ?a ?b :in [?a ?b]] :a)))
      (is (thrown-with-msg? ExceptionInfo #"Cannot bind value :a to collection \[\?a \.\.\.\]"
                            (d/q '[:find ?a :in [?a ...]] :a)))
      (is (thrown-with-msg? ExceptionInfo #"Not enough elements in a collection \[:a\] to bind tuple \[\?a \?b\]"
                            (d/q '[:find ?a ?b :in [?a ?b]] [:a]))))))

(deftest test-nested-bindings
  (is (= (d/q '[:find  ?k ?v
                :in    [[?k ?v] ...]
                :where [(> ?v 1)]]
              {:a 1, :b 2, :c 3})
         #{[:b 2] [:c 3]}))

  (is (= (d/q '[:find  ?k ?min ?max
                :in    [[?k ?v] ...] ?minmax
                :where [(?minmax ?v) [?min ?max]]
                [(> ?max ?min)]]
              {:a [1 2 3 4]
               :b [5 6 7]
               :c [3]}
              #(vector (reduce min %) (reduce max %)))
         #{[:a 1 4] [:b 5 7]}))

  (is (= (d/q '[:find  ?k ?x
                :in    [[?k [?min ?max]] ...] ?range
                :where [(?range ?min ?max) [?x ...]]
                [(even? ?x)]]
              {:a [1 7]
               :b [2 4]}
              range)
         #{[:a 2] [:a 4] [:a 6]
           [:b 2]})))

(deftest test-offset
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Alice", :age   15}
                           {:db/id 2, :name  "Bob", :age   37}
                           {:db/id 3, :name  "Charlie", :age   37}]))]
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :limit -1}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :limit nil}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :offset -1}))))
    (is (= 3 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :offset nil}))))
    (is (= 1 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :offset 1
                          :limit 1}))))
    (is (= 2 (count (d/q {:query '[:find ?e :where [?e :name _]]
                          :args [db]
                          :limit 2}))))
    (is (= 2 (count  (d/q {:query '[:find ?e :where [?e :name _]]
                           :args [db]
                           :offset 1
                           :limit 2}))))
    (is (= 1 (count  (d/q {:query '[:find ?e :where [?e :name _]]
                           :args [db]
                           :offset 2
                           :limit 2}))))
    (is (not (= (d/q {:query '[:find ?e :where [?e :name _]]
                      :args [db]
                      :limit 2})
                (d/q {:query '[:find ?e :where [?e :name _]]
                      :args [db]
                      :offset 1
                      :limit 2}))))
    (is (= (d/q {:query '[:find ?e :where [?e :name _]]
                 :args [db]
                 :offset 4})
           #{}))
    (is (= (d/q {:query '[:find ?e :where [?e :name _]]
                 :args [db]
                 :offset 10
                 :limit 5})
           #{}))
    (is (= (d/q {:query '[:find ?e :where [?e :name _]]
                 :args [db]
                 :offset 1
                 :limit 0})
           #{}))))

(deftest test-return-maps
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Alice", :age   15}
                           {:db/id 2, :name  "Bob", :age   37}
                           {:db/id 3, :name  "Charlie", :age   37}]))]
    (testing "returns map"
      (is (map? (first (d/q {:query '[:find ?e :keys name :where [?e :name _]]
                             :args [db]})))))
    (testing "returns set without return-map"
      (is (= #{["Charlie"] ["Alice"] ["Bob"]}
             (d/q {:query '[:find ?name :where [_ :name ?name]]
                   :args [db]}))))
    (testing "returns map with key return-map"
      (is (= [{:foo 3} {:foo 2} {:foo 1}]
             (d/q {:query '[:find ?e :keys foo :where [?e :name _]]
                   :args [db]}))))
    (testing "returns map with string return-map"
      (is (= [{"foo" "Charlie"} {"foo" "Alice"} {"foo" "Bob"}]
             (d/q {:query '[:find ?name :strs foo :where [?e :name ?name]]
                   :args [db]}))))
    (testing "return map with keys using multiple find vars"
      (is (= #{["Bob" {:age 37 :db/id 2}]
               ["Charlie" {:age 37 :db/id 3}]
               ["Alice" {:age 15 :db/id 1}]}
             (into #{} (d/q {:find '[?name (pull ?e ?p)]
                             :args [db '[:age :db/id]]
                             :in '[$ ?p]
                             :where '[[?e :name ?name]]})))))))

(deftest test-memoized-parse-query
  (testing "no map return"
    (is (= nil
           (:qreturnmap (dq/memoized-parse-query '[:find ?e :where [?e :name]])))))
  (testing "key map return"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :keys, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e :keys foo :where [?e :name]])))))
  (testing "key map return multiple"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :keys, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo}, #datalog.parser.type.MappingKey{:mapping-key bar})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e ?f :keys foo bar :where [?e :name ?f]])))))
  (testing "string map return multiple"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :strs, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo}, #datalog.parser.type.MappingKey{:mapping-key bar})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e ?f :strs foo bar :where [?e :name ?f]])))))
  (testing "symbol map return multiple"
    (is (= '#datalog.parser.type.ReturnMaps{:mapping-type :syms, :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo}, #datalog.parser.type.MappingKey{:mapping-key bar})}
           (:qreturnmaps (dq/memoized-parse-query '[:find ?e ?f :syms foo bar :where [?e :name ?f]]))))))

(deftest test-convert-to-return-maps
  (testing "converting keys"
    (is (= [{:foo 3} {:foo 2} {:foo 1}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :keys,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
                                      #{[1] [2] [3]}))))
  (testing "converting strs"
    (is (= [{"foo" 3} {"foo" 2} {"foo" 1}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :strs,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
                                      #{[1] [2] [3]}))))
  (testing "converting syms"
    (is (= [{'foo 3} {'foo 2} {'foo 1}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :syms,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo})}
                                      #{[1] [2] [3]}))))
  (testing "converting keys"
    (is (= '[{:foo 1, :bar 11, :baz "Ivan"} {:foo 3, :bar 21, :baz "Petr"} {:foo 3, :bar 31, :baz "Ivan"}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :keys,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key bar},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key baz})}
                                      #{[1 11 "Ivan"]
                                        [3 31 "Ivan"]
                                        [3 21 "Petr"]}))))
  (testing "converting strs"
    (is (= '[{"foo" 1, "bar" 11, "baz" "Ivan"} {"foo" 3, "bar" 21, "baz" "Petr"} {"foo" 3, "bar" 31, "baz" "Ivan"}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :strs,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key bar},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key baz})}
                                      #{[1 11 "Ivan"]
                                        [3 31 "Ivan"]
                                        [3 21 "Petr"]}))))
  (testing "converting syms"
    (is (= '[{foo 1, bar 11, baz "Ivan"} {foo 3, bar 21, baz "Petr"} {foo 3, bar 31, baz "Ivan"}]
           (dq/convert-to-return-maps '#datalog.parser.type.ReturnMaps{:mapping-type :syms,
                                                                       :mapping-keys (#datalog.parser.type.MappingKey{:mapping-key foo},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key bar},
                                                                                      #datalog.parser.type.MappingKey{:mapping-key baz})}
                                      #{[1 11 "Ivan"]
                                        [3 31 "Ivan"]
                                        [3 21 "Petr"]})))))

#_(deftest test-clause-order-invariance                     ;; TODO: this is what should happen after rewirite of query engine
    (let [db (-> (db/empty-db)
                 (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                             {:db/id 2, :name  "Petr", :age   37}
                             {:db/id 3, :name  "Ivan", :age   37}
                             {:db/id 4, :age 15}]))]
      (testing "Clause order does not matter for predicates"
        (is (= (d/q {:query '{:find [?e]
                              :where [[?e :age ?age]
                                      [(= ?age 37)]]}
                     :args [db]})
               #{[2] [3]}))
        (is (= (d/q {:query '{:find [?e]
                              :where [[(= ?age 37)]
                                      [?e :age ?age]]}
                     :args [db]})
               #{[2] [3]})))))

(deftest test-clause-order
  (let [db (-> (db/empty-db)
               (d/db-with [{:db/id 1, :name  "Ivan", :age   15}
                           {:db/id 2, :name  "Petr", :age   37}
                           {:db/id 3, :name  "Ivan", :age   37}
                           {:db/id 4, :age 15}]))]
    (testing "Predicate clause before variable binding throws exception"
      (is (= (d/q {:query '{:find [?e]
                            :where [[?e :age ?age]
                                    [(= ?age 37)]]}
                   :args [db]})
             #{[2] [3]}))
      (is (thrown-with-msg? Throwable #"Insufficient bindings: #\{\?age\} not bound"
                            (d/q {:query '{:find [?e]
                                           :where [[(= ?age 37)]
                                                   [?e :age ?age]]}
                                  :args [db]}))))))

(deftest test-zeros-in-pattern
  (let [cfg {:store {:backend :mem
                     :id "sandbox"}
             :schema-flexibility :write
             :attribute-refs? false}
        conn (do
               (d/delete-database cfg)
               (d/create-database cfg)
               (d/connect cfg))]
    (d/transact conn [{:db/ident :version/id
                       :db/valueType :db.type/long
                       :db/cardinality :db.cardinality/one
                       :db/unique :db.unique/identity}
                      {:version/id 0}
                      {:version/id 1}])
    (is (= 1
           (count (d/q '[:find ?t :in $ :where
                         [?t :version/id 0]]
                       @conn))))
    (d/release conn)))

;; https://github.com/replikativ/datahike/issues/471
(deftest keyword-keys-test
  (let [schema [{:db/ident       :name
                 :db/cardinality :db.cardinality/one
                 :db/index       true
                 :db/unique      :db.unique/identity
                 :db/valueType   :db.type/string}
                {:db/ident       :parents
                 :db/cardinality :db.cardinality/many
                 :db/valueType   :db.type/ref}
                {:db/ident       :age
                 :db/cardinality :db.cardinality/one
                 :db/valueType   :db.type/long}]
        cfg    {:store              {:backend :mem
                                     :id      "DEV"}
                :schema-flexibility :write
                :attribute-refs?    true}
        conn   (utils/setup-db cfg)]
    (d/transact conn schema)
    (d/transact conn [{:name "Alice"
                       :age  25}
                      {:name "Bob"
                       :age  35}])
    (d/transact conn [{:name    "Charlie"
                       :age     5
                       :parents [[:name "Alice"] [:name "Bob"]]}])
    (let [db             @conn
          keyword-result (into #{} (d/q '[:find ?n ?a
                                          :keys :name :age
                                          :where
                                          [?e :name ?n]
                                          [?e :age ?a]]
                                        db))
          symbol-result  (into #{} (d/q '[:find ?n ?a
                                          :keys name age
                                          :where
                                          [?e :name ?n]
                                          [?e :age ?a]]
                                        db))]
      (testing "keyword result keys"
        (is (= #{{:name "Alice" :age 25}
                 {:name "Charlie" :age 5}
                 {:name "Bob" :age 35}}
               keyword-result)))
      (testing "keyword equals symbol keys"
        (is (= symbol-result
               keyword-result))))
    (d/release conn)))

(deftest test-normalize-q-input
  (testing "query as vector"
    (is (= {:query {:find '[?n]
                    :where '[[?e :name ?n]]}
            :args :db}
           (dq/normalize-q-input '[:find ?n
                                   :where [?e :name ?n]]
                                 :db))))

  (testing "query in :query field"
    (is (= {:query {:find '[?n]
                    :where '[[?e :name ?n]]}
            :args [:db]}
           (dq/normalize-q-input {:query '{:find [?n]
                                           :where [[?e :name ?n]]}
                                  :args [:db]}
                                 [])))
    (is (= {:query {:find '[?n], :where '[[?e :name ?n]]}
            :args [:db]}
           (dq/normalize-q-input {:query '{:find [?n]
                                           :where [[?e :name ?n]]}}
                                 [:db])))
    (is (= {:query {:find '[?n], :where '[[?e :name ?n]]}
            :args [:db]}
           (dq/normalize-q-input {:query '{:find [?n]
                                           :where [[?e :name ?n]]}
                                  :args [:db]}
                                 [:db2])))
    (is (= {:query {:find '[?n]
                    :where '[[?e :name ?n]]}
            :args [:db]
            :limit 100
            :offset 0}
           (dq/normalize-q-input {:query '[:find ?n
                                           :where [?e :name ?n]]
                                  :offset 0
                                  :limit 100
                                  :args [:db]}
                                 []))))

  (testing "query in top-level map"
    (is (= {:query {:find '[?e]
                    :where '[[?e :name ?value]]}
            :args []
            :limit 100
            :offset 0}
           (dq/normalize-q-input {:find '[?e]
                                  :where '[[?e :name ?value]]
                                  :offset 0
                                  :limit 100} [])))))

(deftest test-distinct-tuples
  (is (= [[3 4]] (dq/distinct-tuples [[3 4]])))
  (let [arrays [(object-array [:a]) (object-array [:a])]
        result (dq/distinct-tuples arrays)
        object-array-type (type (object-array []))]
    (is (every? #(= object-array-type (type %)) result))
    (is (= [[:a]] (map vec result)))

    ;; This is just to highlight the difference w.r.t. `distinct`:
    (is (= [[:a] [:a]] (map vec (distinct arrays)))))
  (is (= [[3 4]] (dq/distinct-tuples [[3 4] [3 4]])))
  (is (= [[3 4]] (dq/distinct-tuples [[3 4]
                                      (long-array [3 4])])))
  (is (= [[3 4] [9 7]] (dq/distinct-tuples [[3 4] [9 7] [3 4]]))))

(defn simple-rel
  ([v values] (simple-rel v values {}))
  ([v values extra]
   (dq/->Relation (merge {v 0} extra) (map vector values))))

(deftest test-relprod
  (let [x (simple-rel '?x [1 2 3 4])
        y (simple-rel '?y [90] {'?w 1})
        z (simple-rel '?z [10 11 12])
        rels [x y z]
        xy-vars ['?x '?y]
        rel-data (dq/expansion-rel-data rels xy-vars)
        relprod (dq/init-relprod rel-data xy-vars)
        relprod-x (dq/relprod-select-keys relprod #{['?x]})
        relprod-xy (dq/relprod-select-keys relprod #{['?x] ['?y]})
        relprod-xy2 (dq/select-all relprod)
        relprod-y (dq/select-simple relprod)
        prodks (comp set keys :attrs :product)]
    (is (= #{} (dq/relprod-vars relprod-x)))
    (is (= #{'?x} (dq/relprod-vars relprod-x :include)))
    (is (= #{'?y} (dq/relprod-vars relprod-x :exclude)))
    (is (= #{'?x '?y} (dq/relprod-vars relprod-x :include :exclude)))
    (is (= 2 (count rel-data)))
    (is (= [{:rel x
             :tuple-count 4
             :vars ['?x]}
            {:rel y
             :tuple-count 1
             :vars ['?y]}]
           rel-data))
    (is (sequential? (:exclude relprod)))
    (is (= 2 (count (:exclude relprod))))
    (is (= 1 (count (:exclude relprod-x))))
    (is (= 0 (count (:exclude relprod-xy))))
    (is (= #{'?x} (prodks relprod-x)))
    (is (= #{'?x '?y '?w} (prodks relprod-xy)))
    (is (= #{'?x '?y '?w} (prodks relprod-xy2)))
    (is (= #{'?y '?w} (prodks relprod-y)))

    (doseq [{:keys [include exclude vars]} [relprod relprod-x relprod-xy relprod-xy2 relprod-y]]
      (is (= 2 (+ (count include)
                  (count exclude))))
      (is (= xy-vars vars)))))






;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; W I P   T E S T S
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def ex0 '{:source {:max-tx 536926163},
           :pattern1 [?oc 48 "occupation-name"],
           :context {:rels [], :consts {}},
           :clause [?oc :concept/type "occupation-name"],
           :constrained-patterns [[?oc 48 "occupation-name"]],
           :constrained-pattern-count 1})

;; A good one
(def ex1 '{:source {:max-tx 536926163},
           :pattern1 [?r1 79 ?oc],
           :context
           {:rels
            [{:attrs {?oc 0},
              :tuples
              [[5289]
               [5294]
               [5299]
               [5304]
               [5307]
               [5310]
               [5313]
               [5317]
               [5322]
               [5325]],
              :tuple-count 3654}
             {:attrs {?__auto__1 0}, :tuples [], :tuple-count 0}],
            :consts {?__auto__1 "narrow-match"}},
           :clause [?r1 :relation/concept-1 ?oc],
           :constrained-patterns
           [[?r1 79 5289]
            [?r1 79 5294]
            [?r1 79 5299]
            [?r1 79 5304]
            [?r1 79 5307]
            [?r1 79 5310]
            [?r1 79 5313]
            [?r1 79 5317]
            [?r1 79 5322]
            [?r1 79 5325]],
           :constrained-pattern-count 3654})

(def ex2 '{:source {:max-tx 536926163},
           :pattern1 [?r1 81 "narrow-match"],
           :context
           {:rels
            [{:attrs {?__auto__1 0}, :tuples [], :tuple-count 0}
             {:attrs {?oc 0, ?r1 1},
              :tuples
              [[5289 5290]
               [5289 5292]
               [5289 42370]
               [5289 44778]
               [5289 45135]
               [5289 45137]
               [5289 54795]
               [5289 58829]
               [5289 59931]
               [5289 89838]],
              :tuple-count 61136}],
            :consts {?__auto__1 "narrow-match"}},
           :clause [?r1 :relation/type ?__auto__1],
           :constrained-patterns
           [[5290 81 "narrow-match"]
            [5292 81 "narrow-match"]
            [42370 81 "narrow-match"]
            [44778 81 "narrow-match"]
            [45135 81 "narrow-match"]
            [45137 81 "narrow-match"]
            [54795 81 "narrow-match"]
            [58829 81 "narrow-match"]
            [59931 81 "narrow-match"]
            [89838 81 "narrow-match"]],
           :constrained-pattern-count 61136})

(def ex3 '{:source {:max-tx 536926163},
           :pattern1 [?r1 80 ?esco],
           :context
           {:rels
            [{:attrs {?oc 0, ?r1 1, ?__auto__1 2},
              :tuples [],
              :tuple-count 0}],
            :consts {?__auto__1 "narrow-match"}},
           :clause [?r1 :relation/concept-2 ?esco],
           :constrained-patterns [],
           :constrained-pattern-count 0})

(def ex4 '{:source {:max-tx 536926163},
           :pattern1 [?r1 80 ?oc],
           :context
           {:rels
            [{:attrs {?oc 0},
              :tuples
              [[5289]
               [5294]
               [5299]
               [5304]
               [5307]
               [5310]
               [5313]
               [5317]
               [5322]
               [5325]],
              :tuple-count 3654}
             {:attrs {?__auto__1 0, ?reverse-type__auto__3 1},
              :tuples [["narrow-match" "broad-match"]],
              :tuple-count 1}],
            :consts {?__auto__1 "narrow-match"}},
           :clause [?r1 :relation/concept-2 ?oc],
           :constrained-patterns
           [[?r1 80 5289]
            [?r1 80 5294]
            [?r1 80 5299]
            [?r1 80 5304]
            [?r1 80 5307]
            [?r1 80 5310]
            [?r1 80 5313]
            [?r1 80 5317]
            [?r1 80 5322]
            [?r1 80 5325]],
           :constrained-pattern-count 3654})

(deftest test-new-search-strategy
  (let [;; pattern1 = [?r1 79 ?oc]
        {:keys [context pattern1]} ex1
        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)

        clean-pattern (dq/replace-unbound-symbols-by-nil bsm pattern1)

        strategy0 [nil :substitute :substitute nil]
        strategy1 [nil :substitute :filter nil]


        subst-inds0 (dq/substitution-relation-indices
                     bsm clean-pattern strategy0)
        subst-inds1 (dq/substitution-relation-indices
                     bsm clean-pattern strategy1)
        filt-inds0 (dq/filtering-relation-indices
                    bsm clean-pattern strategy0 subst-inds0)
        filt-inds1 (dq/filtering-relation-indices
                    bsm clean-pattern strategy1 subst-inds1)]
    (is (seq rels))
    (is (= '{?oc {:relation-index 0, :tuple-element-index 0},
             ?__auto__1 {:relation-index 1, :tuple-element-index 0}}
           bsm))
    (is (= #{0} subst-inds0))
    (is (= #{} subst-inds1))
    (is (= #{} filt-inds0))
    (is (= #{0} filt-inds1))))




(deftest test-substitution-plan
  (let [-pattern1 '[?w ?x ?y]
        context '{:rels [{:attrs {?x 0
                                  ?y 1}
                          :tuples [[1 2]
                                   [3 4]
                                   [3 5]
                                   [5 6]]}
                         {:attrs {?z 0}
                          :tuples [[9] [10] [11]]}]}
        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)
        clean-pattern (dq/replace-unbound-symbols-by-nil bsm -pattern1)
        strategy [nil :substitute :filter nil]
        subst-inds (dq/substitution-relation-indices
                    bsm clean-pattern strategy)
        filt-inds (dq/filtering-relation-indices
                   bsm clean-pattern strategy subst-inds)
        subst-plan (dq/substitution-plan
                    bsm clean-pattern strategy rels subst-inds)]
    (is (= #{0} subst-inds))
    (is (= #{} filt-inds))
    (is (= {'?x {:relation-index 0 :tuple-element-index 0}
            '?y {:relation-index 0 :tuple-element-index 1}
            '?z {:relation-index 1 :tuple-element-index 0}}
           bsm))
    (is (= '([[nil 1 nil nil] [[(2) #{2}]]]
             [[nil 3 nil nil] [[(2) #{4 5}]]]
             [[nil 5 nil nil] [[(2) #{6}]]])
           subst-plan))))

(deftest test-index-feature-extractor
  (let [e (dq/index-feature-extractor [1])]
    (is (= 3 (e [119 3])))
    (is (= 4 (e [120 4 9 3]))))
  (let [e (dq/index-feature-extractor [1 0])]
    (is (= [3 119] (e [119 3])))
    (is (= [4 120] (e [120 4 9 3]))))
  (let [e (dq/index-feature-extractor [])]
    (is (nil? (e [119 3])))
    (is (nil? (e [120 4 9 3])))))

(deftest test-filtering-plan
  (let [pattern1 '[?w ?x ?y]
        context '{:rels [{:attrs {?x 0}
                          :tuples [[1]
                                   [3]
                                   [5]]}
                         {:attrs {?y 0}
                          :tuples [[2] [4] [6]]}
                         {:attrs {?z 0}
                          :tuples [[9] [10] [11]]}]}

        rels (vec (:rels context))
        bsm (dq/bound-symbol-map rels)
        clean-pattern (dq/replace-unbound-symbols-by-nil bsm pattern1)
        strategy [nil :substitute :filter nil]
        subst-inds (dq/substitution-relation-indices bsm pattern1 strategy)
        filt-inds (dq/filtering-relation-indices bsm clean-pattern
                                                 strategy subst-inds)
        subst-plan (dq/substitution-plan
                    bsm clean-pattern strategy rels subst-inds)
        filt-plan (dq/filtering-plan
                   bsm clean-pattern strategy rels filt-inds)

        dfilter (dq/datom-filter (first filt-plan))

        bfn (dq/search-batch-fn bsm clean-pattern rels)]
    (is (= '[nil ?x ?y nil] clean-pattern))
    (is (= #{0} subst-inds))
    (is (= #{1} filt-inds))
    (is (= {'?x {:relation-index 0 :tuple-element-index 0}
            '?y {:relation-index 1 :tuple-element-index 0}
            '?z {:relation-index 2 :tuple-element-index 0}}
           bsm))
    (is (= '([[nil 1 nil nil] []]
             [[nil 3 nil nil] []]
             [[nil 5 nil nil] []])
           subst-plan))
    (is (= '[#{4 6 2}] (map second filt-plan)))
    (is (= [[1 3 2]]
           (into []
                 dfilter
                 [[1 3 2]
                  [1 9 7]])))
    (is (= [] (bfn strategy (fn [[_e _a _v _t]] []))))))


