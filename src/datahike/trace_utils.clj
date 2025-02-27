(ns datahike.trace-utils
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [jobtechdev.html-report :as r]
            [datahike.api :as d]
            [clojure.walk :refer [postwalk]]
            [datahike.query :as dq]
            [clojure.string :as str]
            [clojure.pprint :as pp]
            [jobtech-cljutils.core :as ju]))

(defn default-data []
  (nippy/thaw-from-file (io/resource "taxonomy.nippy")))

(def db-config {:keep-history? true, :keep-history true, :search-cache-size 10000, :index :datahike.index/persistent-set, :store {:id "in-mem-nippy-data2", :backend :mem}, :name "Nippy testing data", :store-cache-size 1000, :wanderung/type :datahike, :attribute-refs? true, :writer {:backend :self}, :crypto-hash? false, :schema-flexibility :write, :branch :db})

(defn with-connection [db f]
  (let [conn (d/connect db)]
    (try
      (f conn)
      (finally
        (d/release conn)))))

(defn create-populated-database []
  (let [db (d/create-database db-config)]
    (with-connection db
      (fn [conn]
        (deref (d/load-entities conn (default-data)))))
    (println "Database created and populated.")
    db))

(defonce db (delay (create-populated-database)))

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

(defn parse-trace
  ([trace] (parse-trace trace {}))
  ([trace settings]
   (let [settings (merge {:dont-wrap #{}} settings)
         dont-wrap (set (:dont-wrap settings))
         completed-log-item? (fn [{:keys [type]}]
                               (or (keyword? type)
                                   (and (vector? type)
                                        (contains? dont-wrap (first type)))))]
     (reverse
      (reduce (fn [stack x]
                (let [[type1 opening] (:type x)]
                  (if (contains? dont-wrap type1)
                    (conj stack x)
                    (case opening
                      :begin (conj stack x)
                      :end (let [[popped stack]
                                 (split-with completed-log-item? stack)
                                 [begin & stack] stack]
                             (conj stack
                                   (merge begin
                                          x
                                          {:type type1
                                           :children (reverse popped)})))))))
              '()
              trace)))))

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
    [result (-> trace deref (parse-trace {:dont-wrap [:q]}))]))

(defn trace-main-type [x]
  (if (vector? x)
    (first x)
    x))

(defn resolve-clause? [x]
  (= :resolve-clause (:type x)))

(defn get-resolve-clause-children [x]
  (when (and (resolve-clause? x)
             (some resolve-clause? (:children x)))
    (:children x)))

(defn flattening-resolve-clause [step]
  (fn self
    ([dst] (step dst))
    ([dst x]
     (if-let [children (seq (get-resolve-clause-children x))]
       (reduce self dst children)
       (step dst x)))))

(defn page-items [trace]
  (into []
        (comp (remove (fn [x] (= :q (trace-main-type (:type x)))))
              flattening-resolve-clause
              (map-indexed (fn [i x] (assoc x :page-index i))))
        trace))

(defn relation-summary-hiccup [rels]
  (r/table-hiccup [["Count" :count]
                   ["Symbols" :symbols]]
                  (for [{:keys [attrs tuples]} rels]
                    {:count (count tuples)
                     :symbols [:pre (str/join "\n" (map name (sort (keys attrs))))]})
                  {:include-header false}))

(defn relation-table-hiccup [{:keys [attrs tuples]}]
  (if (empty? tuples)
    [:p {:class "pl-mdr"} "Empty relation"]
    (let [attrs (sort-by key attrs)
          ks (map first attrs)
          inds (map second attrs)
          header (into [["Index" :index]] (map (fn [k] [[:code (name k)] k])) ks)
          data (into []
                     (map-indexed
                      (fn [i tup]
                        (-> ks
                            (zipmap (map #(get tup %) inds))
                            (assoc :index i))))
                     tuples)]
      (r/table-hiccup
       header
       (r/abbreviate-table-data header data 10)))))

(defn relation-tables-hiccup [rels]
  (->> rels
       (sort-by (fn [{:keys [attrs tuples]}] [(count tuples) (count attrs)]))
       (map relation-table-hiccup)))

(defn clause-code [clause]
  [:code (r/abbreviate-string (pr-str clause) 40)])



(defn clauses-hiccup [rows]
  (r/table-hiccup
   [["Label" :label]
    ["Clause" :clause]]
   (map #(update % :clause clause-code) rows)
   {:include-header false}))

(defn context-hiccup
  ([context] (context-hiccup context :h2))
  ([{:keys [rels consts]} header-level]
   (list [header-level "Rels"]
         (relation-tables-hiccup rels)
         [header-level "Consts"]
         (r/table-hiccup
          [["Symbol" :symbol] ["Value" :value]]
          (->> consts
               (sort-by key)
               (map (fn [[k v]] {:symbol [:code (pr-str k)]
                                 :value [:code (pr-str v)]})))))))

(defn render-page [page-item]
  (let [type (:type page-item)
        page-sym (r/gensym-page)
        link (r/link-wrapper page-sym)
        step (:page-index page-item)]
    (-> (case type
          :context
          (let [{:keys [rels]} (:context page-item)
                context-size (transduce (map (comp count :tuples)) + rels)]
            {:row {:rels (relation-summary-hiccup rels)
                   :context-size (str context-size)}
             :page [[:h1 (format "Step %d: Context" step)]
                    (context-hiccup (:context page-item))]})
          :resolve-clause
          (let [{:keys [clause _orig-clause children context]} page-item
                lookups (filter #(= :lookup-pattern (:type %)) children)]
            {:row {:clause (clause-code clause)
                   :lookup-count (count lookups)
                   :total-result-size (transduce (map (comp count
                                                            :tuples
                                                            :result))
                                                 +
                                                 lookups)
                   :elapsed-seconds (format "%.3f" (* 1.0e-9 (:elapsed-ns page-item)))}
             :page [[:h1 (format "Step %d: Resolve clause" step)]
                    [:p "We want to update the context by evaluating the following clause."]

                    (clauses-hiccup [{:label "Clause"
                                      :clause clause}])

                    [:h2 "Context before evaluation"]
                    [:p "This is what the context initially looks like before we have resolved this clause."]
                    (context-hiccup context :h3)

                    (when (seq lookups)
                      (list [:h2 "Lookups"]
                            [:p (format "We performed %d lookups against the database backend." (count lookups))]
                            (map (fn [lookup]
                                   (if lookup
                                     (list [:h3 (format "Lookup %d/%d"
                                                        (inc (:index lookup))
                                                        (count lookups))]
                                           [:p "Perform the following lookup"]
                                           (clauses-hiccup [{:label "Original pattern"
                                                             :clause (:orig-pattern lookup)}
                                                            {:label "Pattern"
                                                             :clause (:pattern lookup)}])
                                           [:p "The result returned from the database backend:"]
                                           (relation-table-hiccup (:result lookup)))
                                     [:h3 "...more lookups..."]))
                                 (r/abbreviate-vec
                                  (map-indexed (fn [i x] (assoc x :index i))
                                               lookups)
                                  4 nil))))
                    [:h2 "Context after evaluation"]
                    [:p "The context after being updated with the results from quering the database backend."]
                    (context-hiccup (:result page-item) :h3)]}))
        (assoc-in [:row :type] (link [:code (name type)]))
        (assoc-in [:row :step] (link (str step)))
        (assoc :page-sym page-sym))))

(defn datahike-object? [x]
  (when-let [cl (class x)]
    (str/includes? (.getName cl) "datahike")))

(defn strip-database [x]
  (ju/walk-and-map x
                   (fn
                     ([] 0)
                     ([counter x]
                      (when (datahike-object? x)
                        [(inc counter) 'MASKED])))))

(defn write-trace-report [dst-file trace]
  (let [[q-begin q-end] (filter #(= :q (trace-main-type (:type %))) trace)
        pages (keep render-page (page-items trace))
        rows (mapv :row pages)
        lookup-count (transduce (keep :lookup-count) + rows)
        result-size (transduce (keep :total-result-size) + rows)
        final-row {:step "Overall"
                   :lookup-count lookup-count
                   :total-result-size result-size
                   :elapsed-seconds (format "%.3f" (* 1.0e-9 (:elapsed-ns q-end)))}]
    (r/render [[:h1 "Datahike Engine Trace"]
               [:h2 "Algorithmic Steps"]
               (r/table-hiccup [["Step" :step]
                                ["Type" :type]
                                ["Context size" :context-size]
                                        ;["Relations" :rels]
                                ["Clause" :clause]
                                ["Lookup count" :lookup-count]
                                ["Total result size" :total-result-size]
                                ["Duration (s)" :elapsed-seconds]]
                               (conj rows final-row))
               [:h2 "Input Query"]
               [:pre (with-out-str (pp/pprint (strip-database (:args q-begin))))]
               [:h2 "Output"]
               [:pre (with-out-str (pp/pprint (:result q-end)))]]
              (into {}
                    (map (juxt :page-sym :page) #_(fn [{:keys [page-sym page]}] [page-sym page]))
                    pages)
              {:title "Datahike Query Engine Report"
               :out-file dst-file
               :display-report true
               :error-on-non-referred-details true})))

(comment


  (def x (strip-database the-query))

  )
