(ns demotools.html-report
  (:require [hiccup.core :as hiccup]
            [clojure.java.io :as io]
            [clojure.walk :refer [postwalk]]
            [clojure.string :as str]
            [babashka.fs :as fs]))

(defn page-contents? [x]
  (and (sequential? x)
       (not (keyword? (first x)))))

(defn page-symbol? [x]
  (and (symbol? x) (str/starts-with? (name x) "-page")))

(defn sub-pages? [x]
  (and (map? x)
       (every? #(or (string? %) (page-symbol? %)) (keys x))
       (every? page-contents? (vals x))))

(def style "
.wrapper {
   display: flex;
   flex-direction: row;
}

.wrapper > div {
    border: 1px solid black;
    padding: 0.5em;
    flex: 1;
}

.details {
    display: None;
}

.details:target {
    display: block;
}
")

(def default-config {:title "Report"
                     :display-report false
                     :out-file "demo.html"
                     :error-on-non-referred-details false})

(defn parsed-args? [x]
  (and (map? x)
       (map? (:config x))
       (page-contents? (:main-page x))
       (sub-pages? (:sub-pages x))))

(defn parse-args [args]
  {:post [(parsed-args? %)]}
  (if (and (= 1 (count args)) (parsed-args? (first args)))
    (first args)
    (-> {:main-page []
         :sub-pages {}
         :config {}}
        (merge (zipmap [:main-page :sub-pages :config] args))
        (update :config #(merge default-config %)))))

(defn a-href [x]
  (when (vector? x)
    (let [[tag args] x]
      (when (and (= :a tag) (map? args))
        (when-let [href (:href args)]
          href)))))

(defn extract-detail-ref [x]
  (when-let [href (a-href x)]
    (cond
      (str/starts-with? href "#") (subs href 1)
      (page-symbol? href) href
      :else nil)))

(defn gensym-page []
  (gensym "-page"))

(defn map-page-symbols [map-atom prefix x]
  (postwalk (fn [x]
              (if (page-symbol? x)
                (str prefix
                     (get (swap! map-atom
                                 (fn [m]
                                   (if (contains? m x)
                                     m
                                     (assoc m x (format "page%d" (count m))))))
                          x))
                x))
            x))

(defn find-all-details-refs [args]
  (let [referred-pages (atom #{})]
    (postwalk (fn [x]
                (if-let [detail-ref (extract-detail-ref x)]
                  (swap! referred-pages conj detail-ref)
                  x))
              args)
    (deref referred-pages)))

(defn map-sub-pages-symbols [symbol-map sub-pages]
  {:pre [(map? sub-pages)]}
  (let [m (seq sub-pages)
        ks (keys m)
        vs (vals m)]
    (zipmap (map-page-symbols symbol-map "" ks)
            (map-page-symbols symbol-map "#" vs))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; A P I
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn details-href [pagekey]
  {:pre [(string? pagekey)]}
  {:href (str "#" pagekey)})

(defn hiccup-report
  "Takes three arguments: `main-page sub-pages config`. The last two ones are optional."
  [& args]
  (let [{:keys [main-page sub-pages config]} (parse-args args)
        symbol-map (atom {})
        main-page (map-page-symbols symbol-map "#" main-page)
        sub-pages (map-sub-pages-symbols symbol-map sub-pages)
        config (merge default-config config)
        github-style (-> "css/github-markdown-light.css"
                         io/resource
                         slurp)]
    [:html
     [:head
      [:style {:type "text/css"} github-style]
      [:style {:type "text/css"} style]
      [:title (:title config)]]
     [:body
      [:div {:class "wrapper"}
       (into [:div {:class "markdown-body"}]
             main-page)
       (into [:div {:class "markdown-body"}]
             (for [[k v] sub-pages]
               (into [:div {:id k :class "details"}] v)))]]]))

(defn abbreviate-string [s max-len]
  (if (<= (count s) max-len)
    s
    (str (subs s 0 (max 0 (- max-len 3))) "...")))

(defn abbreviate-vec [rows max-len ellipsis]
  (let [rows (vec rows)
        n (count rows)]
    (if (<= n max-len)
      rows
      (let [before-count (max 0 (dec max-len))
            last-count (min 1 max-len)]
        (into []
              cat
              [(subvec rows 0 before-count)
               [ellipsis]
               (subvec rows (- n last-count))])))))

(defn abbreviate-table-data [header-pairs rows max-len]
  (abbreviate-vec
   rows max-len
   (into {} (map (fn [[_ k]] [k "..."])) header-pairs)))

(defn table-hiccup
  ([header-title-key-pairs row-maps]
   (table-hiccup header-title-key-pairs row-maps {}))
  ([header-title-key-pairs row-maps config]
   {:pre [(every? (fn [x]
                    (and (vector? x)
                         (= 2 (count x))))
                  header-title-key-pairs)
          (every? map? row-maps)]}
   (let [config (merge {:include-header true
                        :exclude-nil-columns true}
                       config)
         header-title-key-pairs (if (:exclude-nil-columns config)
                                  (remove (fn [[_title k]] (every? #(nil? (get % k)) row-maps))
                                          header-title-key-pairs)
                                  header-title-key-pairs)]
     (into [:table
            (when (:include-header config)
              (into [:tr]
                    (map (fn [[title _]] [:th title]))
                    header-title-key-pairs))]
           (map (fn [m] (into [:tr]
                              (map (fn [[_ k]] [:td (m k)]))
                              header-title-key-pairs)))
           row-maps))))

(defn html-report [& args]
  (hiccup/html (apply hiccup-report args)))

(defn render [& args]
  (let [{:keys [sub-pages config] :as args} (parse-args args)
        report-string (html-report args)
        dst (:out-file config)
        drefs (find-all-details-refs args)]
    (when-let [invalid-refs (seq (remove sub-pages drefs))]
      (throw (ex-info "Invalid page references" {:refs invalid-refs})))
    (when-let [non-referred-pages (and (:error-on-non-referred-details config)
                                       (seq (remove drefs (keys sub-pages))))]
      (throw (ex-info "Detail pages not referred to" {:keys non-referred-pages})))
    (spit (fs/file dst) report-string)
    (when (:display-report config)
      (.open (java.awt.Desktop/getDesktop) (io/file dst)))))

(defn link-wrapper [href]
  (fn [& args]
    (into [:a {:href href}] args)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; E X A M P L E
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn demo []
  (render
   [[:h1 "Main header"]
    [:table
     [:tr [:th "Timestamp"] [:th "Result"] [:th "Keyword"]]
     [:tr
      [:td "2023-12-19"]
      [:td [:a (details-href "result0")
            "Result 0"]]
      [:td [:tt "data"]]]
     [:tr
      [:td "2023-12-20"]
      [:td [:a (details-href "result1")
            "Result 1"]]
      [:td "Mjao"]]]]
   {"result0" [[:h1 "Result 0"]
               [:h2 "Stack trace"]
               [:pre "               RestFn.java: 1523  clojure.lang.RestFn/invoke
    interruptible_eval.clj:   84  nrepl.middleware.interruptible-eval/evaluate
    interruptible_eval.clj:   56  nrepl.middleware.interruptible-eval/evaluate
    interruptible_eval.clj:  152  nrepl.middleware.interruptible-eval/interruptible-eval/fn/fn
                  AFn.java:   22  clojure.lang.AFn/run
               session.clj:  218  nrepl.middleware.session/session-exec/main-loop/fn
               session.clj:  217  nrepl.middleware.session/session-exec/main-loop
                  AFn.java:   22  clojure.lang.AFn/run
               Thread.java: 1623  java.lang.Thread/run
"]]
    "result1" [[:h1 "Result 1"]
               [:ul
                [:li "Item 1"]
                [:li "Item 2"]]]}
   {:display-report true
    :error-on-non-referred-details true}))

(comment

  (demo)

  )
