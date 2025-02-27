(ns demotools.html-report
  (:require [hiccup.core :as hiccup]
            [hiccup.page :as hiccup-page]
            [clojure.java.io :as io]
            [clojure.walk :refer [postwalk]]
            [clojure.string :as str]
            [babashka.fs :as fs]
            [clojure.spec.alpha :as spec]))

(defn page-contents? [x]
  (and (sequential? x)
       (not (keyword? (first x)))))

(defn page-symbol? [x]
  (and (symbol? x) (str/starts-with? (name x) "-page")))

(defn sub-pages? [x]
  (and (map? x)
       (every? #(or (string? %) (page-symbol? %)) (keys x))
       (every? page-contents? (vals x))))

(defn style [lambda]
  (format "
.wrapper {
   display: flex;
   flex-direction: row;
}

.wrapper > div {
    border: 1px solid black;
    padding: 0.5em;
//    flex: 1;
}

.main-column {
  flex-grow: %d
}

.detail-column {
  flex-grow: %d
}

.details {
    display: None;
}

.details:target {
    display: block;
}
"
          (->> lambda (* 100) Math/round int)
          (->> lambda (- 1.0) (* 100) Math/round int)))

(spec/def ::main-page any?)
(spec/def ::sub-pages map?)
(spec/def ::config map?)
(spec/def ::split-report (spec/keys :req-un [::main-page ::sub-pages]
                                    :opt-un  [::config]))

(spec/def ::page (spec/keys :req-un [::body
                                     ::title]))
(spec/def ::slides (spec/coll-of ::page))
(spec/def ::slideshow (spec/keys :req-un [::slides]
                                 :opt-un [::config]))

(spec/def ::single-page (spec/keys :req-un [::body]
                                   :opt-un [::config]))

(def default-config {:title "Report"
                     :display-report false
                     :out-file "demo.html"
                     :error-on-non-referred-details false
                     :col-lambda 0.5})

(defn complete-args [spec args]
  {:pre [(spec/valid? spec args)]}
  (update args :config #(merge default-config %)))

(defn complete-single-page [args]
  (complete-args ::single-page args))

(defn complete-split-args [args]
  (complete-args ::split-report args))

(defn complete-slideshow-args [args]
  (complete-args ::slideshow args))

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

(defn wrap-body [body config]
  (let [github-style (-> "css/github-markdown-light.css"
                         io/resource
                         slurp)]
    (list [:head
           [:style {:type "text/css"} github-style]
           [:style {:type "text/css"} (style (:col-lambda config))]
           [:title (:title config)]]
          [:body body])))

(defn output-html [hiccup config]
  (let [report-string (hiccup-page/html5 hiccup)
        dst (:out-file config)]
    (spit (fs/file dst) report-string)
    (when (:display-report config)
      (.open (java.awt.Desktop/getDesktop) (io/file dst)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; A P I
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn details-href [pagekey]
  {:pre [(string? pagekey)]}
  {:href (str "#" pagekey)})

(defn split-hiccup-report
  "Takes three arguments: `main-page sub-pages config`. The last two ones are optional."
  [args]
  (let [{:keys [main-page sub-pages config]} (complete-split-args args)
        symbol-map (atom {})
        main-page (map-page-symbols symbol-map "#" main-page)
        sub-pages (map-sub-pages-symbols symbol-map sub-pages)
        config (merge default-config config)
        ]
    (wrap-body [:div {:class "wrapper"}
                (into [:div {:class "markdown-body main-column"}]
                      main-page)
                (into [:div {:class "markdown-body detail-column"}]
                      (for [[k v] sub-pages]
                        (into [:div {:id k :class "details"}] v)))]
               config)))

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




(defn render-split [args]
  (let [{:keys [sub-pages config]} (complete-split-args args)
        drefs (find-all-details-refs args)]
    (when-let [invalid-refs (seq (remove sub-pages drefs))]
      (throw (ex-info "Invalid page references" {:refs invalid-refs})))
    (when-let [non-referred-pages (and (:error-on-non-referred-details config)
                                       (seq (remove drefs (keys sub-pages))))]
      (throw (ex-info "Detail pages not referred to" {:keys non-referred-pages})))
    (output-html (split-hiccup-report args) config)))

(defn link-wrapper [href]
  (fn [& args]
    (into [:a {:href href}] args)))

(defn slideshow-hiccup [args]
  (let [{:keys [slides config]} (complete-slideshow-args args)
        slide-key (fn [i] (format "slide%d" (inc i)))]
    (wrap-body (into [:div
                      (into [:span]
                            (comp (map-indexed (fn [i slide]
                                                 [[:a (details-href (slide-key i))
                                                   (:title slide)]
                                                  " "]))
                                  cat)
                            slides)]
                     (map-indexed (fn [i slide]
                                    [:div {:class "markdown-body details"
                                           :id (slide-key i)}
                                     (:body slide)]))
                     slides)
               config)))

(defn render-multipage-slideshow [args]
  (let [{:keys [slides config]} (complete-slideshow-args args)
        out-file (:out-file config)
        parent (fs/parent out-file)
        [base-name ext] (fs/split-ext (fs/file-name out-file))
        page-names (into [out-file]
                         (map-indexed
                          (fn [i _slide]
                            (fs/file parent
                                     (format "%s_page%d.%s"
                                             base-name
                                             (inc i)
                                             ext))))
                         (rest slides))
        slides (map (fn [slide page-name] (assoc slide :page-file page-name))
                    slides
                    page-names)
        nav-bar (into [:div]
                      (mapcat (fn [slide]
                                [[:a {:href (fs/file-name (:page-file slide))}
                                  (:title slide)]
                                 " "]))
                      slides)]
    (doseq [[i slide] (map-indexed vector slides)]
      (output-html
       (wrap-body (list nav-bar [:span {:class "markdown-body"}
                                 (:body slide)])
                  config)
       (assoc config
              :out-file (:page-file slide)
              :display-report (if (zero? i)
                                (:display-report config)
                                false))))))

(defn render-slideshow [args]
  (let [args (complete-slideshow-args args)]
    (output-html (slideshow-hiccup args) (:config args))))

(defn with-temp-output-fn [config f]
  (let [out-file (fs/file (fs/create-temp-dir) "index.html")
        config (assoc config :out-file out-file)]
    (f config)
    (str out-file)))

(defmacro with-temp-output [[config-sym config] & body]
  `(with-temp-output-fn ~config (fn [~config-sym] ~@body)))

(defn render-page [args]
  (let [{:keys [body config]} (complete-single-page args)
        config (merge default-config config)]
    (output-html (wrap-body [:span {:class "markdown-body"} body] config) config)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;
;;;; E X A M P L E
;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn demo-render-page []
  (with-temp-output [config {:display-report true}]
    (render-page {:body [:h1 "HEJ"] :config config})))

(defn demo-split-report []
  (render-split
   {:main-page [[:h1 "Main header"]
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
    :sub-pages {"result0" [[:h1 "Result 0"]
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
    :config {:display-report true
             :error-on-non-referred-details true}}))

(comment

  (demo)

  )

(defn demo-slideshow []
  (with-temp-output [config {}]
    (render-multipage-slideshow
     {:slides [{:title "Overview"
                :body (list [:h1 "Slide 1"]
                            [:tt "This is good"]
                            [:pre "And here we have a code block\nDon'nt we?"])}
               {:title "About me"
                :body [:h1 "Slide 2"]}]
      :config config})))



(comment


  (demo-slideshow)

  )

