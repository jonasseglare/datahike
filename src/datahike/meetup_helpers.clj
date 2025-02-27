(ns datahike.meetup-helpers
  (:require [clojure.string :as str]
            [demotools.html-report :as html-report]))

;; (set-face-attribute 'default nil :height 160)

(defn render-slides [slides]
  (html-report/with-temp-output [cfg {:display-report true}]
    (html-report/render-slideshow
     {:slides slides
      :config cfg})))

(defn intro-slideshow []
  (render-slides
   [{:title "Title page"
     :body (list [:h1 "Datahike and its Query Engine"]
                 [:p "Jonas Östlund"]
                 [:p [:tt "https://github.com/jonasseglare"]])}
    {:title "Overview"
     :body (list [:h1 "Overview"]
                 [:ul
                  [:li "Introduction on Datahike"]
                  [:li "How Datahike stores data"]
                  [:li "The Datahike query engine"]])}
    {:title "About me"
     :body (list [:h1 "About me"]
                 [:ul
                  [:li "Jonas Östlund"]
                  [:li "Live in Liatorp"]
                  [:li "Works at Arbetsförmedlingen in Växjö (with Clojure)"]
                  [:li "Interested in functional programming, algorithms and nature."]])}
    {:title "About Datahike"
     :body (list [:h1 "What is Datahike?"]
                 [:ul
                  [:li "A " [:b "database"]]
                  [:li "Implemented in" " " [:b "Clojure"]]
                  [:li "Maintains the full " [:b "history"] " of all changes"]
                  [:li [:b "Datalog"] " query engine"]])}
    {:title "About Clojure"
     :body (list [:h1 "What is Clojure?"]
                 [:ul
                  [:li "A" " " [:b "lisp"]]
                  [:li "Runs on the " [:b "JVM"]]
                  [:li "A mostly " [:b "functional"] " programming language"]
                  [:li [:b "Dynamically typed"]]
                  [:li "Other implementations:"
                   [:ul
                    [:li "ClojureScript (JavaScript)"]
                    [:li "Babashka (JavaScript)"]
                    [:li "...and others"]]]])}
    {:title "Timeline"
     :body (list [:h1 "A brief timeline"]
                 [:table
                  [:tr [:th "Year"] [:th "Name"] [:th "Description"]]
                  [:tr [:td "1972"] [:td "Prolog"] [:td "Logic programming language"]]
                  [:tr [:td "1977"] [:td "Datalog"]
                   [:td "Logic programming language, simpler than Prolog."]]
                  [:tr [:td "2012"] [:td "Datomic"] [:td "A database with Datomic as a query language. Tracks history of database. Closed source."]]
                  [:tr [:td "2014"] [:td "DataScript"] [:td "Open-source in-memory database similar to Datomic, but with fewer features. Works on frontend (ClojureScript)"]]
                  [:tr [:td "2014"] [:td "Datahike"] [:td "Open-source database similar to Datomic, including history tracking"]]
                  [:tr [:td "2020"] [:td "Datalevin"] [:td "Open-source database similar to Datomic but without history tracking"]]])}
    {:title "Datoms"
     :body (list [:h1 "Datoms"]
                 (html-report/table-hiccup
                  [["Entity" :entity]
                   ["Attribute" :attribute]
                   ["Value" :value]]
                  [{:entity "abc"
                    :attribute "asdf"
                    :value "xyz"
                    }]))}]))

(comment

  (intro-slideshow)

  )


(defn print-db-datoms [raw-datoms]
  (let [datoms (->> raw-datoms
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

(comment

  
  "/Users/osdjn/jonaswiki/meetings/2024-12-04-ssyk-sokdemo.html"

  (println "MJAO")

  )
