(ns demotools.html-report-test
  (:require [demotools.html-report :as r]
            [clojure.test :refer [deftest is]]
            [clojure.string :as str]))

(deftest test-html-report
  (is (= (r/extract-detail-ref [:a {:href "#asdf"}])
         "asdf"))
  (is (nil? (r/extract-detail-ref [:a {:href "asdf"}])))
  (is (nil? (r/extract-detail-ref {})))
  (is (= #{"asdf" "mjao"}
         (r/find-all-details-refs
          [[[[[[[[:a {:href "#asdf"}]]]]
              [[[[[[:a {:href "#mjao"}]]]]]]]]]])))
  (is (not (r/parsed-args? [])))
  (is (not (r/parsed-args? {})))
  (is (r/parsed-args? (r/parse-args [])))
  (is (r/parsed-args? (r/parse-args [(r/parse-args [])])))
  (is (str/starts-with?
       (r/html-report ["Hej"] {"detail" ["Detail"]})
       "<html>"))
  (is (str/starts-with?
       (r/html-report ["Hej"] {"detail" ["Detail"]} {})
       "<html>"))
  (is (str/starts-with?
       (r/html-report ["Hej"])
       "<html>"))
  (is (str/starts-with?
       (r/html-report)
       "<html>"))
  (let [x (r/html-report ["Hej" [:a {:href '-pageK}]] {'-pageK ["Detail"]} {})]
    (is (str/starts-with? x "<html>"))
    (is (str/includes? x "id=\"page0\""))
    (is (str/includes? x "a href=\"#page0\""))))

(deftest test-map-page-symbols
  (is (not (r/page-symbol? {})))
  (is (not (r/page-symbol? 'asdfa)))
  (is (r/page-symbol? '-page234))
  (is (r/page-symbol? (r/gensym-page)))
  (let [m (atom {})]
    (is (= "page0" (r/map-page-symbols m "" '-page234234)))
    (is (= "page1" (r/map-page-symbols m "" '-page2234)))
    (is (= '[[{:a 'b :c "#page2"}]]
           (r/map-page-symbols m "#" '[[{:a 'b :c -pagekatt}]])))
    (is (= [[["#page1"]]] (r/map-page-symbols m "#" [[['-page2234]]])))
    (is (= {'-page234234 "page0"
            '-page2234 "page1"
            '-pagekatt "page2"}
           (deref m)))))

(deftest test-table-hiccup
  (is (= [0 1 2 3 :mjao 9]
         (r/abbreviate-vec (range 10) 5 :mjao)))
  (is (= [:table
          [:tr [:th "File"] [:th "Size"]]
          [:tr [:td "mjao.dat"] [:td 234]]]
         (r/table-hiccup [["File" :file] ["Size" :size]]
                         [{:file "mjao.dat" :size 234}])))
  (is (= (r/abbreviate-table-data [["Title" :title]]
                                  [{:title "A"} 
                                   {:title "B"} 
                                   {:title "C"}
                                   {:title "D"}]
                                  2)
         [{:title "A"} {:title "..."} {:title "D"}])))

(deftest test-abbreviate-string
  (is (= "hej" (r/abbreviate-string "hej" 9)))
  (is (= "asdf..." (r/abbreviate-string "asdfasdfasdfasdfasdfasdfasdfasdfasfd" 7))))
