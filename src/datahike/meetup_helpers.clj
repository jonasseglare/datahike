(ns datahike.meetup-helpers
  (:require [clojure.string :as str]
            [demotools.html-report :as html-report]))

;; (set-face-attribute 'default nil :height 160)


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
