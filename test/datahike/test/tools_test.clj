(ns datahike.test.tools-test
  (:require [datahike.tools :as dt]
            [clojure.test :refer :all]))

(deftest test-map-vector-elements
  (is (= [11 19] (dt/map-vector-elements [10 20]
                   a (inc a)
                   b (dec b))))
  (is (= [11] (dt/map-vector-elements [10]
                a (inc a)
                b (+ a b))))
  (is (= [300 40] (dt/map-vector-elements [10 30]
                    a (* a b)
                    b (+ a b)))))
