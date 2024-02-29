(ns datahike.test.tools-test
  (:require [datahike.tools :as dt]
            [clojure.test :refer :all]))

(deftest test-with-destructured-vector
  (is (= [11 19]
         (dt/with-destructured-vector [10 20]
           a (inc a)
           b (dec b))))
  (is (= [11]
         (dt/with-destructured-vector [10]
           a (inc a)
           b (+ a b))))
  (is (= [300 40]
         (dt/with-destructured-vector [10 30]
           a (* a b)
           b (+ a b))))
  (is (= [100 400]
         (dt/with-destructured-vector [10 20]
           a (* a a)
           b (* b b)
           c (throw (ex-info "This element should not be evaluated" {}))))))

(deftest test-match-vector
  (is (= 0 (dt/match-vector [nil nil]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 1 (dt/match-vector [nil 9]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 2 (dt/match-vector [10 nil]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 2 (dt/match-vector [10 :asdf]
                            [_ _] 0
                            [_ 1] 1
                            [1 *] 2)))
  (is (= 3 (dt/match-vector [10 :asdf]
                            [_ _] 0
                            [_ 1] 1
                            [1 _] 2
                            [1 1] 3)))
  (is (= 2 (dt/match-vector [10 nil]
                            [_ _] 0
                            [_ 1] 1
                            [1 _] 2
                            [1 1] 3))))
