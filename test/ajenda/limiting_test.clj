(ns ajenda.limiting-test
  (:require [ajenda.limiting :refer :all]
            [clojure.test :refer :all])
  (:import (java.util.concurrent.atomic AtomicInteger)
           (java.util.concurrent TimeUnit)))

(defn- log-attempt! [i]
  (println "Attempt:" i))


(deftest rate-limited-tests
  (let [checkbox1 (AtomicInteger. 0)
        checkbox2 (AtomicInteger. 0)
        f1 (rate-limited (fn []
                           (.incrementAndGet checkbox1))
                         [5 TimeUnit/MILLISECONDS 500]
                         log-attempt!)
        f2 (rate-limited (fn []
                           (.incrementAndGet checkbox2)
                           (Thread/sleep 1000)) ;; guaranteed to succeed
                         [1 TimeUnit/SECONDS 1]
                         log-attempt!)
        ]
    (testing ""
      (dotimes [_ 10]
        (f1))
      (is (= 5 (.get checkbox1))) ;; only the first 5 calls had any effect
      )



    (dotimes [_ 5]
      (f2))
    (is (= 5 (.get checkbox2))) ;; all calls had an effect because the call itself is sleeping for 1000ms
    )


  )
