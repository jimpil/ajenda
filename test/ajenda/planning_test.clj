(ns ajenda.planning-test
  (:require [clojure.test :refer :all]
            [ajenda.planning :refer :all]
            [ajenda.utils :as ut])
  (:import (java.util.concurrent.atomic AtomicInteger)
           (java.time LocalDateTime)
           (java.time.temporal ChronoUnit)))


(deftest periodically-tests
  (let [check-box (AtomicInteger. 0)
        cancel-exec (periodically 0 10 nil (.incrementAndGet check-box))
        cancel-canceller  (do-after* 100 nil cancel-exec)]
    (Thread/sleep 400) ;; sleep for 4x time on this thread
    (let [res (.get check-box)]
      ;; there is no way to be more precise!
      ;; nonetheless, we know that periodic execution stopped, otherwise <check-box> would have a much higher value
      (is (or (= 10 res)
              (= 11 res))))
    )
  )

(deftest do-at-tests
  (let [check-box (AtomicInteger. 0)
        target-date (.plus (LocalDateTime/now) 1 ChronoUnit/SECONDS)
        arg-string  (.format ut/rfc3339-formatter target-date)]
    (do-at! arg-string nil (.incrementAndGet check-box))
    (is (= 0 (.get check-box)))
    (Thread/sleep 1000)
    (is (= 1 (.get check-box)))
    )
  )



