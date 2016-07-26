(ns ajenda.expiring-test
  (:require [clojure.test :refer :all]
            [ajenda.expiring :refer :all])
  (:import (clojure.lang ExceptionInfo)))


(deftest with-timeout-tests
  (is (thrown-with-msg? ExceptionInfo #"TIMEOUT!"
               (with-timeout 10 nil (throw (ex-info "TIMEOUT!" {}))
                             (apply + (range 1000000000)))))

  (is (= 15 (with-timeout 10 nil (throw (ex-info "TIMEOUT!" {}))
                          (apply + (range 1 6)))))
  )
