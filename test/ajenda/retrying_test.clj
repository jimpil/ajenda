(ns ajenda.retrying-test
  (:require [clojure.test :refer :all]
            [ajenda.retrying :refer :all])
  (:import (java.util.concurrent.atomic AtomicInteger)
           (clojure.lang ExceptionInfo)))

(deftest with-max-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; limit reached - no result - return nil
    (is (nil? (with-max-retries nil 10 nil? (.incrementAndGet check-box))))
    (is (= 11 (.get check-box)))

    ;; returns result
    (is (= 15 (with-max-retries nil 6 (partial = 15) (.incrementAndGet check-box))))
    (is (= 15 (.get check-box)))
    )
  )

(deftest with-max-retries-timeout-tests
  (let [check-box (AtomicInteger. 0)]

    ;; retry limit reached - return nil
    (is (nil? (with-max-retries-timeout nil 10 :timeout 1000 nil? (.incrementAndGet check-box))))

    ;; timeout limit reached - interrupt thread - return :timeout
    (is (= :timeout
           (with-max-retries-timeout nil 10 :timeout Integer/MAX_VALUE nil? (.incrementAndGet check-box))))
    ;; can't really know the contents of <check-box> at this point

    (.set check-box 0) ;reset it

    ;; got result - return it
    (is (= 100
           (with-max-retries-timeout nil 10 :timeout Integer/MAX_VALUE (partial = 100) (.incrementAndGet check-box))))
    (is (= 100 (.get check-box)))

    )
  )

(deftest with-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; finds an answer
    (is (= 150 (with-retries nil (partial = 150)
                 (.incrementAndGet check-box))))
    (is (= 150 (.get check-box)))
    )
  )

(deftest with-error-retries-tests
  (let [check-box (AtomicInteger. 0)]
    (testing "`with-error-retries`"
      ;; answer found on the 10th retry
      (is (= :whatever
             (with-error-retries nil [ArithmeticException IndexOutOfBoundsException]
                (condp = (.getAndIncrement check-box)
                  5 (throw (IndexOutOfBoundsException. ""))
                  10 :whatever
                  (throw (ArithmeticException. ""))))))
      )


    (.set check-box 0)
    (testing "`:ex-pred` option"
      (is (true?
            (with-error-retries {:ex-pred (fn [e] (= "foo" (.getMessage e)))}
                                [ArithmeticException IndexOutOfBoundsException]
                                (condp = (.getAndIncrement check-box)
                                  5 (throw (IndexOutOfBoundsException. "foo"))
                                  10 :whatever
                                  (throw (ArithmeticException. "bar"))))))

      (is (= 6 (.get check-box))) ;; 5th attempt succeeded
      )
    )
  )

(deftest with-error-retries-timeout-tests
  (let [check-box (AtomicInteger. 0)]

    ;; timeout elapsed - return :done
    (is (= :done
           (with-error-retries-timeout nil 10 :done [ArithmeticException IndexOutOfBoundsException]
             (throw (ArithmeticException. "")))))

    ;; answer found after 10 retries and before 100ms
    (is (= 10
           (with-error-retries-timeout nil 100 :done [ArithmeticException IndexOutOfBoundsException]
             (let [i (.getAndIncrement check-box)]
               (if (= 10 i)
                 i
                 (throw (ArithmeticException. "")))))))
    )
  )

(deftest with-max-error-retries-tests
  (testing "with-max-error-retries"
    (let [check-box (AtomicInteger. 0)]

      ;; max-retries elapsed - no answer
      (is (nil?
            (with-max-error-retries {:retry-fn! default-log-fn} 3
                                    [ArithmeticException IndexOutOfBoundsException ExceptionInfo]
                                    (condp = (.getAndIncrement check-box)
                                      0 (throw (ArithmeticException. ""))
                                      1 (throw (IndexOutOfBoundsException. ""))
                                      2 (throw (ex-info "foo" {}))
                                      3 (throw (ex-info "bar" {}))
                                      :whatever))))

      (.set check-box 0) ;reset it

      ;; found an answer before max-retries
      (is (= :whatever
             (with-max-error-retries nil 3 [ArithmeticException IndexOutOfBoundsException ExceptionInfo]
                                     (condp = (.getAndIncrement check-box)
                                       0 (throw (ArithmeticException. ""))
                                       1 (throw (IndexOutOfBoundsException. ""))
                                       2 :whatever
                                       nil))))
      ))
  )


(deftest with-retries-timeout-tests
  (let [check-box (AtomicInteger. 0)]

    ;; timeout limit reached - return :done
    (is (= :done (with-retries-timeout nil 10 :done nil? (.incrementAndGet check-box))))

    (.set check-box 0)
    ;; answer found before timeout
    (is (= 1 (with-retries-timeout nil 20 :done some? (.incrementAndGet check-box))))
    )
  )

(deftest delaying-calc-tests
  (let [add-delay (additive-delay 10 25)
        sub-delay (additive-delay 110 -25)
        mul-delay (multiplicative-delay 10 2)
        div-delay (multiplicative-delay 160 0.5)
        exp-delay (exponential-delay 10)
        cycl-delay (cyclic-delay [10 20 30])
        osc-delay (oscillating-delay 10 50)
        simulated-iterations (range 5)]

    (testing "delaying strategies correctness"

      (is (= [10 35 60 85 110] ;; +25 at each step
             (map add-delay simulated-iterations)))
      (is (= [10 20 40 80 160] ;; *2 at each step
             (map mul-delay simulated-iterations)))
      (is (= [10 100 1000 10000 100000] ;; +1 exponent at each step
             (map exp-delay simulated-iterations)))
      (is (= (take 100 (cycle [10 20 30]))
             (map cycl-delay (range 100))))
      (is (= (take 100 (cycle [10 50]))
             (map osc-delay (range 100))))



      (is (= (reverse [10 35 60 85 110])  ;; -25 at each step
             (map sub-delay simulated-iterations)))

      (is (= (reverse [10 20 40 80 160]) ;; /2 at each step
             (map div-delay simulated-iterations)))
      )
    )
  )


(deftest retrying-options-tests
  (let [check-box (AtomicInteger. 0)]

    (testing "testing :retry-fn!"

      (with-max-retries {:retry-fn! (fn [_] (.getAndIncrement check-box))}
                        10
                        (constantly false)
                        (.getAndIncrement check-box))

      ;; got incremented twice per iteration (1 attempt + 10 retries)
      ;; `retry-fn` was NOT called in the very last iteration (hence 21 instead of 22)
      (is (= 21 (.get check-box)))

      )

    (.set check-box 0)
    (testing "delay-fn!"
      (testing "fixed-delay"
        (let [delay-algo (fixed-delay 100)]
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn delay-algo i)
                                              (.getAndIncrement check-box))
                                 :delay-calc delay-algo}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))

        (is (= 20 (.get check-box))) ;; ended up doing 10 iterations due to the delay (1000 / 100 = 10)
        )

      (.set check-box 0)

      (testing "additive-delay"
        (let [delay-algo (additive-delay 100 50)] ;; 100 => 150 => 200 => 250 ...
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn delay-algo i)
                                              (.getAndIncrement check-box))
                                 :delay-calc delay-algo}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))
        ;; 100 + 150 + 200 + 250 + 300 = 1000
        (is (= 10 (.get check-box))) ;; there was not enough time for the 6th iteration
        )

      (.set check-box 0)
      (testing "increasing (10x) multiplicative-delay (aka exponential-backoff)"
        (let [delay-algo (exponential-delay 10)] ;; 10^1 - 10^N progression (ten-fold increase on each iteration)
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn delay-algo i)
                                              (.getAndIncrement check-box))
                                 :delay-calc delay-algo}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))
        ;; only 3 iterations fit in 1000 ms due to the (increasing) delays (10 => 100 => 1000)
        (is (= 6 (.get check-box)))
        )

      (.set check-box 0)
      (testing "decreasing (2x) multiplicative-delay"
        (let [delay-algo (multiplicative-delay 600 0.5)]  ;; 2x decrease at each iteration
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn delay-algo i)
                                              (.getAndIncrement check-box))
                                 :delay-calc delay-algo}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))
        ;; only 3 iterations fit in 1000 ms due to the (decreasing) delays (600 => 900 => 1050)
        (is (= 6 (.get check-box)))
        )
      (.set check-box 0)

      (testing "cyclic-delay"
        (let [delay-algo (cyclic-delay [100 200 300])]
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn delay-algo i)
                                              (.getAndIncrement check-box))
                                 :delay-calc delay-algo}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box))
          ;; only 6 iterations fit in 1000 ms due to the (cycling) delays (100 + 200 + 300 + 100 + 200 = 900)
          (is (= 12 (.get check-box)))
          )

        )

      )
    )

  )


