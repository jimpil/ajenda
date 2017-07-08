(ns ajenda.retrying-test
  (:require [clojure.test :refer :all]
            [ajenda.retrying :refer :all])
  (:import (java.util.concurrent.atomic AtomicInteger)
           (clojure.lang ExceptionInfo)))

(deftest with-max-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; limit reached - no result - return nil
    (is (nil? (with-max-retries nil 10 nil? (.incrementAndGet check-box))))
    (is (= 10 (.get check-box)))

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
                                      2 (throw (ex-info "" {}))
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


(deftest retrying-options-tests
  (let [check-box (AtomicInteger. 0)]

    (testing "testing :retry-fn!"

      (with-max-retries {:retry-fn! (fn [_] (.getAndIncrement check-box))}
                        10
                        (constantly false)
                        (.getAndIncrement check-box))

      (is (= 20 (.get check-box))) ;; got incremented twice per iteration

      )

    (.set check-box 0)
    (testing "delay-fn!"
      (testing "fixed-delay"
        (let [[do-delay! get-delay] (fixed-delay 100)]
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn get-delay i)
                                              (.getAndIncrement check-box))
                                 :delay-fn! do-delay!}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))

        (is (= 20 (.get check-box))) ;; ended up doing 10 iterations due to the delay (1000 / 100 = 10)
        )

      (.set check-box 0)

      (testing "additive-delay"
        (let [[do-delay! get-delay] (additive-delay 100 50)] ;; 100 => 150 => 200 => 250 ...
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn get-delay i)
                                              (.getAndIncrement check-box))
                                 :delay-fn! do-delay!}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))
        ;; 100 + 150 + 200 + 250 + 300 = 1000
        (is (= 10 (.get check-box))) ;; there was not enough time for the 6th iteration
        )

      (.set check-box 0)
      (testing "increasing (10x) multiplicative-delay (aka exponential-backoff)"
        (let [[do-delay! get-delay] (exponential-delay 10)] ;; 10^1 - 10^N progression (ten-fold increase on each iteration)
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn get-delay i)
                                              (.getAndIncrement check-box))
                                 :delay-fn! do-delay!}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))
        ;; only 3 iterations fit in 1000 ms due to the (increasing) delays (10 => 100 => 1000)
        (is (= 6 (.get check-box)))
        )

      (.set check-box 0)
      (testing "decreasing (2x) multiplicative-delay"
        (let [[do-delay! get-delay] (multiplicative-delay 600 0.5)]  ;; 2x decrease at each iteration
          (with-retries-timeout {:retry-fn! (fn [i]
                                              (default-log-fn get-delay i)
                                              (.getAndIncrement check-box))
                                 :delay-fn! do-delay!}
                                1000
                                nil
                                (constantly false)
                                (.getAndIncrement check-box)))
        ;; only 3 iterations fit in 1000 ms due to the (decreasing) delays (600 => 900 => 1050)
        (is (= 6 (.get check-box)))
        )
      )
    )

  )


