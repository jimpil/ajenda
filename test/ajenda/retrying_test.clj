(ns ajenda.retrying-test
  (:require [clojure.test :refer :all]
            [ajenda.retrying :refer :all])
  (:import (java.util.concurrent.atomic AtomicInteger)
           (clojure.lang ExceptionInfo)))

(deftest with-max-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; limit reached - no result - return nil
    (is (nil? (with-max-retries 10 nil? (.incrementAndGet check-box))))
    (is (= 10 (.get check-box)))

    ;; returns result
    (is (= 15 (with-max-retries 6 (partial = 15) (.incrementAndGet check-box))))
    (is (= 15 (.get check-box)))
    )
  )

(deftest with-max-retries-timeout-tests
  (let [check-box (AtomicInteger. 0)]

    ;; retry limit reached - return nil
    (is (nil? (with-max-retries-timeout 10 :timeout 1000 nil? (.incrementAndGet check-box))))

    ;; timeout limit reached - interrupt thread - return :timeout
    (is (= :timeout
           (with-max-retries-timeout 10 :timeout Integer/MAX_VALUE nil? (.incrementAndGet check-box))))
    ;; can't really know the contents of <check-box> at this point

    (.set check-box 0) ;reset it

    ;; got result - return it
    (is (= 100
           (with-max-retries-timeout 10 :timeout Integer/MAX_VALUE (partial = 100) (.incrementAndGet check-box))))
    (is (= 100 (.get check-box)))

    )
  )

(deftest with-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; finds an answer
    (is (= 15000 (with-retries (partial = 15000) (.incrementAndGet check-box))))
    (is (= 15000 (.get check-box)))
    )
  )

(deftest with-error-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; answer found on the 10th retry
    (is (= :whatever
           (with-error-retries [ArithmeticException IndexOutOfBoundsException]
             (condp = (.getAndIncrement check-box)
               5 (throw (IndexOutOfBoundsException. ""))
               10 :whatever
               (throw (ArithmeticException. ""))))))
    )

  )

(deftest with-error-retries-timeout-tests
  (let [check-box (AtomicInteger. 0)]

    ;; timeout elapsed - return :done
    (is (= :done
           (with-error-retries-timeout 10 :done [ArithmeticException IndexOutOfBoundsException]
             (throw (ArithmeticException. "")))))

    ;; answer found after 10 retries and before 100ms
    (is (= 10
           (with-error-retries-timeout 100 :done [ArithmeticException IndexOutOfBoundsException]
             (let [i (.getAndIncrement check-box)]
               (if (= 10 i)
                 i
                 (throw (ArithmeticException. "")))))))
    )
  )

(deftest with-max-error-retries-tests
  (let [check-box (AtomicInteger. 0)]

    ;; max-retries elapsed - no answer
    (is (nil?
          (with-max-error-retries 3 [ArithmeticException IndexOutOfBoundsException ExceptionInfo]
            (condp = (.getAndIncrement check-box)
              0 (throw (ArithmeticException. ""))
              1 (throw (IndexOutOfBoundsException. ""))
              2 (throw (ex-info "" {}))
              :whatever))))

    (.set check-box 0) ;reset it

    ;; found an answer before max-retries
    (is (= :whatever
           (with-max-error-retries 3 [ArithmeticException IndexOutOfBoundsException ExceptionInfo]
             (condp = (.getAndIncrement check-box)
               0 (throw (ArithmeticException. ""))
               1 (throw (IndexOutOfBoundsException. ""))
               2 :whatever
               nil))))
    )
  )


(deftest with-retries-timeout-tests
  (let [check-box (AtomicInteger. 0)]

    ;; timeout limit reached - return :done
    (is (= :done (with-retries-timeout 10 :done nil? (.incrementAndGet check-box))))

    (.set check-box 0)
    ;; answer found before timeout
    (is (= 1 (with-retries-timeout 20 :done some? (.incrementAndGet check-box))))
    )
  )


