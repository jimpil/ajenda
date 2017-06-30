(ns ajenda.retrying
  (:require [ajenda.expiring :as schedule]
            [ajenda.utils :as ut]))

;;GENERIC - CONDITION FOCUSED (bottom level utility)
;;=================================================

(defn with-retries*
  "Retries <f> (a fn of no args) until either <done?> (a fn of 1 arg: the result of `(f)`)
  returns a truthy value, in which case  the result of `(f)` is returned,
  or <retry?> (a fn of 1 arg - the current retry number) returns false,
  in which case nil is returned."
  [retry? done? f]
  (loop [i 0]
    (when (retry? i)
      (let [res (f)]
        (if (done? res)
          res
          (recur (unchecked-inc i)))))))

(defmacro with-retries
  "Retries <body> until <condition> returns a truthy value."
  [condition & body]
  `(with-retries*
     (constantly true)
     ~condition
     (fn [] ~@body)))


;; MAX-RETRIES FOCUSED
;;====================

(defmacro with-max-retries
  "Like <with-retries>, but with a  retries limit."
  [max-retries condition & body]
  `(do
     (assert (pos? ~max-retries)
             "<with-max-retries> expects a positive number for <max-retries>!")

     (with-retries*
       (partial > ~max-retries)
       ~condition
       (fn [] ~@body))))


;; EXCEPTION FOCUSED
;;==================

(defmacro with-error-retries
  "Retries <body> for as long as it throws one of the
  <exceptions> (a vector of exception classes)."
  [exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (with-retries*
       (constantly true)
       (partial not= ::error) ;; anything that doesn't throw one of <exceptions> succeeds
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions ::error))))))

(defmacro with-max-error-retries
  "The combination of `with-max-retries` & `with-error-retries`.
   Retries <body> until either <max-retries> is reached, or
   it doesn't throw one of <exceptions> (a vector of exception classes)."
  [max-retries exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (with-retries*
       (partial > ~max-retries)
       (partial not= ::error) ;; anything that doesn't throw one of <exceptions> succeeds
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions ::error))))))


;; TIMEOUT FOCUSED
;;================

(defmacro with-retries-timeout
  "Like <with-retries>, but with a timeout.
   Will keep retrying until <condition> returns a truthy value,
   or <timeout> ms have elapsed, in which case <timeout-res> is returned."
  [timeout-ms timeout-res condition & body]
  `(schedule/with-timeout ~timeout-ms nil ~timeout-res
     (with-retries*
       (complement ut/thread-interrupted?)
       ~condition
       (fn [] ~@body))))

(defmacro with-max-retries-timeout
  "Like <with-retries-timeout>, but with a retries limit.
   Will keep retrying until <max-retries> is reached,
   or until <condition> returns a truthy value,
   or <timeout> ms have elapsed."
  [timeout timeout-res max-retries condition & body]
  `(do
     (assert (pos? ~max-retries)
             "<with-max-retries-timeout> expects a positive number for <max-retries>!")
     (assert (pos? ~timeout)
             "<with-max-retries-timeout> expects a positive number for <timeout>!")

     (schedule/with-timeout ~timeout nil ~timeout-res
       (with-retries*
         (every-pred (partial > ~max-retries)
                     (complement ut/thread-interrupted?))
         ~condition
         (fn [] ~@body)))))

(defmacro with-error-retries-timeout
  "Like `with-error-retries`, but with a timeout."
  [timeout timeout-res exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (assert (pos? ~timeout)
             "<with-error-retries-timeout> expects a positive number for <timeout>!")

     (schedule/with-timeout ~timeout nil ~timeout-res
       (with-retries*
         (complement ut/thread-interrupted?)
         (partial not= ::error) ;; anything that doesn't throw one of <exceptions> succeeds
         (fn []
           (ut/try+
             (do ~@body)
             (catch-all ~exceptions ::error)))))))

(defmacro with-max-error-retries-timeout
  "Like `with-error-retries-timeout`, but with a retries limit."
  [timeout timeout-res max-retries exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (assert (pos? ~timeout)
             "<with-max-error-retries-timeout> expects a positive number for <timeout>!")
     (assert (pos? ~max-retries)
             "<with-max-error-retries-timeout> expects a positive number for <max-retries>!")

     (schedule/with-timeout ~timeout nil ~timeout-res
       (with-retries*
         (every-pred (partial > ~max-retries)
                     (complement ut/thread-interrupted?))
         (partial not= ::error) ;; anything that doesn't throw one of <exceptions> succeeds
         (fn []
           (ut/try+
             (do ~@body)
             (catch-all ~exceptions ::error)))))))