(ns ajenda.retrying
  (:require [ajenda.expiring :as schedule]
            [ajenda.utils :as ut]))

(defn with-retries*
  "Retries <f> (a fn of no args) until either <done?> (a fn of 1 arg: the result of `(f)`) returns a truthy value,
  in which case  the result of `(f)` is returned, or <retry?> (a fn of 1 arg - the current retry number) returns false,
  in which case nil is returned."
  [retry? done? f]
  (loop [i 0]
    (when (retry? i)
      (let [res (f)]
        (if (done? res)
          res
          (recur (unchecked-inc i)))))))

(defmacro with-max-retries
  "Like <with-retries*>, but accepts an arbitrary number of forms as <body>."
  [max-retries condition & body]
  `(do
     (assert (pos? ~max-retries) "<with-max-retries> expects a positive number for <max-retries>!")
     (with-retries*
       (partial > ~max-retries)
       ~condition
       (fn [] ~@body))))


(defmacro with-retries
  "Like <with-max-retries>, but with no retries limit.
   Will keep retrying until <condition> returns a truthy value."
  [condition & body]
  `(with-retries*
     (constantly true)
     ~condition
     (fn [] ~@body)))

(defmacro with-retries-timeout
  "Like <with-retries>, but with a timeout.
   Will keep retrying until <condition> returns a truthy value,
   OR the execution thread gets interrupted (which is what happens when <timeout> ms have elapsed)."
  [timeout-ms timeout-res condition & body]
  `(schedule/with-timeout ~timeout-ms nil ~timeout-res
     (with-retries*
       (complement ut/thread-interrupted?)
       ~condition
       (fn [] ~@body))))

(defmacro with-max-retries-timeout
  "Like <with-retries-timeout>, but with a retries limit.
   Will retry <max-retries> times, or until <condition> returns a truthy value,
   OR the execution thread gets interrupted (which is what happens when <timeout> ms have elapsed)."
  [timeout timeout-res max-retries condition & body]
  `(do
     (assert (pos? ~max-retries) "<with-max-retries-timeout> expects a positive number for <max-retries>!")
     (schedule/with-timeout ~timeout nil ~timeout-res
       (with-retries*
         (every-pred (partial > ~max-retries)
                     (complement ut/thread-interrupted?))
         ~condition
         (fn [] ~@body)))))
