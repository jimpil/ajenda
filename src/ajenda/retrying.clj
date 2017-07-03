(ns ajenda.retrying
  (:require [ajenda.expiring :as schedule]
            [ajenda.utils :as ut]))

;;GENERIC - CONDITION FOCUSED (bottom level utility)
;;=================================================

(defn with-retries*
  "Retries <f> (a fn of no args) until either <done?> (a fn of 1 arg: the result of `(f)`)
  returns a truthy value, in which case  the result of `(f)` is returned,
  or <retry?> (a fn of 1 arg - the current retry number) returns nil/false,
  in which case nil is returned.

  <opts> can be a map supporting the following options:

  :retry-fn!    A (presumably side-effecting) function of 1 argument (the current retrying attempt).
                Logging can be implemented on top of this for example. Runs on the first retry onwards - NOT on
                the very first try (i.e. the fn will never see `0`).

  :delay-ms     Some integer value specifying how many milliseconds to delay retrying.
                It applies right after <retry-fn!>, and only on the first retry onwards
                (i.e. the very first try will NOT be delayed)."
  ([retry? done? f]
   (with-retries* retry? done? f nil))
  ([retry? done? f opts]
   (let [{:keys [retry-fn! delay-ms]} opts
         delay! (if (integer? delay-ms)
                  (fn [_]
                    (Thread/sleep delay-ms))
                  ut/do-nothing)
         retry!+delay! (cond-> delay!
                               (fn? retry-fn!) (comp retry-fn!))]
     (loop [i 0]
       (when (retry? i)
         (let [res (f)]
           (if (done? res)
             res
             (do (retry!+delay! i)
                 (recur (unchecked-inc i))))))))))

(defmacro with-retries
  "Retries <body> until <condition> returns a truthy value.
  <opts> as per `with-retries*`."
  [opts condition & body]
  `(with-retries*
     (constantly true)
     ~condition
     (fn [] ~@body)
     ~opts))


;; MAX-RETRIES FOCUSED
;;====================

(defmacro with-max-retries
  "Like <with-retries>, but with a  retries limit.
  <opts> as per `with-retries*`."
  [opts max-retries condition & body]
  `(do
     (assert (pos? ~max-retries)
             "<with-max-retries> expects a positive number for <max-retries>!")

     (with-retries*
       (partial > ~max-retries)
       ~condition
       (fn [] ~@body)
       ~opts)))


;; EXCEPTION FOCUSED
;;==================

(defmacro with-error-retries
  "Retries <body> for as long as it throws one of the
  <exceptions> (a vector of exception classes).
  <opts> as per `with-retries*`, including the following:
  
   :ex-pred#    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not). Provides finer control by giving a 
                chance to recover from a particular exception."
  [opts exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (with-retries*
       (constantly true)
       (partial not= :ajenda.retrying/error) ;; anything that doesn't throw one of <exceptions> succeeds
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions ~'_e_
             (if-let [pred# (:ex-pred# ~opts)] 
               (or (pred# ~'_e_)
                   :ajenda.retrying/error)
               :ajenda.retrying/error))))
       ~opts)))

(defmacro with-max-error-retries
  "The combination of `with-max-retries` & `with-error-retries`.
   Retries <body> until either <max-retries> is reached, or
   it doesn't throw one of <exceptions> (a vector of exception classes)."
  [opts max-retries exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (with-retries*
       (partial > ~max-retries)
       (partial not= :ajenda.retrying/error) ;; anything that doesn't throw one of <exceptions> succeeds
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions ~'_e_
             (if-let [pred# (:ex-pred# ~opts)]
               (or (pred# ~'_e_)
                   :ajenda.retrying/error)
               :ajenda.retrying/error))))
       ~opts)))


;; TIMEOUT FOCUSED
;;================

(defmacro with-retries-timeout
  "Like <with-retries>, but with a timeout.
   Will keep retrying until <condition> returns a truthy value,
   or <timeout> ms have elapsed, in which case <timeout-res> is returned.
   <opts> as per `with-retries*`."
  [opts timeout-ms timeout-res condition & body]
  `(schedule/with-timeout ~timeout-ms nil ~timeout-res
     (with-retries*
       (complement ut/thread-interrupted?)
       ~condition
       (fn [] ~@body)
       ~opts)))

(defmacro with-max-retries-timeout
  "Like <with-retries-timeout>, but with a retries limit.
   Will keep retrying until <max-retries> is reached,
   or until <condition> returns a truthy value,
   or <timeout> ms have elapsed. <opts> as per `with-retries*`."
  [opts timeout timeout-res max-retries condition & body]
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
         (fn [] ~@body)
         ~opts))))

(defmacro with-error-retries-timeout
  "Like `with-error-retries`, but with a timeout.
  <opts> as per `with-retries*`."
  [opts timeout timeout-res exceptions & body]
  `(do
     (assert (every? (partial instance? Class) ~exceptions))
     (assert (pos? ~timeout)
             "<with-error-retries-timeout> expects a positive number for <timeout>!")

     (schedule/with-timeout ~timeout nil ~timeout-res
       (with-retries*
         (complement ut/thread-interrupted?)
         (partial not= :ajenda.retrying/error) ;; anything that doesn't throw one of <exceptions> succeeds
         (fn []
           (ut/try+
             (do ~@body)
             (catch-all ~exceptions ~'_e_
               (if-let [pred# (:ex-pred# ~opts)]
                 (or (pred# ~'_e_)
                     :ajenda.retrying/error)
                 :ajenda.retrying/error))))
         ~opts))))

(defmacro with-max-error-retries-timeout
  "Like `with-error-retries-timeout`, but with a retries limit.
  <opts> as per `with-retries*`."
  [opts timeout timeout-res max-retries exceptions & body]
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
         (partial not= :ajenda.retrying/error) ;; anything that doesn't throw one of <exceptions> succeeds
         (fn []
           (ut/try+
             (do ~@body)
             (catch-all ~exceptions ~'_e_
               (if-let [pred# (:ex-pred# ~opts)]
                 (or (pred# ~'_e_)
                     :ajenda.retrying/error)
                 :ajenda.retrying/error))))
         ~opts))))