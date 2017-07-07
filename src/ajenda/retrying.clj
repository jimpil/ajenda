(ns ajenda.retrying
  (:require [ajenda.expiring :as schedule]
            [ajenda.utils :as ut])
  (:import (java.util.concurrent.atomic AtomicLong)))

(defn max-retries-limiter
  "Returns `(partial > max-retries)`."
  [max-retries]
  (assert (and (integer? max-retries)
               (pos? max-retries))
          "Negative or zero <max-retries> is not allowed!")
  (partial > max-retries))

(defn exception-success-condition
  "Returns `(partial not= ::error)`."
  [exceptions]
  (assert (every? (partial instance? Class) exceptions))
  (partial not= ::error)) ;; anything that doesn't throw one of <exceptions> succeeds


(defn multiplicative-delay
  "Returns a function which will block the current thread via `Thread/sleep`
   using increasing/decreasing <ms> (positive amount of milliseconds).
   The rate of increase/decrease is controlled by <multiplier> (a positive number).
   This has multiplication semantics. At each retry you will get
   `(* ms (Math/pow multiplier retry))` delaying. If <multiplier> & <ms> are the same value,
   what you get is essentially `exponential-backoff` style of delaying."
  [ms multiplier]
  (assert (pos? ms)
          "Negative or zero <ms> is NOT allowed!")
  (assert (and (pos? multiplier)
               (not= 1 multiplier))
          "Negative or zero <multiplier> is NOT allowed! Neither is `1` (see `fixed-delay` for that effect)...")

  (fn [retry]
    (when-not (ut/thread-interrupted?) ;; skip delaying if we've time-outed already!
      (Thread/sleep
        (* ms (Math/pow multiplier retry))))))

(defn exponential-delay
  "Returns a function which will block the current thread via `Thread/sleep`,
   using exponential delaying. See `multiplicative-delay` for details."
  [ms]
  (multiplicative-delay ms ms))


(defn additive-delay
  "Returns a function which will block the current thread via `Thread/sleep`,
   using fixed increments. This has addition semantics.
   At each retry you will get `(+ current-ms fixed-increment)` delaying."
  [ms fixed-increment]
  (assert (pos? ms)
          "Negative or zero <ms> is NOT allowed!")

  (let [dlay-ms (AtomicLong. ms)]
    (fn [_]
      (when-not (ut/thread-interrupted?) ;; skip delaying if we've time-outed already!
        (Thread/sleep
          (.getAndAdd dlay-ms fixed-increment))))))

(defn fixed-delay
  "Returns a function which will block the current thread via `Thread/sleep`
   using fixed <ms> (positive amount of milliseconds)."
  [ms]
  (assert (pos? ms)
          "Negative or zero <ms> is NOT allowed!")

  (fn [_]
    (when-not (ut/thread-interrupted?) ;; skip delaying if we've time-outed already!
      (Thread/sleep ms))))



;;GENERIC - CONDITION FOCUSED (bottom level utility)
;;=================================================

(defn with-retries*
  "Retries <f> (a fn of no args) until either <done?> (a fn of 1 arg: the result of `(f)`)
   returns a truthy value, in which case  the result of `(f)` is returned,
   or <retry?> (a fn of 1 arg - the current retry number) returns nil/false,
   in which case nil is returned.

   <opts> can be a map supporting the following options:

  :retry-fn!    A (presumably side-effecting) function of 1 argument (the current retrying attempt).
                Logging can be implemented on top of this for example.

  :delay-fn!    A delay producing function of 1 argument (the current retrying attempt).
                See `fixed-delay`, `additive-delay`, `multiplicative-delay` & `exponential-delay` for examples."
  ([retry? done? f]
   (with-retries* retry? done? f nil))
  ([retry? done? f opts]
   (let [{:keys [retry-fn! delay-fn!]} opts
         retry!+delay! (if (fn? retry-fn!)
                         (if (fn? delay-fn!)
                           (fn [i]
                             ;; do both
                             (retry-fn! i)
                             (delay-fn! i))
                           (fn [i]
                             ;; only retry
                             (retry-fn! i)))
                         (if (fn? delay-fn!)
                           (fn [i]
                             ;;only delay
                             (delay-fn! i))
                           ut/do-nothing))]
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
  (cond-> `(with-retries*
             (constantly true)
             ~condition
             (fn [] ~@body))

          opts (concat `[~opts])))


;; MAX-RETRIES FOCUSED
;;====================

(defmacro with-max-retries
  "Like <with-retries>, but with a  retries limit.
  <opts> as per `with-retries*`."
  [opts max-retries condition & body]
  (cond-> `(with-retries*
             (max-retries-limiter ~max-retries)
             ~condition
             (fn [] ~@body))

          opts (concat `[~opts])))


;; EXCEPTION FOCUSED
;;==================

(defmacro with-error-retries
  "Retries <body> for as long as it throws one of the
  <exceptions> (a vector of exception classes).
  <opts> as per `with-retries*`, including the following:
  
   :ex-pred    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not). Provides finer control by giving a 
                chance to recover from a particular exception."
  [opts exceptions & body]
  (cond-> `(with-retries*
             (constantly true)
             (exception-success-condition ~exceptions)
             (fn []
               (ut/try+
                 (do ~@body)
                 (catch-all ~exceptions ~'_e_
                            (if-let [pred# (:ex-pred ~opts)]
                              (or (pred# ~'_e_)
                                  :ajenda.retrying/error)
                              :ajenda.retrying/error)))))

          opts (concat `[~opts])))

(defmacro with-max-error-retries
  "The combination of `with-max-retries` & `with-error-retries`.
   Retries <body> until either <max-retries> is reached, or
   it doesn't throw one of <exceptions> (a vector of exception classes).
   <opts> as per `with-retries*`, including the following:

   :ex-pred    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not). Provides finer control by giving a
                chance to recover from a particular exception."
  [opts max-retries exceptions & body]
  (cond-> `(with-retries*
             (max-retries-limiter ~max-retries)
             (exception-success-condition ~exceptions)
             (fn []
               (ut/try+
                 (do ~@body)
                 (catch-all ~exceptions ~'_e_  ;; not the most hygienic thing to do
                            (if-let [pred# (:ex-pred ~opts)]
                              (or (pred# ~'_e_) 
                                  :ajenda.retrying/error)
                              :ajenda.retrying/error)))))

          opts (concat `[~opts])))


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
  `(schedule/with-timeout ~timeout nil ~timeout-res
     (with-retries*
       (every-pred (max-retries-limiter ~max-retries)
                   (complement ut/thread-interrupted?))
       ~condition
       (fn [] ~@body)
       ~opts)))

(defmacro with-error-retries-timeout
  "Like `with-error-retries`, but with a timeout.
  <opts> as per `with-retries*`, including the following:

   :ex-pred    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not). Provides finer control by giving a
                chance to recover from a particular exception."
  [opts timeout timeout-res exceptions & body]
  `(schedule/with-timeout ~timeout nil ~timeout-res
     (with-retries*
       (complement ut/thread-interrupted?)
       (exception-success-condition ~exceptions)
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions ~'_e_
                      (if-let [pred# (:ex-pred ~opts)]
                        (or (pred# ~'_e_)
                            :ajenda.retrying/error)
                        :ajenda.retrying/error))))
       ~opts)))

(defmacro with-max-error-retries-timeout
  "Like `with-error-retries-timeout`, but with a retries limit.
  <opts> as per `with-retries*`, including the following:

   :ex-pred    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not). Provides finer control by giving a
                chance to recover from a particular exception."
  [opts timeout timeout-res max-retries exceptions & body]
  `(schedule/with-timeout ~timeout nil ~timeout-res
     (with-retries*
       (every-pred (max-retries-limiter ~max-retries)
                   (complement ut/thread-interrupted?))
       (exception-success-condition ~exceptions)
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions ~'_e_
                      (if-let [pred# (:ex-pred ~opts)]
                        (or (pred# ~'_e_)
                            :ajenda.retrying/error)
                        :ajenda.retrying/error))))
       ~opts)))

