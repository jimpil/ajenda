(ns ajenda.retrying
  (:require [ajenda.expiring :as schedule]
            [ajenda.utils :as ut]))

(defn retryable-error
  [cause]
  (ex-info "Retryable error" {:retry? true} cause))

(defn retryable-error?
  [x]
  (some-> x ex-data :retry?))

(defmacro handle-error
  "Helper for handling (potentially retryable) errors."
  [e opts]
  `(let [e# ~e]
     (if-let [pred# (:halt-on ~opts)]
       (if (pred# e#)
         ;; If :halt-on returns truthy we don't want to retry
         ;; so we throw something non-retryable
         (throw e#)
         ;; Otherwise we indicate we want to retry
         (retryable-error e#))
       ;; Always retry if :halt-on not specified
       (retryable-error e#))))

(defn max-retries-limiter
  "Returns `(partial >= max-retries)`."
  [max-retries]
  (assert (and (integer? max-retries)
               (not (neg? max-retries)))
          "Non-negative integer is required for <max-retries>.")
  (partial >= max-retries))

(def exception-success-condition
  "Returns `(complement retryable-error?)`."
  ;; success predicate - done if not retryable
  (complement retryable-error?))

;;DELAYING STRATEGIES
;;===================
(defn multiplicative-delay
  "Returns a function that will calculate the amount of delaying
   given the current retrying attempt, with multiplication semantics.
   At each retry you will get `(* ms (Math/pow multiplier retry))` delaying.
   If <multiplier> & <ms> are equal, what you get is essentially
   `exponential-backoff` style of delaying."
  [ms multiplier]
  (assert (pos? ms)
          "Negative or zero <ms> is NOT allowed!")
  (assert (and (pos? multiplier)
               (not= 1 multiplier))
          "Negative or zero <multiplier> is NOT allowed! Neither is `1` (see `fixed-delay` for that effect)...")

  (fn [i]
    ;; each delay blocks the next retry, so <i> will never be 0.
    ;; however in order to calculate it correctly we must take
    ;; into account the very first attempt too (hence need to decrement <i>)
    (long (* ms (Math/pow multiplier (unchecked-dec i))))))

(defn exponential-delay
  "A special case of `multiplicative-delay`."
  [ms]
  (multiplicative-delay ms ms))


(defn additive-delay
  "Returns a function that will calculate the amount of delaying
   given the current retrying attempt, with addition semantics.
   At each retry you will get `(+ ms (* fixed-increment (dec i)))` delaying."
  [ms fixed-increment]
  (assert (pos? ms)
          "Negative or zero <ms> is NOT allowed!")

  (fn [i]
    ;; each delay blocks the next retry, so <i> will never be 0.
    ;; however in order to calculate it correctly we must take
    ;; into account the very first attempt too (hence need to decrement <i>)
    (+ ms (* fixed-increment (unchecked-dec i)))))

(defn cyclic-delay
  "Returns a fn with the same effect as `(partial nth (cycle mss))`."
  [mss]
  (assert (every? pos? mss)
          "Negative or zero delay is NOT allowed!")

  (let [ceiling (count mss)
        m (zipmap (range ceiling) mss)]
    (fn [i] ;; O(1) implementation of `nth` for cycles
      ;; no need to do decrement <i> here because `(= 0 (rem x x))`
      (m (rem i ceiling))))
  ; O(n)
  ;(partial nth (cycle mss))
  )


(defn oscillating-delay
  "Returns `(cyclic-delay [xms yms])`."
  [xms yms]
  (cyclic-delay [xms yms]))

(defn fixed-delay
  "Returns `(constantly ms)`."
  [ms]
  (assert (pos? ms)
          "Negative or zero <ms> is NOT allowed!")
  (constantly ms))

(defn default-log-fn
  "Prints a rudimentary message to stdout about the current retrying attempt,
   and the (potential) upcoming delay."
  ([retry-attempt]
   (default-log-fn (constantly 0) retry-attempt))
  ([delay-calc retry-attempt]
  (println
    (format "Attempt #%s failed! Retrying [%s] in %s ms ..."
            (unchecked-dec retry-attempt)
            retry-attempt
            (delay-calc retry-attempt)))))

(defn default-delay-fn!
  "Blocks the thread via `(Thread/sleep ms)`."
  [ms]
  ;; need to be careful here as delaying strategies could be decreasing,
  ;; and in some cases they might start return negative values (e.g. additive delay)
  (when (and (pos? ms)
             ;; NEVER attempt to call `.sleep()` on an interrupted thread!
             (not (ut/thread-interrupted?)))
    (Thread/sleep ms)))

(defonce do-nothing
  (constantly nil))

;;GENERIC - CONDITION FOCUSED (bottom level utility)
;;=================================================

(defn with-retries*
  "Retries <f> (a fn of no args) until either <done?> (a fn of 1 arg: the result of `(f)`)
   returns a truthy value, in which case  the result of `(f)` is returned,
   or <try?> (a fn of 1 arg - the current attempt number) returns nil/false,
   in which case nil is returned.

   <opts> can be a map supporting the following options:

  :retry-fn!    A (presumably side-effecting) function of 1 argument (the current retrying attempt).
                Runs after the first attempt, and before each retry apart from the very last one.
                As such, this fn will never see `0` because it always refers to the upcoming retry.
                Logging can be implemented on top of this. See `default-log-fn` for an example.

  :delay-fn!    A function of 1 argument (the number of milliseconds) which blocks the current thread.
                The default is `default-delay-fn!`, and it should suffice for the vast majority of use-cases.
                Runs only if `:delay-calc` has been provided.

  :delay-calc   A function of 1 argument (the current retrying attempt), returning the amount of milliseconds.
                This is expected to be a pure function, which can called more than once (e.g. by `:retry-fn!`).
                See `fixed-delay`, `additive-delay`, `multiplicative-delay` & `exponential-delay` for examples."
  ([retry? done? f]
   (with-retries* retry? done? f nil))
  ([retry? done? f opts]
   (let [{:keys [retry-fn! delay-fn! delay-calc]
          :or {delay-fn! default-delay-fn!}} opts
         retry!+delay! (if (fn? retry-fn!)
                         (if (fn? delay-calc)
                           (fn [i]
                             ;; do both
                             (retry-fn! i)
                             (delay-fn! (delay-calc i)))
                           (fn [i]
                             ;; only retry
                             (retry-fn! i)))
                         (if (fn? delay-calc)
                           (fn [i]
                             ;;only delay
                             (delay-fn! (delay-calc i)))
                           do-nothing))]
     (loop [i 0 ;; start from 0 - the very first attempt is not a retry!
            res (f)]
       (if (done? res)
         res
         ;; What's the most sensible thing to do after Long/MAX_VALUE retries?
         ;; keep retrying with a bad counter, give up (i.e. throw), or keep retrying with
         ;; a good counter? I'd like to say that keep retrying is the right thing to do,
         ;; but at the same time I feel that auto-promoting (`inc'`)  would be an overkill here.
         ;; If someone retries something for that many times, chances are he doesn't care
         ;; about the number of attempts (e.g. timeout). So it seems `unchecked-inc` is the
         ;; best option here.
         (let [next-i (unchecked-inc i)]
           (if (retry? next-i)
             (do
               ;; clever optimisation which looks one step ahead and skips the final retries/delays
               ;; this enables using 0 as the number of max-retries and no retries/delays will happen
               (retry!+delay! next-i)
               (recur next-i (f)))
             ;; Finished trying - check for retryable-error
             (if (retryable-error? res)
               ;; and throw its cause as we're done retrying
               (throw (.getCause res))
               ;;otherwise throw fixed error with some info
               (throw (ex-info "Retries exhausted!"
                               {:retried i
                                :last-result res}))))))))))

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
  
   :halt-on    A fn of 1 argument (the exception object thrown), responsible for deciding
               whether retrying should occur (or not)."
  [opts exceptions & body]
  (cond-> `(with-retries*
             (constantly true)
             exception-success-condition
             (fn []
               (ut/try+
                 (do ~@body)
                 (catch-all ~exceptions e#
                            (handle-error e# ~opts)))))

          opts (concat `[~opts])))

(defmacro with-max-error-retries
  "The combination of `with-max-retries` & `with-error-retries`.
   Retries <body> until either <max-retries> is reached, or
   it doesn't throw one of <exceptions> (a vector of exception classes).
   <opts> as per `with-retries*`, including the following:

   :halt-on    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not)."
  [opts max-retries exceptions & body]
  (cond-> `(with-retries*
             (max-retries-limiter ~max-retries)
             exception-success-condition
             (fn []
               (ut/try+
                 (do ~@body)
                 (catch-all ~exceptions e#
                            (handle-error e# ~opts)))))

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

   :halt-on    A fn of 1 argument (the exception object thrown), responsible for deciding
               whether retrying should occur (or not)."
  [opts timeout timeout-res exceptions & body]
  `(schedule/with-timeout ~timeout nil ~timeout-res
     (with-retries*
       (complement ut/thread-interrupted?)
       exception-success-condition
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions e#
                      (handle-error e# ~opts))))
       ~opts)))

(defmacro with-max-error-retries-timeout
  "Like `with-error-retries-timeout`, but with a retries limit.
  <opts> as per `with-retries*`, including the following:

   :halt-on    A fn of 1 argument (the exception object thrown), responsible for deciding
                whether retrying should occur (or not)."
  [opts timeout timeout-res max-retries exceptions & body]
  `(schedule/with-timeout ~timeout nil ~timeout-res
     (with-retries*
       (every-pred (max-retries-limiter ~max-retries)
                   (complement ut/thread-interrupted?))
       exception-success-condition
       (fn []
         (ut/try+
           (do ~@body)
           (catch-all ~exceptions e#
                      (handle-error e# ~opts))))
       ~opts)))

