(ns ajenda.limiting
  (:import [java.util.concurrent Semaphore TimeUnit]
           [java.util.concurrent.atomic AtomicLong]))


(defn permit-limited
  "Returns a function which wraps `f`. That fn can only be called
  <in-flight-limit> times without returning. As soon as it returns
  new calls can be made. This is a permit-based mechanism
  (see `java.util.concurrent.Semaphore`)."
  ([f in-flight-limit]
   (permit-limited f in-flight-limit false))
  ([f in-flight-limit fair?]
   (let [bucket (Semaphore. in-flight-limit fair?)]
     (fn [& args]
       (.acquire bucket) ;; acquire 1 permit
       (try (apply f args)
            (finally
              (.release bucket))))))) ;; release 1 permit


(defn rate-limited
  "Returns a function which wraps `f`. That fn can only be called
   <n-calls> times during the specified <time-unit> (java.util.concurrent.TimeUnit).
   Any further calls will return nil without calling <f> (noop). As soon as the specified
   <time-unit> passes, new calls will start calling <f> again."

  ([f rate]
   (rate-limited f rate (constantly nil)))
  ([f [n-calls ^TimeUnit time-unit n-units :as rate] log-fn]
   (let [times-called (AtomicLong. 0) ;; not been called yet
         nanos-per-unit (.toNanos time-unit (or n-units 1))
         t (AtomicLong. 0)
         do-f (fn [args]
                (try (apply f args)
                     (finally
                       (log-fn (.incrementAndGet times-called))))) ;; go from n => n+1
         ]

     (fn [& args]
       (let [previous-time* (.get t)
             calls-so-far (.get times-called)
             first-time? (zero? previous-time*) ;; this will be true the very first time only
             now (System/nanoTime)
             previous-time (if first-time?
                             ;; very first time being called - initialise it properly
                             (.addAndGet t now)
                             previous-time*)
             time-elapsed (- now previous-time)]
         ;(println time-elapsed)

         (if (>= time-elapsed nanos-per-unit)
           (do ;; reset everything
             ;(println "resetting everything...")
             (.set times-called 0)
             (.set t now)
             ;; don't forget to call <f>
             (do-f args))

           (when (> n-calls calls-so-far) ;; call count starts at 0 so use `>`
             ;; all good -call <f>
             (do-f args))
           )
         )
       )
     )
    )
  )
