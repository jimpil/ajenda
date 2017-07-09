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
  [f [n-calls ^TimeUnit time-unit n-units :as rate]]
  (let [times-called (AtomicLong. 0)
        nanos-per-unit (.toNanos time-unit (or n-units 1))
        t (AtomicLong. 0)  ;; not been called yet
        do-f (fn [first-time? args]
               (try (apply f args)
                    (finally
                      (if first-time?
                        (.addAndGet times-called 2) ;; go from 0 => 2 in order to simulate going from 1 => 2
                        (.incrementAndGet times-called))))) ;; go from n => n+1
        ]

    (fn [& args]
      (let [calls-so-far (.get times-called)
            first-time? (zero? calls-so-far)
            now (System/nanoTime)
            previous-time (if first-time?
                            ;; very first time round - initialise it properly
                            (.addAndGet t now)
                            (.get t))
            time-elapsed (- now previous-time)]
        ;(println time-elapsed)

        (if (>= time-elapsed nanos-per-unit)
          (do ;; reset everything
            ;(println "resetting everything...")
            (.set times-called 1) ;; don't start from 0 now
            (.set t now)
            ;; don't forget to call <f>
            (do-f first-time? args))

          (when (>= n-calls calls-so-far)
            ;; all good -call <f>
            (do-f first-time? args))
          )
        )
      )
    )
  )
