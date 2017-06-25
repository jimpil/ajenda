(ns ajenda.expiring
  (:import (java.util.concurrent ExecutorService TimeUnit Future TimeoutException ExecutionException)))

(defn with-timeout*
  "Executes `(f)` and waits for the result until <timeout> (e.g. a positive number)
   of <unit> (e.g. TimeUnit/MILLISECONDS), after which <timeout-res>, a `delay`, is forced.
   Very similar to the 2-arg version of `clojure.core/deref-future`, but also cancels the future,
   upon timeout.

   ATTENTION: Cancelling the future doesn't necessarily stop the thread.
   The code running on that thread must be actively checking for the interrupt signal,
   in order for the execution to stop gracefully (see `ajenda.retrying/with-retries-timeout` for an example)."
  ([[_timeout _unit :as t] timeout-res f]
   (with-timeout* t nil timeout-res f))
  ([[timeout unit] ^ExecutorService pool timeout-res ^Callable f]
   (assert (pos? timeout) "<timeout> must be a positive number (of time-units).")
   (assert (instance? TimeUnit unit) "<time-unit> must be an instance of `java.util.concurrent.TimeUnit`.")
   (let [^Future fut (if pool
                       (.submit pool f)
                       (future (f)))]
     (try
       (.get fut timeout unit)
       (catch TimeoutException _
         (do (future-cancel fut)
             (force timeout-res)))
       (catch ExecutionException x
         (throw (.getCause x)))))))


(defmacro with-timeout
  "A generic way to execute arbitrary code (which returns a result) with a timeout in milliseconds (<ms>).
   This is essentially the same as `clojure.core/deref-future`
   with the only difference being that we cancel the future before exiting.
   <executor-service> ^ExecutorService to submit the body to - if nil uses clojure.core/future.
   Blocks the current thread (via `Future.get(...)`)."
  [ms executor-service timeout-result & body]
  `(with-timeout* [~ms TimeUnit/MILLISECONDS]
     ~executor-service
     (delay ~timeout-result)
     (fn [] ~@body)))
