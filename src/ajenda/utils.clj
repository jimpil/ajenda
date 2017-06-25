(ns ajenda.utils
  (:import (java.util.concurrent ExecutorService ThreadFactory Executors)
           (java.time.format DateTimeFormatter)))

(defonce ^DateTimeFormatter rfc3339-formatter
         DateTimeFormatter/ISO_DATE_TIME)

(defn thread-pool
  (^ExecutorService [named-type n-threads]
   (thread-pool named-type n-threads (Executors/defaultThreadFactory)))
  (^ExecutorService [named-type n-threads factory]
   (let [^ThreadFactory factory factory]
     (case named-type
       :scheduled (Executors/newScheduledThreadPool n-threads factory)
       :scheduled-solo (Executors/newSingleThreadScheduledExecutor factory)
       :fixed (if (= 1 n-threads)
                (recur :solo n-threads factory)
                (Executors/newFixedThreadPool n-threads factory))
       :cached (Executors/newCachedThreadPool factory)
       :solo (Executors/newSingleThreadExecutor factory)))))


(defn thread-interrupted?
  ([_] ;; convenience overload (see `ajenda.retrying/with-max-retries-timeout`)
   (thread-interrupted?))
  ([]
   (.isInterrupted (Thread/currentThread))))
