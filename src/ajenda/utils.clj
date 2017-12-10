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
                (thread-pool :solo n-threads factory)
                (Executors/newFixedThreadPool n-threads factory))
       :cached (Executors/newCachedThreadPool factory)
       :solo (Executors/newSingleThreadExecutor factory)))))


(defn thread-interrupted?
  "Wrapper around `Thread.isInterrupted`."
  ([_] ;; convenience overload (see `ajenda.retrying/with-max-retries-timeout`)
   (thread-interrupted?))
  ([]
   (.isInterrupted (Thread/currentThread))))

(defn- catch-all*
  "Produces a list of `catch` clauses for all exception classes <exs>
   with the same <catch-tail>."
  [[_catch-all exs & catch-tail]]
  (map #(list* 'catch % catch-tail) exs))

(def ^:private supported-catches
  {"catch" 'catch
   "catch-all" 'catch-all})

(defmacro try+
  "Same as `clojure.core/try`, but also recognises `catch-all` clause(s).
   These must be in the same form as regular `catch`, but instead of a
   single exception class, you are expected to provide a vector of them."
  [& bodies]
  (let [[body catches] (reduce
                         (fn [res [fsymbol & args :as exp]]
                           (let [fname (name fsymbol)]
                             (if (contains? supported-catches fname)
                             (update res 1 conj (list* (get supported-catches fname) args))
                             (update res 0 conj exp))))
                         [[][]]
                         bodies)
        catch-all? #(and (seq? %)
                         (= (first %) 'catch-all))
        catch-all-guard (fn [form]
                          (if (catch-all? form)
                            (catch-all* form)
                            [form]))]
    `(try ~@body
          ~@(mapcat catch-all-guard catches))))

