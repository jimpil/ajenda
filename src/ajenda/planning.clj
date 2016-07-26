(ns ajenda.planning
  (:require [ajenda.utils :as ut])
  (:import (java.util.concurrent ScheduledExecutorService TimeUnit)
           (java.time LocalDateTime Duration)
           (java.time.format DateTimeParseException)))


(defn periodically*
  "Schedules periodic execution of `(f)` at a fixed rate <interval>.
   The 3-arg arity expects an initial <delay>, while the 4-arg one
   expects your your own <pool> (java.util.concurrent.ScheduledExecutorService).
   Returns a function of no arguments to unschedule <f>."
  ([interval f]
   (periodically* 0 interval f))
  ([dlay interval f]
   (periodically* dlay interval nil f))
  ([dlay interval pool f]
   (when pool
     (assert (or (nil? pool)
                 (instance? ScheduledExecutorService pool))
             "<periodically*> expects either nil, or a ScheduledExecutorService as <pool>. "))
   (let [^ScheduledExecutorService pool (or pool (ut/thread-pool :scheduled-solo nil))
         fut (.scheduleAtFixedRate pool f (or dlay 0) interval TimeUnit/MILLISECONDS)]
     #(future-cancel fut))))

(defmacro periodically
  "Same as`periodically*`, but accepts an arbitrary number of forms as <body>."
  [dlay interval exec & body]
  `(periodically* ~dlay ~interval ~exec (fn [] ~@body)))


(defn do-after*
  "Schedules <f> (a fn of no args) for execution after <ms> milliseconds.
   You typically use this to trigger a side effect after some delay (for 0 delay you can simply use `future`).
   Returns a function of no arguments to unschedule <f> if possible (which depends on a number of things).
   Most commonly, the task will NOT execute if it's cancelled before having been started.
   Otherwise, the code you're running must be actively checking for the interrupt flag."
  [dlay ^ScheduledExecutorService pool f]
  (when pool
    (assert (or (nil? pool)
                (instance? ScheduledExecutorService pool))
            "<do-after*> expects either nil, or a ScheduledExecutorService as <pool>. "))
  (let [^ScheduledExecutorService pool (or pool (ut/thread-pool :scheduled-solo nil))
        fut (.schedule pool ^Callable f (long dlay) TimeUnit/MILLISECONDS)]
    #(future-cancel fut)))

(defmacro do-after!
  "Like `do-after*` but accepts an arbitrary number of forms as <body>."
  [dlay exec & body]
  `(do
     (assert (pos? ~dlay) "<do-after!> expects a positive number (milliseconds) for <dlay>...")
     (do-after* ~dlay ~exec (fn [] ~@body))))

(defn do-at*
  "Schedules <f> (a fn of no args) for execution at the specified
  <date-time> (yyyy-MM-dd'T'HH:mm:ss.SSSZ - aka DateTimeFormatter/ISO_DATE_TIME),
  on thread-pool <pool> (can be nil)."
  [pool date-time f]
  (let [parsed-time (try
                      (cond
                        (string? date-time) (LocalDateTime/parse date-time ut/rfc3339-formatter)
                        (instance? LocalDateTime date-time) date-time
                        :else
                        (throw (ex-info "Can either accept LocalDateTime objects or Strings as <date-time>"
                                        {:provided date-time
                                         :class (class date-time)})))
                      (catch DateTimeParseException _
                        (throw (IllegalArgumentException.
                                 (format "Could not parse [%s] as an ISO_DATE_TIME DateTime (yyyy-MM-dd'T'HH:mm:ss.SSSZ)..." date-time)))))
        now (LocalDateTime/now)
        diff (.toMillis (Duration/between now parsed-time))]
    (assert (pos? diff) (format "DateTime specified [%s] is in the past!" date-time))
    (do-after* diff pool f)))


(defmacro do-at!
  "Like `do-at*` but accepts an arbitrary number of forms as <body>."
  [time-string pool & body]
  `(do-at* ~pool  ~time-string (fn [] ~@body)))


(defn- multi-schedule
  "Private helper for removing boilerplate."
  [global-timeout schedule! schedules]
  (let [stoppers (into {} (map (fn [[id task-details]]
                                 [id (apply schedule! task-details)]))
                       schedules)
        stop-id (fn cancel
                  ([]
                   (cancel :all))
                  ([id]
                   (let [all? (= id :all)
                         stop (when-not all?
                                (get stoppers id))]
                     (when (or all? stop) ;; do nothing for an invalid id
                       (if stop
                         (stop)
                         (mapv (fn [[_ stop]] (stop)) stoppers))))))
        cancel-timeout (when (integer? global-timeout)
                         (do-after* global-timeout nil stop-id))]

    [stop-id cancel-timeout]))

(defn adhoc-multischedule
  "Takes a map with entries like `task-id => [time-string f]`,
   and schedules <f> at the specified <time-string> (yyyy-MM-dd'T'HH:mm:ss.SSSZ).
   Returns a vector with 2 functions. First one will always be there and will expect either 1 argument -
   the id of the task  you want cancelled, or no-args if you happen to want to cancel them all.
   If you didn't provide a <global-timeout> the second item in the vector will be nil,
   otherwise it will be a function which cancels the <global-timeout>."
  [global-timeout pool schedules]
  (let [pool (or pool (ut/thread-pool :scheduled (count schedules)))]
    (multi-schedule global-timeout (partial do-at* pool) schedules)))


(defn periodic-multischedule
  "Builds on top of `periodically` and allows for an arbitrary number of jobs
   to be scheduled at once (can however be stopped independently).
   <schedules> must be a map  (with entries of the form `[id [delay interval f]]`).
   The scheduling will be happen on thread pool <pool> (by default with n threads where n is the number of tasks to schedule).
   Returns a vector with 2 functions. First one will always be there and will expect either 1 argument - the id of the task
   you want cancelled, or no-args if you happen to want to cancel them all. If you didn't provide a <global-timeout>
   the second item in the vector will be nil, otherwise it will be a function which cancels the <global-timeout>."
  [global-timeout pool schedules]
  (let [pool (or pool (ut/thread-pool :scheduled (count schedules)))]
    (multi-schedule global-timeout (fn [dlay interval f]
                                     (periodically* (or dlay 0) interval pool f))
                    schedules)))


