# ajenda
![ajenda](https://cdn1.iconfinder.com/data/icons/significon/512/Significon-List-512.png)

Clojure utility functions for scheduling side effects. The focus is on retrying, expiring (aka timeout-ing) & planning (aka scheduling). 

## Usage

### Retrying 

`ajenda.retrying` exposes 4 macros:

1. *with-max-retries* [options max-retries condition & body]
Repeatedly executes `<body>` until either it returns a satisfactory result (via `<condition>`), or `<max-retries>` has been reached.

Example:

```clj
(with-max-retries nil 3 some? 
  (println "Hi!"))
Hi!
Hi!
Hi!
=> nil

(with-max-retries nil 3 nil? 
  (println "Hi!"))
Hi!
=> nil
```


2. *with-retries* [options condition & body]
Repeatedly executes `<body>` until it returns a satisfactory result (via `<condition>`).

Example:

```clj
(let [state (atom 0)]
  (with-retries nil (partial = 5) 
    (println "Hi!")
    (swap! state inc)))
Hi!
Hi!
Hi!
Hi!
Hi!
=> 5

;; INFINITE LOOP - BE CAREFUL WITH THIS! 
;; SEE `with-retries-timeout` BELOW FOR A SAFER ALTERNATIVE.

(with-retries nil some? 
  (println "Hi!"))
  
```

3. *with-retries-timeout* [options timeout-ms timeout-res condition & body]
Repeatedly executes `<body>` until either it returns a satisfactory result (via `<condition>`), or `<timeout-ms>` has elapsed, 
in which case `<timeout-res>` is returned.

Example:

```clj
(with-retries-timeout nil 10 :END some?   ;; stop retrying after 10 millis
  (println "Hi!"))
Hi!
Hi!
...
...
...
=> :END
  
```


4. *with-max-retries-timeout* [options timeout timeout-res max-retries condition & body]
Repeatedly executes `<body>` until either it returns a satisfactory result (via `<condition>`), or `<max-retries>` has been reached, 
or `<timeout-ms>` has elapsed, in which case `<timeout-res>` is returned.


```clj
;; stop retrying after 10 millis, OR after 5 retries
(with-max-retries-timeout nil 10 :END 5 some? 
  (println "Hi!"))
Hi!
Hi!
Hi!
Hi!
Hi!
=> ExceptionInfo Retries exhausted! clojure.core/ex-info (core.clj:4739)

```


### Timeout(ing)

`ajenda.expiring` exposes a single macro:

1. *with-timeout* [ms executor-service timeout-result & body]

Very similar to `clojure.core/deref-future`, except that it will also cancel the future upon timeout!
NOTE THAT CANCELLING THE FUTURE DOES NOT NECESSARILY STOP THE THREAD. THE CODE RUNNING ON THAT FUTURE SHOULD BE 
ACTIVELY CHECKING FOR THE INTERRUPT CAUSED BY THE CANCELLING CALL (see the source of `ajenda.retrying/with-retries-timeout` for a good example).

Example:

```clj
(with-timeout 1 nil :DONE
  (apply + (range 100000))) 

=> :DONE

;; It is important to know that the the code above executed on a separate thread,
;; and that the sum was eventually calculated on that thread. We just didn't wait long enough...
```


### Planning

`ajenda.planning` exposes 5 utilities:

1. *periodically*  [delay interval executor & body]

Schedules `<body>` for periodic execution at fixed intervals, with an optional initial `<delay>`.

Example:
```clj
(def stop-printing  ;; you get back a funciton to unschedule 
  (periodically 0 10 nil 
    (println "Hi!")))
    
    Hi!
    Hi!
    Hi!
    Hi!
    Hi!
  (stop-printing)
  => true
```


2. *do-after!* [delay executor & body]

Schedules `<body>` for execution after <delay> milliseconds.

Example:
```clj
(do-after! 1000 nil 
  (prinltn "Hi!"))
  
  ;; nothing happens for 1 second
   
 "Hi!"      
```

3. *do-at!* [date-time executor & body]

Schedules <body> for execution at that particular `<date-time>` string (per *DateTimeFormatter.ISO_DATE_TIME*).
It also accepts LocalDateTime objects for better integration with existing code, or in case the ISO_DATE_TIME format isn't to our liking.

Example:
```clj
(def cancel-sms  
  (do-at! "2016-12-03T10:15:30+01:00[Europe/Paris]" nil 
    (send-sms phone-number)))      
```
 
4. *adhoc-multischedule* [global-timeout executor schedules]

Builds on top of `do-at!`, and accepts a map of schedules (task-id => [date-time task-fn]). 
Supports an optional global timeout, after which any tasks not yet started will never run. 
Returns a vector with 2 functions. First one expects either 1 argument -
the id of the task  you want cancelled, or no-args if you happen to want to cancel them all.
In the absence of a `<global-timeout>` the second item in the vector will be nil, otherwise it will be a function which cancels the `<global-timeout>`.

5. *periodic-multischedule* [global-timeout executor schedules]

Builds on top of `do-at!` (similarly to `adhoc-multischedule`), but also brings `periodically` into the mix.
It accepts  a map of schedules (task-id => [delay interval task-fn]), and it too supports a global timeout.
Returns the same vector of 2 functions as `adhoc-multischedule`.
 
 
## Notes 
 
1. As evidenced by the demo code above, wherever you see a 'pool' (Executor) argument in `ajenda.planning`, you can pass nil and one will be created for you (via `Executors/newSingleThreadScheduledExecutor` or `Executors/newScheduledThreadPool`). 
2. `ajenda.expiring/with-timeout` can also take nil as its 'executor-service' argument, in which case it will create a future.
3.  There are public functions that are suffixed with *. These are actually the functions that do all the work, but they don't always mirror the arguments of the macros - so take caution using those!   


## License

Copyright © 2016 Dimitrios Piliouras

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
