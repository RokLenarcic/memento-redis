(ns memento.redis.poll.daemon
  "Daemon thread namespace.

  On every thread wakeup we try to fetch results that are being awaited from redis.

  There are longer, rarer jobs to do:
  - maintain expiry of load markers (generally about each second)
  - secondary indexes cleanup (generally every 5 seconds)"
  (:require [memento.redis.loader :as loader]
            [memento.redis.sec-index :as sec-index]
            [taoensso.timbre :as log]))

(def sleep-interval
  "Time in ms between thread wakeups."
  (Integer/parseInt (System/getProperty "memento.redis.daemon_interval" "50")))

(def load-markers-interval 1000)
(def sec-index-interval 5071)

(defn perform-step
  "Perform maintenance steps, given a map of last time each type was done."
  [action-timestamps]
  (let [current (System/currentTimeMillis)
        load-markers? (< load-markers-interval (- current (:load-markers action-timestamps 0)))
        sec-index? (< sec-index-interval (- current (:sec-index action-timestamps 0)))
        new-timestamps (cond-> action-timestamps
                         load-markers? (assoc :load-markers current :sec-index current))]
    (try
      (loader/maintenance-step loader/maint load-markers?)
      (when sec-index?
        (sec-index/maintenance-step sec-index/all-indexes))
      new-timestamps
      (catch Exception e
        (log/warn e "Error running maintenance step")
        new-timestamps))))

(defonce daemon-thread
         (delay
           (doto
             (Thread. ^Runnable (fn []
                                  (loop [action-timestamps {}]
                                    (Thread/sleep sleep-interval)
                                    (recur (perform-step action-timestamps))))
                      "Memento Daemon")
             (.setDaemon true)
             (.start))))
