(ns memento.redis.poll.daemon
  "Daemon thread namespace.

  On every thread wakeup we try to fetch results that are being awaited from redis.

  There are longer, rarer jobs to do:
  - maintain expiry of load markers (generally about each second)
  - secondary indexes cleanup (generally every 4 seconds)"
  (:require [memento.redis.loader :as loader]
            [memento.redis.sec-index :as sec-index]
            [taoensso.timbre :as log])
  (:import (memento.redis.poll Loads)))

(def sleep-interval
  "Time in ms between thread wakeups."
  (Long/parseLong (System/getProperty "memento.redis.daemon_interval" "40")))

(def load-markers-interval 1000)
(def sec-index-interval
  "Time in ms to perform secondary index cleanups (removes entries pointing to
  non-existent keys)"
  (Long/parseLong (System/getProperty "memento.redis.sec_index_interval" "4071")))

(defn perform-step
  "Perform maintenance steps, given a map of last time each type was done."
  [action-timestamps]
  (let [current (System/currentTimeMillis)
        load-markers? (< load-markers-interval (- current (:load-markers action-timestamps 0)))
        sec-index? (< sec-index-interval (- current (:sec-index action-timestamps 0)))
        new-timestamps (cond-> action-timestamps
                         load-markers? (assoc :load-markers current :sec-index current))]
    (try
      (loader/maintenance-step Loads/maint load-markers?)
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
                                    (Thread/sleep ^long sleep-interval)
                                    (recur (perform-step action-timestamps))))
                      "Memento Daemon")
             (.setDaemon true)
             (.start))))
