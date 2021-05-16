(ns memento.redis.sec-index
  (:require [clojure.java.io :as io]
            [taoensso.carmine :as car])
  (:import (java.util.concurrent ConcurrentHashMap)))

(def add-to-index-script "redis.call('sadd', _:id-key, _:k)\nredis.call('sadd', _:indexes, _:id-key)")
(def invalidate-script (slurp (io/resource "memento/redis/sec-index-invalidate.lua")))

(def ^ConcurrentHashMap all-indexes
  "Stores pairs of connection + indexes key seen"
  (ConcurrentHashMap. (int 4) (float 0.75) (int 8)))

(defn add-to-index
  "Add a key to sec index. indexes-key is the main key that keeps all the
  secondary index keys, id-key is secondary index set. It assumes that it's done inside carw"
  [indexes-key id-key k]
  (car/lua add-to-index-script {:k k :id-key id-key :indexes indexes-key} {}))

(defn invalidate-by-index
  "Remove (invalidate) secondary index and all the keys therein"
  [conn indexes-key id-key]
  (car/wcar conn (car/lua invalidate-script {:id-key id-key :indexes indexes-key} {})))

;; SECONDARY INDEX MAINTENANCE
(def clean-up-script (slurp (io/resource "memento/redis/clean-up-expired.lua")))

(defn removed-expired-keys
  "Looks at 20 keys in master index then look at up to 20 of their entries. If more than 20%
  were expired, repeat the process."
  [conn set-key]
  (let [[total expired]
        (->> (car/wcar conn (car/srandmember set-key 20))
             (map #(car/wcar conn (car/lua clean-up-script {:indexes set-key :k %} {:n 20})))
             (reduce #(map + %1 %2) [0 0]))]
    (when (and (pos-int? expired) (< (/ total expired) 5))
      (recur conn set-key))))

(defn maintenance-step
  "Perform maintenance multithreaded, one future per each connection after the first."
  [^ConcurrentHashMap all-indexes]
  ;; do maintenance of one conn in this thread, the rest in futures
  (loop [[[conn indexes-key] & more] (into [] (enumeration-seq (.keys all-indexes)))
         futures (list)]
    (if more
      (recur more (conj futures (future (removed-expired-keys conn indexes-key))))
      ;; process last conn in this thread and await others
      (when conn
        (removed-expired-keys conn indexes-key)
        (doseq [f futures] (deref f))))))
