(ns memento.redis.test-util
  (:require [taoensso.carmine :as car]
            [memento.redis.cache]
            [memento.redis.poll.daemon :as daemon]
            [memento.redis.keys :as keys]
            [memento.redis.loader :as loader]
            [memento.redis.util :as util]
            [memento.core :as core])
  (:import (memento.base Segment)
           (memento.redis.cache RedisCache)))

(defn sprn [& args]
  (locking println
    (apply println args)))

(do @daemon/daemon-thread)

(def prefix "MMR-TEST")

(def test-keygen
  (keys/default-keys-generator
    prefix
    "MMRS-TEST"
    :stringify))

(defn test-key
  "An entry key in test keyspace."
  [k]
  (keys/entry-key test-keygen "" (Segment. identity identity "" {}) k))

(defn add-entry
  "Add a full test generator keyed entry"
  [k v]
  (car/wcar {} (car/set (test-key k) v)))

(defn get-entry
  "Retrieve a full test generator keyed entry"
  [k]
  (car/wcar {} (car/get (test-key k))))

(defn get-entry*
  "Retrieve the arg list value from Redis"
  [f args]
  (let [cache (core/active-cache f)
        segment (.segment ^Segment f)
        k ((-> cache :fns :key-fn) segment args)]
    (when (instance? RedisCache cache)
      (car/wcar ((-> cache :fns :conn)) (car/get k)))))

(defmacro with-kv
  "Insert and delete the specified key values, prefixing the string. This is raw keys values"
  [prefix m & body]
  `(try
     (car/wcar {}
       (apply car/mset
              (interleave
                (map (partial str ~prefix) (keys ~m))
                (vals ~m))))
     ~@body
     (finally
       (let [ks# (car/wcar {} (car/keys (str ~prefix "*")))]
         (when-not (empty? ks#)
           (car/wcar {} (apply car/del ks#)))))))

(defn wipe []
  (util/nuke-keyspace {} test-keygen)
  (.clear loader/maint))

(defn fixture-wipe [f]
  (wipe)
  (try
    (f)
    (finally (wipe))))
