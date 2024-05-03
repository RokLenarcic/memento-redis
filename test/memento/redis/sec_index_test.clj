(ns memento.redis.sec-index-test
  (:require [clojure.test :refer :all]
            [memento.config :as mc]
            [memento.core :as m]
            [memento.redis.keys :as keys]
            [memento.redis.loader :as loader]
            [memento.redis.sec-index :as sec-idx]
            [memento.redis.test-util :as util]
            [memento.redis.cache-test :as cache-test]
            [taoensso.carmine :as car])
  (:import (java.util.concurrent ConcurrentHashMap)))

(use-fixtures :each util/fixture-wipe)

(def add-to-index-script "redis.call('sadd', _:id-key, _:k)\nredis.call('sadd', _:indexes, _:id-key)")

(defn add-to-index
  "Add a key to sec index. indexes-key is the main key that keeps all the
  secondary index keys, id-key is secondary index set. It assumes that it's done inside carw"
  [indexes-key id-key k]
  (car/lua add-to-index-script {:k k :id-key id-key :indexes indexes-key} {}))

(deftest add-to-index-test
  (let [k "MY_KEY"
        id-key (keys/sec-index-id-key util/test-keygen "A" "B")
        indexes-key (keys/sec-indexes-key util/test-keygen)]
    (car/wcar {} (add-to-index indexes-key id-key k))
    (is (= nil (car/wcar {} (car/get k))))
    (is (= [id-key] (car/wcar {} (car/smembers indexes-key))))
    (is (= [k] (car/wcar {} (car/smembers id-key))))))

(deftest invalidate-by-index-test
  (let [_ (util/add-entry :xx 11)
        k (util/test-key :xx)
        id-key (keys/sec-index-id-key util/test-keygen "A" "B")
        indexes-key (keys/sec-indexes-key util/test-keygen)]
    (car/wcar {} (add-to-index indexes-key id-key k))
    (is (= 1 (car/wcar {} (car/exists k))))
    (is (= 1 (car/wcar {} (car/exists indexes-key))))
    (is (= ['("MMRS-TEST" "A" "B")] (car/wcar {} (car/smembers indexes-key))))
    (is (= 1 (car/wcar {} (car/exists id-key))))
    (sec-idx/invalidate-by-index {} indexes-key [id-key])
    (is (= 0 (car/wcar {} (car/exists k))))
    (is (= 0 (car/wcar {} (car/exists indexes-key))))
    (is (= [] (car/wcar {} (car/smembers indexes-key))))
    (is (= 0 (car/wcar {} (car/exists id-key))))))

(deftest removed-expired-keys-test
  (let [l (ConcurrentHashMap.)
        loader (loader/->PollingLoader l util/test-keygen "" 1 1)
        entry-key #(util/test-key (str "T" %))
        index-key1 #(keys/sec-index-id-key util/test-keygen "" [:y %])
        index-key2 #(keys/sec-index-id-key util/test-keygen "" [:t (str "W" %)])]
    (doseq [x (range 1000)]
      (loader/start loader {} (entry-key x))
      (loader/complete loader
                       {}
                       (entry-key x)
                       (-> x
                           (m/with-tag-id :y x)
                           (m/with-tag-id :t (str "W" x)))))
    (is (= (into #{} (concat
                       (map index-key1 (range 1000))
                       (map index-key2 (range 1000))))
           (into #{} (car/wcar {} (car/smembers (keys/sec-indexes-key util/test-keygen))))))
    (is (= (map entry-key (range 1000)) (mapcat #(car/wcar {} (car/smembers (index-key1 %))) (range 1000))))
    (is (= (map entry-key (range 1000)) (mapcat #(car/wcar {} (car/smembers (index-key2 %))) (range 1000))))
    (sec-idx/removed-expired-keys {} (keys/sec-indexes-key util/test-keygen))
    (sec-idx/removed-expired-keys {} (keys/sec-indexes-key util/test-keygen))
    (is (= #{} (into #{} (car/wcar {} (car/smembers (keys/sec-indexes-key util/test-keygen))))))))

(defn test-fn [x] x)

(m/memo test-fn (assoc cache-test/inf mc/tags #{:test-tag}))

(deftest put-test
  (let [f (m/memo inc (assoc cache-test/inf mc/tags #{:test-tag}))
        id-key (keys/sec-index-id-key util/test-keygen "" [:test-tag 55])
        indexes-key (keys/sec-indexes-key util/test-keygen)]
    (m/memo-add! f {[1] (m/with-tag-id 100 :test-tag 55)})
    (is (= {[1] 100} (m/as-map f)))
    (is (= 100 (util/get-entry* f [1])))
    (is (= ['("MMRS-TEST" "" [:test-tag 55])] (car/wcar {} (car/smembers indexes-key))))
    (is (= 1 (car/wcar {} (car/exists id-key))))))