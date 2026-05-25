(ns memento.redis.sec-index-test
  (:require [clojure.test :refer :all]
            [memento.config :as mc]
            [memento.core :as m]
            [memento.redis.keys :as keys]
            [memento.redis.loader :as loader]
            [memento.redis.poll.daemon :as daemon]
            [memento.redis.sec-index :as sec-idx]
            [memento.redis.test-util :as util]
            [memento.redis.cache-test :as cache-test]
            [taoensso.carmine :as car])
  (:import (java.util.concurrent ConcurrentHashMap)
           (memento.base Segment)
           (memento.redis.poll Loader)))

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
    (with-redefs [daemon/sec-index-interval 100000000000000]
      (car/wcar {} (add-to-index indexes-key id-key k))
      (is (= nil (car/wcar {} (car/get k))))
      (is (= [id-key] (car/wcar {} (car/smembers indexes-key))))
      (is (= [k] (car/wcar {} (car/smembers id-key)))))))

(deftest invalidate-by-index-test
  (let [_ (util/add-entry :xx 11)
        k (util/test-key :xx)
        id-key (keys/sec-index-id-key util/test-keygen "A" "B")
        indexes-key (keys/sec-indexes-key util/test-keygen)
        epoch-key (keys/epoch-key util/test-keygen "A")
        tag-epochs-key (keys/tag-epochs-key util/test-keygen "A")]
    (with-redefs [daemon/sec-index-interval 100000000000000]
      (car/wcar {} (add-to-index indexes-key id-key k))
      (is (= 1 (car/wcar {} (car/exists k))))
      (is (= 1 (car/wcar {} (car/exists indexes-key))))
      (is (= ['("MMRS-TEST" "A" "B")] (car/wcar {} (car/smembers indexes-key))))
      (is (= 1 (car/wcar {} (car/exists id-key))))
      (is (= 1 (sec-idx/invalidate-by-index {} indexes-key epoch-key tag-epochs-key [id-key])))
      (is (= 0 (car/wcar {} (car/exists k))))
      (is (= 0 (car/wcar {} (car/exists indexes-key))))
      (is (= [] (car/wcar {} (car/smembers indexes-key))))
      (is (= 0 (car/wcar {} (car/exists id-key))))
      (is (= "1" (car/wcar {} (car/hget tag-epochs-key id-key)))))))

(deftest invalidate-by-index-epoch-test
  (let [indexes-key (keys/sec-indexes-key util/test-keygen)
        epoch-key (keys/epoch-key util/test-keygen "A")
        tag-epochs-key (keys/tag-epochs-key util/test-keygen "A")
        id-key-1 (keys/sec-index-id-key util/test-keygen "A" "B")
        id-key-2 (keys/sec-index-id-key util/test-keygen "A" "C")]
    (is (= 1 (sec-idx/invalidate-by-index {} indexes-key epoch-key tag-epochs-key [id-key-1 id-key-2])))
    (is (= {id-key-1 "1" id-key-2 "1"}
           (apply hash-map (car/wcar {} (car/hgetall tag-epochs-key)))))
    (is (= 2 (sec-idx/invalidate-by-index {} indexes-key epoch-key tag-epochs-key [id-key-1])))
    (is (= {id-key-1 "2" id-key-2 "1"}
           (apply hash-map (car/wcar {} (car/hgetall tag-epochs-key)))))))

(deftest removed-expired-keys-test
  (let [n 1000
        l (ConcurrentHashMap.)
        ;; Use a generous TTL so the populate step doesn't race with expiry;
        ;; we'll simulate expiry deterministically by DEL'ing the value keys.
        loader (Loader. "" util/test-keygen nil nil [60 :s] [60 :s] nil false l loader/support)
        segment (Segment. #(-> %
                               (m/with-tag-id :y %)
                               (m/with-tag-id :t (str "W" %)))
                          #(str "T" %)
                          "IDDD"
                          {})
        entry-key #(util/test-key (str "T" %))
        index-key1 #(keys/sec-index-id-key util/test-keygen "" [:y %])
        index-key2 #(keys/sec-index-id-key util/test-keygen "" [:t (str "W" %)])
        indexes-key (keys/sec-indexes-key util/test-keygen)]
    (with-redefs [daemon/sec-index-interval 100000000000000]
      (doseq [x (range n)]
        (.get loader {} segment (list x) (entry-key x)))
      (is (= {:indexes (into #{} (concat (map index-key1 (range n))
                                         (map index-key2 (range n))))
              :index-key1-members (set (mapcat #(car/wcar {} (car/smembers (index-key1 %))) (range n)))
              :index-key2-members (set (mapcat #(car/wcar {} (car/smembers (index-key2 %))) (range n)))}
             {:indexes (into #{} (car/wcar {} (car/smembers indexes-key)))
              :index-key1-members (set (map entry-key (range n)))
              :index-key2-members (set (map entry-key (range n)))}))
      ;; Simulate expiry of all value keys; sec-index sets still hold the
      ;; (now-dangling) references. removed-expired-keys samples 20 per call
      ;; and recurses only when >20% expired, so loop until the master is empty.
      (doseq [x (range n)]
        (car/wcar {} (car/del (entry-key x))))
      (loop [guard 1000]
        (when (and (pos? guard)
                   (pos? (car/wcar {} (car/scard indexes-key))))
          (sec-idx/removed-expired-keys {} indexes-key)
          (recur (dec guard))))
      (is (= #{} (into #{} (car/wcar {} (car/smembers indexes-key))))))))

(defn test-fn [x] x)

(m/memo test-fn (assoc cache-test/inf mc/tags #{:test-tag}))

(deftest put-test
  (let [f (m/memo inc (assoc cache-test/inf mc/tags #{:test-tag}))
        id-key (keys/sec-index-id-key util/test-keygen "" [:test-tag 55])
        indexes-key (keys/sec-indexes-key util/test-keygen)]
    (with-redefs [daemon/sec-index-interval 100000000000000]
      (m/memo-add! f {[1] (m/with-tag-id 100 :test-tag 55)})
      (is (= {[1] 100} (util/unwrap-as-map (m/as-map f))))
      (is (= 100 (util/get-entry* f [1])))
      (is (= ['("MMRS-TEST" "" [:test-tag 55])] (car/wcar {} (car/smembers indexes-key))))
      (is (= 1 (car/wcar {} (car/exists id-key)))))))
