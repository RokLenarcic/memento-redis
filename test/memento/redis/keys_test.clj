(ns memento.redis.keys-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [memento.redis.keys :as keys]
            [memento.redis.test-util :as util]
            [taoensso.carmine :as car]))

(use-fixtures :each util/fixture-wipe)

(defn create-v [prefix m]
  (car/wcar {}
    (apply car/mset (reduce-kv #(conj %1 (apply list prefix %2) %3) [] m))))

(deftest wildcard-test
  (testing "Wildcarding vectors works"
    (create-v util/prefix {[1 2 3] 4 ["AA[[AA" "Y"] 11 [nil 1] 1 [nil] 11 ["AAA" "BBB"] 4})
    (is (= [[util/prefix 1 2 3]] (car/wcar {} (car/keys (keys/wildcard (list util/prefix 1 2))))))
    (is (= [[util/prefix 1 2 3]] (car/wcar {} (car/keys (keys/wildcard (list util/prefix 1 2 3))))))
    (is (= [[util/prefix 1 2 3]] (car/wcar {} (car/keys (keys/wildcard (list util/prefix 1))))))
    (is (= [] (car/wcar {} (car/keys (keys/wildcard (list util/prefix 2))))))
    (is (= [] (car/wcar {} (car/keys (keys/wildcard (list util/prefix "1 2"))))))
    (is (= #{[util/prefix nil 1]
             [util/prefix nil]} (set (car/wcar {} (car/keys (keys/wildcard (list util/prefix nil)))))))
    (is (= [[util/prefix nil 1]] (car/wcar {} (car/keys (keys/wildcard (list util/prefix nil 1))))))
    (is (= [] (car/wcar {} (car/keys (keys/wildcard (list util/prefix "A*"))))))
    (is (= [] (car/wcar {} (car/keys (keys/wildcard (list util/prefix "AA"))))))
    (is (= [[util/prefix "AA[[AA" "Y"]] (car/wcar {} (car/keys (keys/wildcard (list util/prefix "AA[[AA"))))))))

(deftest keyscan-test
  (testing "Scanning function works"
    (util/with-kv "TEST:"
      (zipmap (range 1500) (range 1500))
      (car/wcar {}
        (let [ks (into [] (keys/by-pattern "TEST:*"))]
          (is (= 2 (count ks)))
          (is (= 1000 (count (first ks))))
          (is (= 500 (count (second ks))))
          (is (every? #(str/starts-with? % "TEST:") (first ks)))
          (is (every? #(str/starts-with? % "TEST:") (second ks))))))))

(deftest epoch-key-test
  (let [kg util/test-keygen
        cache-name ""
        segment (memento.base.Segment. identity identity "e" {})
        entry-key (keys/entry-key kg cache-name segment [1])
        epoch-key (keys/epoch-key kg cache-name)
        tag-epochs-key (keys/tag-epochs-key kg cache-name)]
    (is (= [util/prefix cache-name "e"] epoch-key))
    (is (= [util/prefix cache-name "t"] tag-epochs-key))
    (is (= [util/prefix cache-name "e" [1]] entry-key))
    (is (not= epoch-key entry-key))
    (is (keys/entry-key? kg cache-name entry-key))
    (is (not (keys/entry-key? kg cache-name epoch-key)))
    (is (not (keys/entry-key? kg cache-name tag-epochs-key)))
    (car/wcar {}
      (car/set epoch-key 1)
      (car/set tag-epochs-key 1)
      (car/set entry-key 1))
    (is (= #{epoch-key tag-epochs-key entry-key}
           (set (car/wcar {} (car/keys (keys/cache-wildcard-key kg cache-name))))))))

(deftest wildcard-exact-length-test
  (testing "wildcard-exact-length matches only one-deeper descendants"
    (create-v util/prefix {[1 2] 7
                           [1 2 3] 4
                           [1 2 3 4] 9
                           ["A" "B"] 1})
    (is (= [[util/prefix 1 2 3]]
           (car/wcar {} (car/keys (keys/wildcard-exact-length (list util/prefix 1 2))))))
    (is (= [[util/prefix 1 2]]
           (car/wcar {} (car/keys (keys/wildcard-exact-length (list util/prefix 1)))))))
  (testing "throws when target count byte is a glob metachar"
    (are [target-count] (thrown? clojure.lang.ExceptionInfo
                                 (keys/wildcard-exact-length
                                   (apply list (repeat (dec target-count) :x))))
      42 63 91 92)))

(deftest segment-wildcard-no-epoch-collision-test
  (testing "segment-wildcard-key with segment id \"e\" does not match epoch-key"
    (let [kg util/test-keygen
          cname "C"
          seg-e (memento.base.Segment. identity identity "e" {})
          seg-t (memento.base.Segment. identity identity "t" {})
          epoch-k (keys/epoch-key kg cname)
          tag-epochs-k (keys/tag-epochs-key kg cname)
          entry-k-e (keys/entry-key kg cname seg-e [1])
          entry-k-t (keys/entry-key kg cname seg-t [2])]
      (car/wcar {}
        (car/set epoch-k 1)
        (car/set tag-epochs-k 1)
        (car/set entry-k-e 1)
        (car/set entry-k-t 1))
      (is (= [entry-k-e]
             (car/wcar {} (car/keys (keys/segment-wildcard-key kg cname seg-e)))))
      (is (= [entry-k-t]
             (car/wcar {} (car/keys (keys/segment-wildcard-key kg cname seg-t)))))
      (is (not (keys/segment-key? kg cname seg-e epoch-k)))
      (is (not (keys/segment-key? kg cname seg-t tag-epochs-k)))
      (is (keys/segment-key? kg cname seg-e entry-k-e))
      (is (keys/segment-key? kg cname seg-t entry-k-t)))))
