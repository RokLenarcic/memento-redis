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
