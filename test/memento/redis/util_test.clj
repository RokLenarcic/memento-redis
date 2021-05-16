(ns memento.redis.util-test
  (:require [clojure.test :refer :all]
            [memento.redis.util :as u]
            [memento.redis.test-util :as testu]))

(use-fixtures :each testu/fixture-wipe)

(deftest kv-by-pattern-test
  (testu/with-kv "TEST:"
    (zipmap (range 5) (range 5))
    (is (= {"0" "0"
            "1" "1"
            "2" "2"
            "3" "3"
            "4" "4"}
           (u/kv-by-pattern {} "TEST:*" #(subs % 5))))))

(deftest del-keys-by-pattern
  (testu/with-kv "TEST:"
    (zipmap (range 5) (range 5))
    (u/del-keys-by-pattern {} "TEST:*")
    (is (= {} (u/kv-by-pattern {} "TEST:*" #(subs % 5))))))
