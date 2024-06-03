(ns memento.redis.listener-test
  (:require [clojure.test :refer :all]
            [memento.config :as mc]
            [memento.core :as m]
            [memento.redis :as mr]
            [memento.redis.listener :as l]
            [memento.redis.poll.daemon :as d]
            [memento.redis.test-util :as util]
            [taoensso.carmine :as car])
  (:import (memento.base LockoutTag)))

(use-fixtures :each util/fixture-wipe)

(deftest listener-subscription-test
  (testing "messages work"
    (let [tag (LockoutTag.)
          msgs (atom [])
          _ (l/ensure-l {})]
      (with-open [l (l/listener {} #(swap! msgs conj %)
                                (constantly nil))]
        (Thread/sleep 500)
        (l/event-start tag [[:a 1]])
        (while (empty? @msgs)
          (Thread/sleep 100))
        (is (= (.getId tag) (key (first l/invalidations))))
        (is (= 1 (count l/listeners)))
        (is (= [[:start (.getId tag) [[:a 1]]]] @msgs))
        (l/event-end tag)
        (is (= 0 (count l/invalidations)))
        (is (= 1 (count l/listeners)))
        (is (= [[:start (.getId tag) [[:a 1]]]
                [:end (.getId tag)]]
               @msgs))))))

(deftest our-invalidation-test
  (testing "Our invalidations message"
    @d/daemon-thread
    (l/ensure-l {})
    (let [msgs (atom [])]
      (with-open [l (l/listener {} #(swap! msgs conj %) (constantly nil))]
        (Thread/sleep 500)
        (m/memo-clear-tags! [:tag 1])
        (while (empty? @msgs)
            (Thread/sleep 100))
        (is (= 0 (count l/invalidations)))
        (is (= 1 (count l/listeners)))
        (is (= [:start :end] (mapv first @msgs)))))))

(deftest foreign-invalidation-test
  (testing "Foreign invalidations prevent our loads"
    @d/daemon-thread
    (l/ensure-l {})
    (let [lockout (LockoutTag.)
          cnt (atom 0)
          f (m/memo (fn [x] (Thread/sleep 1000) (swap! cnt inc) (m/with-tag-id {} :tag 1))
                    {mc/type mr/cache mr/conn {} mc/tags :tag mr/keygen util/test-keygen})
          fut (future (f 1))]
      (car/wcar {}
        (car/publish l/channel [:start (.getId lockout) [[:tag 1]]]))
      (while (nil? (first l/invalidations))
        (Thread/sleep 100))
      (is (= [(.getId lockout)] (keys l/invalidations)))
      (car/wcar {}
        (car/publish l/channel [:end (.getId lockout)]))
      @fut
      (is (= 0 (count l/invalidations)))
      (is (= 2 @cnt)))))
