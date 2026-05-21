(ns memento.redis.listener-test
  (:require [clojure.test :refer :all]
            [memento.config :as mc]
            [memento.core :as m]
            [memento.redis :as mr]
            [memento.redis.listener :as l]
            [memento.redis.poll.daemon :as d]
            [memento.redis.test-util :as util]
            [taoensso.carmine :as car]))

(use-fixtures :each util/fixture-wipe)

(defn listener-fixture [f]
  (.clear l/invalidations)
  (.clear l/listeners)
  (f)
  (.clear l/invalidations)
  (.clear l/listeners))

(use-fixtures :each listener-fixture)

(deftest listener-subscription-test
  (testing "messages work"
    (let [epoch 10
          msgs (atom [])
          _ (l/ensure-l {})]
      (with-open [l (l/listener {} #(swap! msgs conj %)
                                 (constantly nil))]
        (Thread/sleep 500)
        (l/event-start [[:a 1]] epoch)
        (while (empty? @msgs)
          (Thread/sleep 100))
        (is (= epoch (key (first l/invalidations))))
        (is (= 1 (count l/listeners)))
        (is (= [[:start epoch [[:a 1]]]] @msgs))
        (l/event-end epoch)
        (while (< (count @msgs) 2)
          (Thread/sleep 100))
        (is (= 0 (count l/invalidations)))
        (is (= 1 (count l/listeners)))
        (is (= [[:start epoch [[:a 1]]]
                [:end epoch]]
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
    (let [epoch 20
          cnt (atom 0)
          f (m/memo (fn [x] (Thread/sleep 1000) (swap! cnt inc) (m/with-tag-id {} :tag 1))
                    {mc/type mr/cache mr/conn {} mc/tags :tag mr/keygen util/test-keygen})
          fut (future (f 1))]
      (car/wcar {}
        (car/publish l/channel [:start epoch [[:tag 1]]]))
      (while (nil? (first l/invalidations))
        (Thread/sleep 100))
      (is (= [epoch] (keys l/invalidations)))
      (car/wcar {}
        (car/publish l/channel [:end epoch]))
      @fut
      (is (= 0 (count l/invalidations)))
      (is (= 2 @cnt)))))

(deftest stale-foreign-invalidation-test
  (testing "Foreign invalidations are only released after they become stale"
    (let [epoch 30
          items [[:stale-tag 1]]]
      (l/process-msg [:start epoch items])
      (is (= epoch (.lastInvalidatedEpoch l/tag-invalidation (set items))))
      (l/remove-old-invalidations 45000)
      (is (= epoch (.lastInvalidatedEpoch l/tag-invalidation (set items))))
      (.put l/invalidations epoch [0 items])
      (l/remove-old-invalidations 1)
      (is (= Long/MIN_VALUE (.lastInvalidatedEpoch l/tag-invalidation (set items))))
      (is (= 0 (count l/invalidations))))))
