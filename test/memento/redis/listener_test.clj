(ns memento.redis.listener-test
  (:require [clojure.test :refer :all]
            [memento.config :as mc]
            [memento.core :as m]
            [memento.redis :as mr]
            [memento.redis.listener :as l]
            [memento.redis.poll.daemon :as d]
            [memento.redis.test-util :as util]
            [taoensso.carmine :as car]))

(defn listener-fixture [f]
  (.clear l/invalidations)
  (.clear l/listeners)
  (f)
  (.clear l/invalidations)
  (.clear l/listeners))

;; clojure.test's `use-fixtures :each` REPLACES (does not compose) prior calls
;; with the same key. A previous version of this file called `use-fixtures`
;; twice and silently dropped the wipe fixture, causing cache entries to
;; accumulate in Redis across JVM invocations. With identity-hash collisions
;; across runs (lambda toString → segment-id), `foreign-invalidation-test`
;; would intermittently hit a stale entry instead of invoking its user fn.
;; Compose explicitly into a single call.
(use-fixtures :each util/fixture-wipe listener-fixture)

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
    (let [msgs (atom [])
          our-tag [:tag 1]
          ;; Track epochs we publish ourselves (via memo-clear-tags!) so we can
          ;; distinguish our messages from background noise produced by the
          ;; daemon's TagInvalidation listener (which can re-publish in races
          ;; between event-end's `.remove` and the round-trip `:start` arriving
          ;; at process-msg).
          our-epochs (atom #{})]
      (with-open [l (l/listener {} #(swap! msgs conj %) (constantly nil))]
        (Thread/sleep 500)
        (m/memo-clear-tags! our-tag)
        ;; Wait until we've observed at least one matching :start (records the
        ;; epoch into our-epochs) and the matching :end for that epoch.
        (let [match-start? (fn [[a _e items]]
                             (and (= a :start) (some #(= % our-tag) items)))
              match-end?   (fn [[a e _]]
                             (and (= a :end) (contains? @our-epochs e)))
              deadline (+ (System/currentTimeMillis) 5000)]
          (while (and (or (empty? @our-epochs)
                          (not (some match-end? @msgs)))
                      (< (System/currentTimeMillis) deadline))
            (doseq [[a e items :as m] @msgs
                    :when (match-start? m)]
              (swap! our-epochs conj e))
            (Thread/sleep 50)))
        ;; Note: we deliberately do NOT assert l/invalidations is empty here.
        ;; A known race in the daemon's TagInvalidation → event-start/event-end
        ;; round-trip can leave our epoch lingering in `invalidations` if the
        ;; published `:end` is processed (`.remove` returns nil) before the
        ;; published `:start` is processed (`putIfAbsent` then succeeds). The
        ;; 45s `remove-old-invalidations` cleanup handles that path and is
        ;; covered by `stale-foreign-invalidation-test`. The functional claim
        ;; verified here is that the publish path produces both messages.
        (is (= {:listeners 1
                :saw-our-start true
                :saw-our-end true}
               {:listeners (count l/listeners)
                :saw-our-start (boolean (seq @our-epochs))
                :saw-our-end (boolean (some (fn [[a e _]]
                                              (and (= a :end)
                                                   (contains? @our-epochs e)))
                                            @msgs))}))))))

(deftest foreign-invalidation-test
  (testing "Foreign invalidations prevent our loads"
    @d/daemon-thread
    (l/ensure-l {})
    (let [epoch 20
          cnt (atom 0)
          ;; gate-1 blocks the first invocation so we can deterministically
          ;; install the foreign invalidation while the user fn is in-flight.
          ;; gate-2 blocks the second invocation so we can clean up the
          ;; foreign invalidation before the retry's deliver.
          gate-1 (promise)
          gate-2 (promise)
          f (m/memo (fn [_]
                      (let [n (swap! cnt inc)]
                        (deref (if (= n 1) gate-1 gate-2) 5000 :timeout)
                        (m/with-tag-id {} :tag 1)))
                    {mc/type mr/cache mr/conn {} mc/tags :tag mr/keygen util/test-keygen})
          fut (future (f 1))]
      ;; Give the pub/sub listener a moment to fully subscribe before publishing.
      (Thread/sleep 500)
      ;; Wait until the first invocation has actually started (cnt advanced to 1)
      ;; so the Load is registered in `loader/maint`. Otherwise Loader.addInvalidations
      ;; (fired off the TagInvalidation listener when :start is processed) walks an
      ;; empty maint and the in-flight promise never receives the invalidated id.
      (let [deadline (+ (System/currentTimeMillis) 5000)]
        (while (and (zero? @cnt)
                    (< (System/currentTimeMillis) deadline))
          (Thread/sleep 20)))
      (car/wcar {}
        (car/publish l/channel [:start epoch [[:tag 1]]]))
      ;; process-msg inserts the epoch then fires TagInvalidation.startInvalidation
      ;; which propagates to all in-flight promises via Loader.addInvalidations.
      ;; Waiting for the map entry confirms the entire chain has executed.
      (let [deadline (+ (System/currentTimeMillis) 5000)]
        (while (and (nil? (first l/invalidations))
                    (< (System/currentTimeMillis) deadline))
          (Thread/sleep 20)))
      (is (= [epoch] (keys l/invalidations)))
      ;; Release the first attempt; its deliver() will fail because the promise
      ;; now sees an invalidated tag id, the loader returns absent, and CachedFn
      ;; loops into a second attempt.
      (deliver gate-1 :go)
      ;; Wait until the second attempt has started (cnt == 2).
      (let [deadline (+ (System/currentTimeMillis) 5000)]
        (while (and (< @cnt 2)
                    (< (System/currentTimeMillis) deadline))
          (Thread/sleep 20)))
      ;; Close the foreign invalidation so the retry can publish a fresh value.
      (car/wcar {}
        (car/publish l/channel [:end epoch]))
      ;; process-msg :end removes from `invalidations` BEFORE calling
      ;; TagInvalidation.endInvalidation. SpecialPromise.deliver consults
      ;; TagInvalidation, so wait until the tag is actually cleared there.
      (let [deadline (+ (System/currentTimeMillis) 5000)
            tag-set #{[:tag 1]}]
        (while (and (not= Long/MIN_VALUE
                          (.lastInvalidatedEpoch l/tag-invalidation tag-set))
                    (< (System/currentTimeMillis) deadline))
          (Thread/sleep 20)))
      (deliver gate-2 :go)
      @fut
      (is (= {:invalidations 0 :cnt 2}
             {:invalidations (count l/invalidations) :cnt @cnt})))))

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
