(ns memento.redis.cache-test
  (:require [clojure.test :refer :all]
            [memento.base :as b]
            [memento.core :refer :all]
            [memento.config :as mc]
            [memento.redis.cache :as cache]
            [memento.redis :as mr]
            [memento.redis.keys :as keys]
            [memento.redis.sec-index :as sec-index]
            [memento.redis.test-util :as util]
            [taoensso.carmine :as car])
  (:import (clojure.lang ExceptionInfo)
           (memento.base Segment)))

(use-fixtures :each util/fixture-wipe)

(def test-conn {:a 1})

(def inf {mc/type mr/cache mr/conn {} mr/keygen util/test-keygen mc/ttl [60 :s]})

(deftest conf-conn-test
  (testing "Connection configuration parsing"
    (are [x]
      (= {:a 1} ((cache/conf-conn {mr/conn x})))
      {:a 1}
      (atom {:a 1})
      (fn [] {:a 1})
      #'test-conn))
  (testing "Throws exception on wrong parameters:"
    (is (thrown-with-msg? ExceptionInfo #"Missing configuration" (cache/conf-conn {})))
    (is (thrown-with-msg? ExceptionInfo #"Wrong configuration key" (cache/conf-conn {mr/conn 1})))))

(deftest conf-cache-name-test
  (is (= "" (cache/conf-cache-name {})))
  (is (= ::x (cache/conf-cache-name {mr/name ::x}))))

(deftest conf-keygen-test
  (is (satisfies? keys/KeysGenerator (cache/conf-keygen {})))
  (let [kg (reify keys/KeysGenerator)]
    (is (= kg (cache/conf-keygen {mr/keygen kg}))))
  (is (thrown-with-msg? ExceptionInfo #"Wrong configuration key" (cache/conf-keygen {mr/keygen 1}))))

(deftest conf-key-fn-test
  (are [conf segment result]
    (is (= result ((cache/conf-key-fn (cache/conf-keygen {}) conf) segment [10])))
    {} (Segment. identity identity :id {}) (list "M^" "" :id [10])
    {mr/name "cache-name"} (Segment. identity identity :id {}) (list "M^" "cache-name" :id [10])
    {mc/key-fn (comp inc first)} (Segment. identity identity :id {}) (list "M^" "" :id 11)
    {mc/key-fn inc} (Segment. identity (comp #(* 2 %) first) :id {}) (list "M^" "" :id 21)))

(deftest as-map-test
  (let [memod (memo inc inf)]
    (doseq [x (range 5)] (memod x))
    (let [cache (active-cache memod)
          keygen (-> cache :fns :keygen)
          cname (:cname cache)]
      (car/wcar {}
        (car/set (keys/epoch-key keygen cname) 1)
        (car/hset (keys/tag-epochs-key keygen cname) "tag" 1)))
    (is (= {[(str inc) [0]] 1
            [(str inc) [1]] 2
            [(str inc) [2]] 3
            [(str inc) [3]] 4
            [(str inc) [4]] 5}
           (util/unwrap-as-map (b/as-map (active-cache memod)))))
    (is (= {[0] 1
            [1] 2
            [2] 3
            [3] 4
            [4] 5}
           (util/unwrap-as-map (as-map memod))))))

(deftest cache-clear-removes-epoch-metadata-test
  (let [c (create inf)
        keygen (-> c :fns :keygen)
        cname (:cname c)]
    (car/wcar {}
      (car/set (keys/epoch-key keygen cname) 1)
      (car/hset (keys/tag-epochs-key keygen cname) "tag" 1))
    (memo-clear-cache! c)
    (is (= 0 (car/wcar {} (car/exists (keys/epoch-key keygen cname)))))
    (is (= 0 (car/wcar {} (car/exists (keys/tag-epochs-key keygen cname)))))))

(defn- check-core-features
  [factory]
  (let [mine (factory identity)
        them (memoize identity)]
    (testing "That the memo function works the same as core.memoize"
      (are [x y] (= x y)
                 (mine 42) (them 42)
                 (mine ()) (them ())
                 (mine []) (them [])
                 (mine #{}) (them #{})
                 (mine {}) (them {})
                 (mine nil) (them nil)))
    (testing "That the memo function has a proper cache"
      (is (memoized? mine))
      (is (not (memoized? them)))
      (is (= 42 (mine 42)))
      (is (= nil (meta (mine {}))))
      (is (not (empty? (into {} (as-map mine)))))
      (is (memo-clear! mine))
      (is (empty? (into {} (as-map mine))))))
  (testing "That the cache retries in case of exceptions"
    (let [access-count (atom 0)
          f (factory
              (fn []
                (swap! access-count inc)
                (throw (Exception.))))]
      (is (thrown? Exception (f)))
      (is (thrown? Exception (f)))
      (is (= 2 @access-count))))
  (testing "That the memo function does not have a race condition"
    (let [access-count (atom 0)
          slow-identity
          (factory (fn [x]
                     (swap! access-count inc)
                     (Thread/sleep 100)
                     x))]
      (every? identity (pvalues (slow-identity 5) (slow-identity 5)))
      (is (= @access-count 1))))
  (testing "That exceptions are correctly unwrapped."
    (is (thrown? ClassNotFoundException ((factory (fn [] (throw (ClassNotFoundException.)))))))
    (is (thrown? IllegalArgumentException ((factory (fn [] (throw (IllegalArgumentException.))))))))
  (testing "Null return caching."
    (let [access-count (atom 0)
          mine (factory (fn [] (swap! access-count inc) nil))]
      (is (nil? (mine)))
      (is (nil? (mine)))
      (is (= @access-count 1)))))

(deftest core-test
  (check-core-features #(memo % inf)))

(deftest memo-with-dropped-args
  ;; must use var to preserve metadata
  (let [mine (memo + (assoc inf mc/key-fn rest))]
    (testing "that key-fnb collapses the cache key space"
      (is (= 13 (mine 1 2 10)))
      (is (= 13 (mine 10 2 1)))
      (is (= 13 (mine 10 2 10)))
      (is (= {[2 10] 13, [2 1] 13} (util/unwrap-as-map (as-map mine)))))))

(deftest put-all-test
  (let [f (memo inc inf)]
    (memo-add! f {[1] 100})
    (is (= {[1] 100} (util/unwrap-as-map (as-map f))))
    (is (= 100 (util/get-entry* f [1])))))

(deftest put-all-test
  ;; TODO PUTALL with expire
  (let [f (memo inc inf)]
    (memo-add! f {[1] 100})
    (is (= {[1] 100} (util/unwrap-as-map (as-map f))))
    (is (= 100 (util/get-entry* f [1])))
    (is (= 100 (f 1)))
    (memo-add! f {[2] (with-tag-id 200 :my-tag 1)})
    (is (= 200 (f 2)))))

(deftest invalidate-test
  (let [f (memo inc inf)
        g (memo dec inf)
        generate (fn [] (f 1) (f 2) (g 1) (g 2))]
    (testing "individual clear"
      (generate)
      (is (= {[1] 2 [2] 3} (util/unwrap-as-map (as-map f))))
      (is (= {[1] 0 [2] 1} (util/unwrap-as-map (as-map g))))
      (memo-clear! f 1)
      (is (= {[2] 3} (util/unwrap-as-map (as-map f))))
      (is (= {[1] 0 [2] 1} (util/unwrap-as-map (as-map g)))))
    (testing "function clear"
      (generate)
      (is (= {[1] 2 [2] 3} (util/unwrap-as-map (as-map f))))
      (is (= {[1] 0 [2] 1} (util/unwrap-as-map (as-map g))))
      (memo-clear! f)
      (is (= {} (util/unwrap-as-map (as-map f))))
      (is (= {[1] 0 [2] 1} (util/unwrap-as-map (as-map g)))))
    (testing "cache clear"
      (generate)
      (is (= {[1] 2 [2] 3} (util/unwrap-as-map (as-map f))))
      (is (= {[1] 0 [2] 1} (util/unwrap-as-map (as-map g))))
      (memo-clear-cache! (active-cache f))
      (is (= {} (util/unwrap-as-map (as-map f))))
      (is (= {} (util/unwrap-as-map (as-map g)))))))

(deftest if-cached-test
  (let [f (memo inc inf)
        g (memo dec inf)
        generate (fn [] (f 1) (f 2) (g 1) (g 2))]
    (testing "individual clear"
      (generate)
      (is (= 3 (b/if-cached (active-cache f) (.segment f) (seq [2]))))
      (is (= b/absent (b/if-cached (active-cache f) (.segment f) (seq [5])))))))

(deftest if-cached-miss-does-not-install-load-marker-test
  (let [f (memo inc inf)]
    (is (= b/absent (b/if-cached (active-cache f) (.segment f) (seq [5]))))
    (is (nil? (util/get-entry* f [5])))))

(deftest if-cached-local-load-returns-absent-test
  (let [started (promise)
        release (promise)
        f (memo (fn [_]
                  (deliver started true)
                  @release
                  2)
                inf)
        fut (future (f 1))]
    @started
    (is (= b/absent (b/if-cached (active-cache f) (.segment f) (seq [1]))))
    (deliver release true)
    (is (= 2 @fut))))

(deftest if-cached-refreshes-fade-on-hit-test
  (let [f (memo inc (dissoc (assoc inf mc/fade [500 :ms]) mc/ttl))]
    (is (= 2 (f 1)))
    (Thread/sleep 300)
    (is (= 2 (b/if-cached (active-cache f) (.segment f) (seq [1]))))
    (Thread/sleep 300)
    (is (= 2 (util/get-entry* f [1])))))

(deftest ttl-test
  (let [access-count (atom 0)
        f (memo (fn [] (swap! access-count inc) 1) (assoc inf mc/ttl [500 :ms]))]
    (f)
    (f)
    (is (= @access-count 1))
    (Thread/sleep 300)
    (f)
    (is (= @access-count 1))
    (Thread/sleep 300)
    (f)
    (is (= @access-count 2))))

(deftest fade-test
  (let [access-count (atom 0)
        f (memo (fn [] (swap! access-count inc) 1) (dissoc (assoc inf mc/fade [500 :ms]) mc/ttl))]
    (f)
    (f)
    (is (= @access-count 1))
    (Thread/sleep 300)
    (f)
    (is (= @access-count 1))
    (Thread/sleep 300)
    (f)
    (is (= @access-count 1))))

(deftest invalidate-by-tag
  (let [access-count (atom 0)
        f (memo (fn [x] (swap! access-count inc)
                  (with-tag-id x :test-tag 1)) [:test-tag] inf)]
    ;; should not throw exception on empty tag
    (memo-clear-tag! :test-tag 1)
    (memo-add! f {[1] (with-tag-id 2 :test-tag 2)})
    (f 1)
    (f 1)
    (is (= 0 @access-count))
    (memo-clear-tag! :test-tag 2)
    (f 1)
    (f 1)
    (is (= 1 @access-count))
    (memo-clear-tag! :test-tag 1)
    (f 1)
    (f 1)
    (is (= 2 @access-count))))

(deftest hit-miss-check
  (let [f (memo identity (assoc inf mr/hit-detect? true))]
    (testing "That hits and misses are recorded"
      (are [in m]
        (= m (meta (f in)))
        [1] nil
        [2] nil
        [1] #:memento.redis{:cached? true}
        [2] #:memento.redis{:cached? true}
        1 nil
        nil nil))))

(deftest exception-test
  (let [e (InterruptedException.)
        cnt (atom 0)
        f (memo #(do (Thread/sleep 1000) (swap! cnt inc) (throw e)) inf)
        f1 (future (try (f) (catch Exception e e)))
        f2 (future (try (f) (catch Exception e e)))]
    (is (= e @f1))
    (is (= e @f2))
    (is (= 1 @cnt))))

(deftest invalidation-test
  (let [c (create inf)
        cnt (atom 0)
        f (bind (fn [x] (swap! cnt inc) (Thread/sleep 1000) (with-tag-id {} :aa 1)) {} c)]
    (are [invalidation-f msg]
      (let [_ (util/wipe)
            _ (reset! cnt 0)
            f1 (future (f 0))]
        (Thread/sleep 100)
        (invalidation-f)
        @f1
        (testing msg (is (= 2 @cnt))))
      #(memo-clear! f) "memo-clear"
      #(memo-clear! f 0) "memo-clear 0"
      #(memo-clear-tag! :aa 1) "memo-clear-tag"
      #(memo-clear-cache! c) "memo-clear-cache")))

(deftest redis-tag-epoch-rejects-stale-load-test
  (let [c (create inf)
        cnt (atom 0)
        f (bind (fn [_]
                  (let [n (swap! cnt inc)]
                    (Thread/sleep 1000)
                    (with-tag-id n :aa 1)))
                {}
                c)
        keygen (-> c :fns :keygen)
        cname (:cname c)
        id-key (keys/sec-index-id-key keygen cname [:aa 1])]
    (let [fut (future (f 0))]
      (Thread/sleep 100)
      ;; Simulate another JVM invalidating the tag without relying on local pub/sub delivery.
      (sec-index/invalidate-by-index
        {}
        (keys/sec-indexes-key keygen)
        (keys/epoch-key keygen cname)
        (keys/tag-epochs-key keygen cname)
        [id-key])
      @fut
      (is (= 2 @cnt))
      (is (= 2 (f 0))))))

(deftest redis-tag-epoch-rejects-joiner-load-test
  ;; Joiners blocked on SpecialPromise.await must also observe absent and re-loop
  ;; when the loader's completeLoad returns COMPLETE_LOAD_STALE_TAG. This validates
  ;; the p.reject() call in Loader.get's tagged-path stale-tag handling.
  (let [c (create inf)
        cnt (atom 0)
        f (bind (fn [_]
                  (let [n (swap! cnt inc)]
                    (Thread/sleep 1000)
                    (with-tag-id n :aa 1)))
                {}
                c)
        keygen (-> c :fns :keygen)
        cname (:cname c)
        id-key (keys/sec-index-id-key keygen cname [:aa 1])]
    (let [loader-fut (future (f 0))
          _ (Thread/sleep 50)
          joiner-fut (future (f 0))]
      (Thread/sleep 100)
      ;; Simulate another JVM invalidating the tag without relying on local pub/sub delivery.
      (sec-index/invalidate-by-index
        {}
        (keys/sec-indexes-key keygen)
        (keys/epoch-key keygen cname)
        (keys/tag-epochs-key keygen cname)
        [id-key])
      (let [loader-v @loader-fut
            joiner-v @joiner-fut]
        (is (= {:cnt 2 :loader 2 :joiner 2}
               {:cnt @cnt :loader loader-v :joiner joiner-v}))))))

(deftest redis-tag-epoch-ignores-unrelated-tags-test
  (let [c (create inf)
        cnt (atom 0)
        f (bind (fn [_]
                  (let [n (swap! cnt inc)]
                    (Thread/sleep 1000)
                    (with-tag-id n :aa 1)))
                {}
                c)
        keygen (-> c :fns :keygen)
        cname (:cname c)
        id-key (keys/sec-index-id-key keygen cname [:bb 1])]
    (let [fut (future (f 0))]
      (Thread/sleep 100)
      (sec-index/invalidate-by-index
        {}
        (keys/sec-indexes-key keygen)
        (keys/epoch-key keygen cname)
        (keys/tag-epochs-key keygen cname)
        [id-key])
      (is (= 1 @fut))
      (is (= 1 @cnt))
      (is (= 1 (f 0)))
      (is (= 1 @cnt)))))

(deftest redis-tag-epoch-allows-manual-add-after-invalidation-test
  (let [f (memo inc (assoc inf mc/tags #{:test-tag}))
        cache (active-cache f)
        keygen (-> cache :fns :keygen)
        cname (:cname cache)
        id-key (keys/sec-index-id-key keygen cname [:test-tag 1])]
    (sec-index/invalidate-by-index
      {}
      (keys/sec-indexes-key keygen)
      (keys/epoch-key keygen cname)
      (keys/tag-epochs-key keygen cname)
      [id-key])
    (memo-add! f {[1] (with-tag-id 100 :test-tag 1)})
    (is (= 100 (f 1)))
    (is (= {[1] 100} (util/unwrap-as-map (as-map f))))))

(deftest joiner-hit-mark-test
  ;; §5.7/§5.9: joiners that block on SpecialPromise.await must observe the same
  ;; :memento.redis/cached? hit-mark as direct-hit callers when hit-detect? is on.
  (let [f (memo (fn [_] (Thread/sleep 500) {:v 1})
                (assoc inf mr/hit-detect? true))
        loader-fut (future (f 0))
        _ (Thread/sleep 50)
        joiner-fut (future (f 0))
        loader-v @loader-fut
        joiner-v @joiner-fut]
    (is (= {:loader-val {:v 1}
            :joiner-val {:v 1}
            :loader-meta nil
            :joiner-meta #:memento.redis{:cached? true}}
           {:loader-val loader-v
            :joiner-val joiner-v
            :loader-meta (meta loader-v)
            :joiner-meta (meta joiner-v)}))))

(deftest nil-round-trip-test
  ;; §5.4: nil values must survive a full cache round-trip (envelope discriminator
  ;; preserves nil; absent → missing-key sentinel). The second call must not re-invoke.
  (let [cnt (atom 0)
        f (memo (fn [_] (swap! cnt inc) nil) inf)]
    (is (= {:first nil :second nil :calls 1}
            {:first (f 0) :second (f 0) :calls @cnt}))))

(deftest as-map-keeps-nil-values-test
  (let [f (memo (constantly nil) inf)]
    (f 0)
    (is (= {[0] nil} (util/unwrap-as-map (as-map f))))))

(deftest stale-tagged-hit-reloads-test
  ;; §5.5 / §5.8: a tagged 0x02 envelope already in Redis whose writeEpoch is
  ;; older than the current tag epoch (i.e. a tag was invalidated after the
  ;; entry was written) must be treated as a miss on the next read. The outer
  ;; RedisCache.cached loop reloads and produces a fresh value; the stale
  ;; envelope must NOT be returned and must not be silently deleted (no DEL on
  ;; the hit slow-path — the new write replaces it).
  (let [c (create inf)
        cnt (atom 0)
        f (bind (fn [_] (with-tag-id (swap! cnt inc) :st 1)) {} c)
        keygen (-> c :fns :keygen)
        cname (:cname c)
        id-key (keys/sec-index-id-key keygen cname [:st 1])]
    ;; Populate: cnt=1, value 1 cached as tagged envelope.
    (f 0)
    ;; Bump tag epoch in Redis to invalidate the cached envelope without
    ;; touching the entry key itself.
    (sec-index/invalidate-by-index
      {}
      (keys/sec-indexes-key keygen)
      (keys/epoch-key keygen cname)
      (keys/tag-epochs-key keygen cname)
      [id-key])
    ;; Second call: slow-path stale check trips, outer loop reloads, cnt=2.
    (is (= {:second 2 :third 2 :calls 2}
           {:second (f 0) :third (f 0) :calls @cnt}))))

(deftest multi-tag-any-stale-rejects-test
  ;; §5.5: a value carrying multiple tag idents is stale if ANY one of those
  ;; tag epochs is newer than the entry's writeEpoch. Bumping a single tag
  ;; (here :mt2) must reject the entry; the unrelated tag :mt1 stays current.
  (let [c (create inf)
        cnt (atom 0)
        f (bind (fn [_]
                  (-> (swap! cnt inc)
                      (with-tag-id :mt1 1)
                      (with-tag-id :mt2 2)))
                {}
                c)
        keygen (-> c :fns :keygen)
        cname (:cname c)
        id-key-mt2 (keys/sec-index-id-key keygen cname [:mt2 2])]
    (f 0)
    (sec-index/invalidate-by-index
      {}
      (keys/sec-indexes-key keygen)
      (keys/epoch-key keygen cname)
      (keys/tag-epochs-key keygen cname)
      [id-key-mt2])
    (is (= {:second 2 :calls 2}
           {:second (f 0) :calls @cnt}))))

(deftest tag-epochs-isolated-by-cache-name-test
  ;; Tag epoch metadata is namespaced by cache name (epoch-key/tag-epochs-key
  ;; both include cname). Invalidating tag :iso on cache A must not affect a
  ;; load on cache B sharing the same tag identifier.
  (let [ca (create (assoc inf mr/name "iso-A"))
        cb (create (assoc inf mr/name "iso-B"))
        cnt-a (atom 0)
        cnt-b (atom 0)
        fa (bind (fn [_] (with-tag-id (swap! cnt-a inc) :iso 1)) {} ca)
        fb (bind (fn [_] (with-tag-id (swap! cnt-b inc) :iso 1)) {} cb)
        keygen (-> ca :fns :keygen)
        cname-a (:cname ca)
        id-key-a (keys/sec-index-id-key keygen cname-a [:iso 1])]
    (fa 0) (fb 0)
    ;; Invalidate :iso on cache A only.
    (sec-index/invalidate-by-index
      {}
      (keys/sec-indexes-key keygen)
      (keys/epoch-key keygen cname-a)
      (keys/tag-epochs-key keygen cname-a)
      [id-key-a])
    (is (= {:fa-second 2 :fb-second 1 :cnt-a 2 :cnt-b 1}
           {:fa-second (fa 0)
            :fb-second (fb 0)
            :cnt-a @cnt-a
            :cnt-b @cnt-b}))))

(deftest var-expiry-test
  (testing "uses segment expiry"
    (let [c (create (assoc inf mc/ttl 5 mr/ttl-fn (fn [_ _ v] (when (= v -1) 1))))
          f (bind (comp identity identity) {mc/ttl 1 mr/ttl-fn (fn [_ _ v] (when (= v -1) 5))} c)
          f2 (bind (comp identity identity) {} c)]
      (f 1)
      (f -1)
      (f2 1)
      (f2 -1)
      (is (= {[-1] -1 [1] 1} (util/unwrap-as-map (as-map f))))
      (is (= {[-1] -1 [1] 1} (util/unwrap-as-map (as-map f2))))
      (Thread/sleep 1500)
      (is (= {[-1] -1} (util/unwrap-as-map (as-map f))))
      (is (= {[1] 1} (util/unwrap-as-map (as-map f2)))))))
