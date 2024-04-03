(ns memento.redis.cache-test
  (:require [clojure.test :refer :all]
            [memento.base :as b]
            [memento.core :refer :all]
            [memento.config :as mc]
            [memento.redis.cache :as cache]
            [memento.redis :as mr]
            [memento.redis.keys :as keys]
            [memento.redis.test-util :as util])
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
    {} (Segment. identity identity :id) (list "M^" "" :id [10])
    {mr/name "cache-name"} (Segment. identity identity :id) (list "M^" "cache-name" :id [10])
    {mc/key-fn (comp inc first)} (Segment. identity identity :id) (list "M^" "" :id 11)
    {mc/key-fn inc} (Segment. identity (comp #(* 2 %) first) :id) (list "M^" "" :id 21)))

(deftest as-map-test
  (let [memod (memo inc inf)]
    (doseq [x (range 5)] (memod x))
    (is (= {[(str inc) [0]] 1
            [(str inc) [1]] 2
            [(str inc) [2]] 3
            [(str inc) [3]] 4
            [(str inc) [4]] 5}
           (b/as-map (active-cache memod))))
    (is (= {[0] 1
            [1] 2
            [2] 3
            [3] 4
            [4] 5}
           (as-map memod)))))

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
      (is (= {[2 10] 13, [2 1] 13} (as-map mine))))))

(deftest put-all-test
  (let [f (memo inc inf)]
    (memo-add! f {[1] 100})
    (is (= {[1] 100} (as-map f)))
    (is (= 100 (util/get-entry* f [1])))))

(deftest put-all-test
  ;; TODO PUTALL with expire
  (let [f (memo inc inf)]
    (memo-add! f {[1] 100})
    (is (= {[1] 100} (as-map f)))
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
      (is (= {[1] 2 [2] 3} (as-map f)))
      (is (= {[1] 0 [2] 1} (as-map g)))
      (memo-clear! f 1)
      (is (= {[2] 3} (as-map f)))
      (is (= {[1] 0 [2] 1} (as-map g))))
    (testing "function clear"
      (generate)
      (is (= {[1] 2 [2] 3} (as-map f)))
      (is (= {[1] 0 [2] 1} (as-map g)))
      (memo-clear! f)
      (is (= {} (as-map f)))
      (is (= {[1] 0 [2] 1} (as-map g))))
    (testing "cache clear"
      (generate)
      (is (= {[1] 2 [2] 3} (as-map f)))
      (is (= {[1] 0 [2] 1} (as-map g)))
      (memo-clear-cache! (active-cache f))
      (is (= {} (as-map f)))
      (is (= {} (as-map g))))))

(deftest if-cached-test
  (let [f (memo inc inf)
        g (memo dec inf)
        generate (fn [] (f 1) (f 2) (g 1) (g 2))]
    (testing "individual clear"
      (generate)
      (is (= 3 (b/if-cached (active-cache f) (.segment f) (seq [2]))))
      (is (= b/absent (b/if-cached (active-cache f) (.segment f) (seq [5])))))))

(deftest conf-millis-test
  (is (= 3000 (cache/conf-millis {:m 3} :m)))
  (is (= 180000 (cache/conf-millis {:m [3 :m]} :m))))

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
        [1] #:memento.redis{:cached? false}
        [2] #:memento.redis{:cached? false}
        [1] #:memento.redis{:cached? true}
        [2] #:memento.redis{:cached? true}
        1 nil
        nil nil))))