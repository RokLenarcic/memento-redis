(ns memento.redis.loader-test
  (:require [clojure.test :refer :all]
            [memento.redis.test-util :as util]
            [memento.redis.loader :as loader]
            [memento.base :as b]
            [memento.core :as c]
            [taoensso.carmine :as car])
  (:import (java.util ArrayList)
           (java.util.concurrent ConcurrentHashMap)
           (memento.redis.loader LoadMarker)
           (clojure.lang IDeref)))

(use-fixtures :each util/fixture-wipe)

(defn other-load-marker?
  "Returns true if v is a load marker that is not this exact load marker."
  [v load-marker]
  (and (instance? LoadMarker v)
       (not= load-marker v)))

(deftest cval-test
  (are [v]
    (= v (do (util/add-entry "A" (loader/cval v)) (util/get-entry "A")))
    :a-keyword
    "A STR"
    11
    114N
    nil
    true))

(deftest cached-entries-test
  (util/add-entry "A" "X")
  (util/add-entry "B" "Y")
  (util/add-entry "C" (loader/new-load-marker))
  (is (= [[(util/test-key "A") "X"] [(util/test-key "B") "Y"]
          [(util/test-key "D") b/absent]]
         (loader/cached-entries {} [(util/test-key "A")
                                    (util/test-key "B")
                                    (util/test-key "C")
                                    (util/test-key "D")])))
  (is (= nil (loader/cached-entries {} [])))
  (is (= nil (loader/cached-entries {} nil))))

(deftest refresh-load-markers-test
  (testing "Refreshes a load marker"
    (util/add-entry "A" (loader/->LoadMarker "AA"))
    (loader/refresh-load-markers
      {}
      [(util/test-key "A")]
      (doto (ArrayList.) (.add (loader/->LoadMarker "AA")))
      1)
    (Thread/sleep 2000)
    (is (= nil (util/get-entry "A"))))
  (testing "Leaves markers that aren't ours alone"
    (util/add-entry "A" (loader/->LoadMarker "AA"))
    (loader/refresh-load-markers
      {}
      [(util/test-key "A")]
      (doto (ArrayList.) (.add (loader/->LoadMarker "BB")))
      1)
    (Thread/sleep 2000)
    (is (= (loader/->LoadMarker "AA") (util/get-entry "A")))))

(deftest remove-connections-without-loads-test
  (let [l (ConcurrentHashMap.)]
    (.put l {} (ConcurrentHashMap.))
    (is (not (.isEmpty l)))
    (loader/remove-connections-without-loads l)
    (is (.isEmpty l))))

(deftest remove-load-markers-test
  (let [l (ConcurrentHashMap.)
        submap (ConcurrentHashMap.)]
    (util/add-entry "A" (loader/new-load-marker))
    (util/add-entry "B" (loader/new-load-marker))
    (.put l {} submap)
    (.put submap (util/test-key "A") {:marker true
                                      :result (promise)})
    (.put submap (util/test-key "B") {:result (promise)})
    (loader/remove-load-markers l)
    (is (.isEmpty l))
    (is (nil? (util/get-entry "A")))
    (is (some? (util/get-entry "B")))))

(deftest update-maint-test
  (let [l (ConcurrentHashMap.)]
    (is (= :y (loader/update-maint l :x "A" (fn [_ cb] (cb :y) "A"))))
    (is (= {:x {"A" "A"}} l))
    (loader/update-maint l :x "A" (constantly nil))
    (is (= {:x {}} l))))

(defn create-loader
  "local-maker is boolean
   marker can be :none :our :their, which describes which marker is in the DB

   returns PollingLoader"
  ([] (loader/->PollingLoader (ConcurrentHashMap.) util/test-keygen "" nil nil))
  ([k local-marker? marker]
   (create-loader k local-marker? marker nil nil))
  ([k local-marker? marker ttl-ms fade-ms]
   (let [l (ConcurrentHashMap.)
         load-marker (loader/new-load-marker)
         entry {:result (promise) :marker (when local-marker? load-marker)}]
     (loader/update-maint l {} (util/test-key k) (constantly entry))
     (case marker
       :none nil
       :our (util/add-entry k load-marker)
       :their (util/add-entry k (loader/new-load-marker)))
     (loader/->PollingLoader l util/test-keygen "" ttl-ms fade-ms))))

(defn complete-with [loader v]
  (let [k (-> loader :maint-map first val first key)
        p (get-in loader [:maint-map {} k :result])
        _ (loader/complete loader {} k v)
        redis-val (car/wcar {} (car/get k))]
    [k p redis-val]))

(deftest complete-test
  (testing "leave alone non-remote load"
    (let [l (create-loader "A" false :none)
          [k p redis-val] (complete-with l :some)]
      (is (= nil redis-val))
      (is (= :none (deref p 1 :none)))
      (is (some? (get-in l [:maint-map {} k])))))
  (testing "our load, finish with non-cache"
    ;; our load, should delete redis marker and deliver local
    (let [l (create-loader "B" true :our)
          [k p redis-val] (complete-with l (c/do-not-cache :some))]
      (is (= nil redis-val))
      (is (= :some (deref p 1 :none)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "our load, finish with val"
    ;; our load, should delete redis marker and deliver local
    (let [l (create-loader "B" true :our)
          [k p redis-val] (complete-with l :some)]
      (is (= :some redis-val))
      (is (= :some (deref p 1 :none)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "their load, finish with non-cache"
    ;; their load, should just deliver promise locally and delete maintenance
    ;; leave their token up
    (let [l (create-loader "C" true :their)
          [k p redis-val] (complete-with l (c/do-not-cache :some))]
      (is (instance? LoadMarker redis-val))
      (is (= :some (deref p 1 :none)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "their load, finish with val"
    ;; their load, should just deliver promise locally and delete maintenance
    ;; leave their token up
    (let [l (create-loader "C" true :their)
          [k p redis-val] (complete-with l :some)]
      (is (instance? LoadMarker redis-val))
      (is (= :some (deref p 1 :none)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "total disconnect, foreign load, finish with non-cache"
    (let [l (create-loader "D" false :their)
          [k p redis-val] (complete-with l (c/do-not-cache :some))]
      (is (instance? LoadMarker redis-val))
      (is (= :none (deref p 1 :none)))
      (is (some? (get-in l [:maint-map {} k])))))
  (testing "total disconnect, foreign load, finish with val"
    (let [l (create-loader "D" false :their)
          [k p redis-val] (complete-with l :some)]
      (is (instance? LoadMarker redis-val))
      (is (= :none (deref p 1 :none)))
      (is (some? (get-in l [:maint-map {} k])))))
  (testing "our load, marker has expired, finish with non-cache"
    (let [l (create-loader "E" true :none)
          [k p redis-val] (complete-with l (c/do-not-cache :some))]
      (is (= nil redis-val))
      (is (= :some (deref p 1 :none)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "our load, marker has expired, finish with val"
    (let [l (create-loader "E" true :none)
          [k p redis-val] (complete-with l :some)]
      (is (= nil redis-val))
      (is (= :some (deref p 1 :none)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "no load locally, but our marker, finish with non-cache"
    ;; we have a marker in the database, but for some reason not locally,
    ;; we're not handling this case, nothing is touched
    (let [l (create-loader "F" false :our)
          [k p redis-val] (complete-with l (c/do-not-cache :some))]
      (is (instance? LoadMarker redis-val))
      (is (= :none (deref p 1 :none)))
      (is (some? (get-in l [:maint-map {} k])))))
  (testing "no load locally, but our marker, finish with val"
    ;; we have a marker in the database, but for some reason not locally,
    ;; we're not handling this case, nothing is touched
    (let [l (create-loader "F" false :our)
          [k p redis-val] (complete-with l (c/do-not-cache :some))]
      (is (instance? LoadMarker redis-val))
      (is (= :none (deref p 1 :none)))
      (is (some? (get-in l [:maint-map {} k]))))))

(deftest fetch-test
  (testing "load marker is set if not there"
    (let [load-marker (loader/new-load-marker)
          [present? v] (loader/fetch {} (util/test-key "A") load-marker 1000000 10000)]
      (is (not present?))
      (is (= load-marker v))
      (is (= load-marker (util/get-entry "A")))))
  (testing "load marker is not set if there is a value already"
    (let [_ (util/add-entry "B" (loader/cval 1))
          load-marker (loader/new-load-marker)
          [present? v] (loader/fetch {} (util/test-key "B") load-marker 1000000 10000)]
      (is present?)
      (is (= 1 v))
      (is (= 1 (util/get-entry "B")))))
  (testing "load marker fade is respected"
    (let [load-marker (loader/new-load-marker)
          [present? v] (loader/fetch {} (util/test-key "C") load-marker 1 10000)]
      (is (not present?))
      (is (= load-marker v))
      (Thread/sleep 10)
      (is (= nil (util/get-entry "C")))))
  (testing "load marker fade is not renewed on access"
    (let [_ (loader/fetch {} (util/test-key "D") (loader/new-load-marker) 1000 10000)
          load-marker (loader/new-load-marker)
          [present? v] (loader/fetch {} (util/test-key "D") load-marker 1000 10000)]
      (is present?)
      (is (other-load-marker? v load-marker))
      (is (other-load-marker? (util/get-entry "D") load-marker))
      (Thread/sleep 500)
      (loader/fetch {} (util/test-key "D") load-marker 1000 10000)
      (Thread/sleep 750)
      (is (= nil (util/get-entry "D")))))
  (testing "fade is refreshed if normal value is there"
    (let [_ (car/wcar {} (car/set (util/test-key "E") "ABC" "PX" 1000))
          load-marker (loader/new-load-marker)
          [present? v] (loader/fetch {} (util/test-key "E") load-marker 1000 1000)]
      (is present?)
      (is (= "ABC" v))
      (Thread/sleep 500)
      (loader/fetch {} (util/test-key "E") load-marker 1000 1000)
      (Thread/sleep 750)
      (is (= "ABC" (util/get-entry "E")))))
  (testing "fetch missing key without putting a load marker"
    (let [load-marker (loader/new-load-marker)
          [present? v] (loader/fetch {} (util/test-key "missing") load-marker -1 nil)]
      (is (not present?))
      (is (= v nil))
      (is (= nil (util/get-entry "missing"))))))

(deftest lookup-test
  (testing "if entry is there always return that promise"
    (are [k local-marker? redis-marker]
      (let [l (create-loader k local-marker? redis-marker)]
        (is (instance? IDeref (loader/start l {} (util/test-key k)))))
      :a true :none
      :b false :none
      :c true :our
      :d false :our
      :e true :their
      :f true :their))
  (testing "if no entry and nothing in redis, create and return maintenance entry"
    (let [l (create-loader)
          ret (loader/start l {} (util/test-key :x1))]
      (is (nil? ret))
      (is (= (get-in l [:maint-map {} (util/test-key :x1) :marker] ret) (util/get-entry :x1)))
      (is (not (realized? (get-in l [:maint-map {} (util/test-key :x1) :result] ret))))))
  (testing "if no entry and there's a load marker there, create and return maintenance without
  the load marker"
    (let [l (create-loader)
          marker (loader/new-load-marker)
          _ (util/add-entry :x2 marker)
          ret (loader/start l {} (util/test-key :x2))]
      (is (instance? IDeref ret))
      (is (not (realized? ret)))))
  (testing "if no entry and there's a value there, create and return maintenance without
  the load marker"
    (let [l (create-loader)
          _ (util/add-entry :x3 "55")
          ret (loader/start l {} (util/test-key :x3))]
      (is (instance? IDeref ret))
      (is (= "55" @ret)))))

(deftest maintenance-step-test
  (testing "Fetching results, returning b/absent"
    (let [l (create-loader :a false :none)
          p (get-in l [:maint-map {} (util/test-key :a) :result])]
      (is (not (realized? p)))
      (loader/maintenance-step (:maint-map l) false)
      (is (= b/absent (deref p 1 :x)))
      (is (= {{} {}} (:maint-map l)))))
  (testing "Fetching results"
    (let [l (create-loader :a false :none)
          _ (util/add-entry :a true)
          p (get-in l [:maint-map {} (util/test-key :a) :result])]
      (is (not (realized? p)))
      (loader/maintenance-step (:maint-map l) false)
      (is (= true (deref p 1 :x)))
      (is (= {{} {}} (:maint-map l))))))
