(ns memento.redis.loader-test
  (:require [clojure.test :refer :all]
            [memento.redis.test-util :as util]
            [memento.redis.loader :as loader]
            [memento.base :as b]
            [memento.core :as c]
            [taoensso.carmine :as car])
  (:import (java.util ArrayList)
           (java.util.concurrent ConcurrentHashMap)
           (memento.base Segment)
           (memento.redis.loader LoadMarker)
           (memento.redis.poll Load Loader)))

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
    (.put submap (util/test-key "A") (Load. (loader/new-load-marker)))
    (.put submap (util/test-key "B") (Load. nil))
    (loader/remove-load-markers l)
    (is (.isEmpty l))
    (is (nil? (util/get-entry "A")))
    (is (some? (util/get-entry "B")))))

(defn create-loader
  "local-maker is boolean
   marker can be :none :our :their, which describes which marker is in the DB

   returns Loader"
  ([] (Loader. "" util/test-keygen nil nil nil nil nil false (ConcurrentHashMap.) loader/support))
  ([k local-marker? marker]
   (create-loader k local-marker? marker nil nil))
  ([k local-marker? marker ttl-ms fade-ms]
   (let [load-marker (loader/new-load-marker)
         loader (Loader. "" util/test-keygen nil nil (when ttl-ms [ttl-ms :ms]) (when fade-ms [fade-ms :ms]) nil false (ConcurrentHashMap.) loader/support)
         entry (Load. (when local-marker? load-marker))]
     (.put (.connMap loader {}) (util/test-key k) entry)
     (case marker
       :none nil
       :our (util/add-entry k load-marker)
       :their (util/add-entry k (loader/new-load-marker)))
     loader)))

(defn load-for [^Loader loader k] (get-in (.getMaint loader) [{} (util/test-key k)]))

(defn start-load [loader k]
  (let [p (promise)
        s (Segment. (fn [] (deref p 10000 :timeout)) nil "" {})
        ret (future (.get loader {} s nil (util/test-key k)))]
    (Thread/sleep 200)
    [p ret (util/get-entry k) (load-for loader k)]))

(defn deliver-val [loader start k v]
  (deliver (first start) v)
  [(deref (second start) 1000 :deliver-timeout) (util/get-entry k) (load-for loader k)])

(deftest lookup-test
  (testing "if no entry and nothing in redis, create and return maintenance entry"
    (let [l (create-loader)
          [p ret redis-val load :as start] (start-load l :x1)]
      (is (= (.getLoadMarker load) redis-val))
      (is (not (realized? ret)))
      (deliver p nil)))
  (testing "if no entry and there's a load marker there, have load that is awaiting"
    (let [l (create-loader)
          marker (loader/new-load-marker)
          _ (util/add-entry :x2 marker)
          [p ret redis-val load] (start-load l :x2)]
      (is (= nil (.getLoadMarker load)))
      (is (instance? LoadMarker redis-val))
      (is (not (realized? ret)))
      (util/add-entry :x2 "10")
      (loader/maintenance-step (.getMaint l) false)
      (is (= "10" (deref ret 5000 :timeout)))))
  (testing "if no entry and there's a value there, return value without any load there"
    (let [l (create-loader)
          _ (util/add-entry :x3 "55")
          [p ret redis-val load] (start-load l :x3)]
      (is (realized? ret))
      (is (= nil load))
      (is (= "55" (deref ret 5000 :timeout))))))

(deftest complete-test
  (testing "leave alone non-remote load, await foreign load"
    (let [l (create-loader "A" false :none)
          [_ _ redis-val load :as start] (start-load l "A")
          [ret redis-after load-after] (deliver-val l start "A" :some)
          ]
      (is (= nil redis-val))
      (is (= nil (.getLoadMarker load)))
      ;; we were awaiting another thread load
      (is (= :deliver-timeout ret))
      (is (= nil (.getLoadMarker load-after)))
      (is (= nil redis-after))))
  (testing "our load, finish with non-cache"
    ;; our load, should delete redis marker and deliver local
    (let [l (create-loader)
          [_ _ redis-val load :as start] (start-load l "B")
          [ret redis-after load-after] (deliver-val l start "B" (c/do-not-cache :some))]
      (is (= (.getLoadMarker load) redis-val))
      ;;
      (is (= :some ret))
      (is (= nil redis-after))
      (is (= nil load-after))))
  (testing "our load, finish with val"
    ;; our load, should delete redis marker and deliver local
    (let [l (create-loader)
          [_ _ redis-val load :as start] (start-load l "B")
          [ret redis-after load-after] (deliver-val l start "B" :some)]
      (is (= (.getLoadMarker load) redis-val))
      ;;
      (is (= :some ret))
      (is (= :some redis-after))
      (is (= nil load-after))))
  (testing "our load, marker has expired, finish with non-cache"
    (let [l (create-loader)
          [_ _ redis-val load :as start] (start-load l "C")
          _ (car/wcar {} (car/del (util/test-key "C")))
          [ret redis-after load-after] (deliver-val l start "C" (c/do-not-cache :some))]
      (is (= redis-val (.getLoadMarker load)))
      (is (= nil redis-after))
      (is (= :some ret))
      (is (= nil load-after))))
  (testing "our load, marker has expired, finish with val"
    (let [l (create-loader)
          [_ _ redis-val load :as start] (start-load l "C")
          _ (car/wcar {} (car/del (util/test-key "C")))
          [ret redis-after load-after] (deliver-val l start "C" :some)]
      (is (= redis-val (.getLoadMarker load)))
      (is (= nil redis-after))
      (is (= :some ret))
      (is (= nil load-after)))))

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

(deftest maintenance-step-test
  (testing "Fetching results, returning b/absent"
    (let [l (create-loader :a false :none)
          p (-> (.getMaint l) (.get {}) (.get (util/test-key :a)) (.getPromise))]
      (is (= b/absent (.getNow p)))
      (loader/maintenance-step (.getMaint l) false)
      (is (= b/absent (.await p :a)))
      (is (= {{} {}} (.getMaint l)))))
  (testing "Fetching results"
    (let [l (create-loader :a false :none)
          _ (util/add-entry :a true)
          p (-> (.getMaint l) (.get {}) (.get (util/test-key :a)) (.getPromise))]
      (is (= b/absent (.getNow p)))
      (loader/maintenance-step (.getMaint l) false)
      (is (= true (.await p :a)))
      (is (= {{} {}} (.getMaint l))))))
