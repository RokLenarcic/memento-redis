(ns memento.redis.loader-test
  (:require [clojure.test :refer :all]
            [memento.redis.test-util :as util]
            [memento.redis.keys :as keys]
            [memento.redis.loader :as loader]
            [memento.base :as b]
            [memento.core :as c]
            [taoensso.carmine :as car])
  (:import (java.util ArrayList)
           (java.util UUID)
           (java.util.concurrent ConcurrentHashMap)
           (memento.base EntryMeta Segment)
           (memento.redis EntryEnvelope)
           (memento.redis.poll Load Loader)))

(use-fixtures :each util/fixture-wipe)

(defn other-load-marker?
  "Returns true if v is a load marker that is not this exact load marker."
  [v load-marker]
  (and (instance? UUID v)
       (not= load-marker v)))

(deftest write-envelope-test
  (are [v]
    (= v (do (util/add-entry "A" (EntryEnvelope/writeEnvelope v)) (util/get-entry "A")))
    :a-keyword
    "A STR"
    11
    114N
    nil
    true))

(deftest cached-entries-test
  (util/add-entry "A" "X")
  (util/add-entry "B" "Y")
  (util/add-entry "C" (Load/newLoadMarker))
  (let [ks [[(util/test-key "A") (Load. 0)]
            [(util/test-key "B") (Load. 0)]
            [(util/test-key "C") (Load. 0)]
            [(util/test-key "D") (Load. 0)]]
        results (loader/cached-entries
                  {} ks)
        delivery-by-key (into {} (map (fn [[k _load d]] [k d]) results))]
    (is (= {(util/test-key "A") "X"
            (util/test-key "B") "Y"
            (util/test-key "D") b/absent}
           delivery-by-key)))
  (is (= nil (loader/cached-entries {} [])))
  (is (= nil (loader/cached-entries {} nil))))

(deftest refresh-load-markers-test
  (testing "Refreshes a load marker"
    (let [m (Load/newLoadMarker)]
      (util/add-entry "A" m)
        (loader/refresh-load-markers
          {}
          [(util/test-key "A")]
        (doto (ArrayList.) (.add (Load/loadMarkerBytes m)))
        1)
      (Thread/sleep 2000)
      (is (= nil (util/get-entry "A")))))
  (testing "Leaves markers that aren't ours alone"
    (let [our (Load/newLoadMarker)
          their (Load/newLoadMarker)]
      (util/add-entry "A" their)
        (loader/refresh-load-markers
          {}
          [(util/test-key "A")]
        (doto (ArrayList.) (.add (Load/loadMarkerBytes our)))
        1)
      (Thread/sleep 2000)
      (is (= their (util/get-entry "A"))))))

(deftest remove-load-markers-test
  (let [l (ConcurrentHashMap.)
        submap (ConcurrentHashMap.)
        foreign-load (doto (Load. 0) (.foreignLoad))]
    (util/add-entry "A" (Load/newLoadMarker))
    (util/add-entry "B" (Load/newLoadMarker))
    (.put l {} submap)
    (.put submap (util/test-key "A") (Load. 0))
    (.put submap (util/test-key "B") foreign-load)
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
   (let [loader (Loader. "" util/test-keygen nil nil (when ttl-ms [ttl-ms :ms]) (when fade-ms [fade-ms :ms]) nil false (ConcurrentHashMap.) loader/support)
          entry (Load. 0)]
     (when-not local-marker?
       (.foreignLoad entry))
     (.put (.connMap loader {}) (util/test-key k) entry)
     (case marker
       :none nil
       :our (util/add-entry k (.getLoadMarker entry))
       :their (util/add-entry k (Load/newLoadMarker)))
     loader)))

(defn load-for [^Loader loader k] (get-in (.getMaint loader) [{} (util/test-key k)]))

(defn fetch [k load-marker load-ms fade-ms]
  (loader/fetch {} (util/test-key k) (keys/epoch-key util/test-keygen "") load-marker load-ms fade-ms))

(defn- decode-fetch-v
  "Decode the raw v returned by `loader/fetch` into a test-friendly value
  using the same envelope rules as test-util/get-entry."
  [v]
  (cond
    (nil? v) nil
    (and (bytes? v) (Load/isLoadMarker v)) (Load/loadMarker v)
    (and (bytes? v) (pos? (alength ^bytes v))
         (or (= EntryEnvelope/TAG_UNTAGGED (aget ^bytes v 0))
             (= EntryEnvelope/TAG_TAGGED   (aget ^bytes v 0))))
    (EntryMeta/unwrap (EntryEnvelope/readEnvelope v))
    :else v))

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
          marker (Load/newLoadMarker)
          _ (util/add-entry :x2 marker)
          [p ret redis-val load] (start-load l :x2)]
      (is (= nil (.getLoadMarker load)))
      (is (instance? UUID redis-val))
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
    (let [load-marker (Load/newLoadMarker)
          [present? v validation-epoch] (fetch "A" load-marker 1000000 10000)]
      (is (not present?))
      (is (= load-marker (decode-fetch-v v)))
      (is (= 0 validation-epoch))
      (is (= load-marker (util/get-entry "A")))))
  (testing "load marker captures current Redis epoch"
    (car/wcar {} (car/set (keys/epoch-key util/test-keygen "") 4))
    (let [load-marker (Load/newLoadMarker)
          [present? v validation-epoch] (fetch "epoch" load-marker 1000000 10000)]
      (is (not present?))
      (is (= load-marker (decode-fetch-v v)))
      (is (= 4 validation-epoch))))
  (testing "load marker is not set if there is a value already"
    (let [_ (util/add-entry "B" (EntryEnvelope/writeEnvelope 1))
          load-marker (Load/newLoadMarker)
          [present? v] (fetch "B" load-marker 1000000 10000)]
      (is present?)
      (is (= 1 (decode-fetch-v v)))
      (is (= 1 (util/get-entry "B")))))
  (testing "load marker fade is respected"
    (let [load-marker (Load/newLoadMarker)
          [present? v] (fetch "C" load-marker 1 10000)]
      (is (not present?))
      (is (= load-marker (decode-fetch-v v)))
      (Thread/sleep 10)
      (is (= nil (util/get-entry "C")))))
  (testing "load marker fade is not renewed on access"
    (let [_ (fetch "D" (Load/newLoadMarker) 1000 10000)
          load-marker (Load/newLoadMarker)
          [present? v] (fetch "D" load-marker 1000 10000)]
      (is present?)
      (is (other-load-marker? (decode-fetch-v v) load-marker))
      (is (other-load-marker? (util/get-entry "D") load-marker))
      (Thread/sleep 500)
      (fetch "D" load-marker 1000 10000)
      (Thread/sleep 750)
      (is (= nil (util/get-entry "D")))))
  (testing "fade is refreshed if normal value is there"
    (let [_ (util/add-entry "E" "ABC")
          _ (car/wcar {} (car/pexpire (util/test-key "E") 1000))
          load-marker (Load/newLoadMarker)
          [present? v] (fetch "E" load-marker 1000 1000)]
      (is present?)
      (is (= "ABC" (decode-fetch-v v)))
      (Thread/sleep 500)
      (fetch "E" load-marker 1000 1000)
      (Thread/sleep 750)
      (is (= "ABC" (util/get-entry "E")))))
  (testing "fetch missing key without putting a load marker"
    (let [load-marker (Load/newLoadMarker)
          [present? v] (fetch "missing" load-marker -1 nil)]
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
      (is (= true (EntryMeta/unwrap (.await p :a))))
      (is (= {{} {}} (.getMaint l))))))

(deftest stale-maintenance-result-does-not-deliver-to-replaced-load-test
  (let [l (create-loader :a false :none)
        loads (-> (.getMaint l) (.get {}))
        k (util/test-key :a)
        old-load (.get loads k)
        new-load (Load. 0)
        _ (.foreignLoad new-load)
        old-p (.getPromise old-load)
        new-p (.getPromise new-load)]
    (util/add-entry :a true)
    (is (.remove loads k old-load))
    (.invalidate old-p)
    (.releaseResult old-p)
    (.put loads k new-load)
    (doseq [[k load delivery] (loader/cached-entries {} [[k old-load]])]
      (when-not (= delivery :load-marker)
        (when (.remove loads k load)
          (let [p (.getPromise ^Load load)]
            (.deliver p delivery (#'loader/latest-tag-invalidation delivery))
            (.releaseResult p)))))
    (is (= b/absent (.getNow old-p)))
    (is (= b/absent (.getNow new-p)))
    (is (= new-load (.get loads k)))))

(deftest invalidate-by-pred-removes-and-releases-load-test
  (let [l (create-loader :a false :none)
        loads (-> (.getMaint l) (.get {}))
        k (util/test-key :a)
        p (-> loads (.get k) (.getPromise))]
    (.invalidateByPred l {} #(= % k))
    (is (= b/absent (.getNow p)))
    (is (nil? (.get loads k)))))

(deftest same-jvm-late-joiner-coalesces-onto-our-load-test
  ;; Regression for: loads.remove(key, newLoad) is in the local-load `finally`,
  ;; after p.deliver. A same-JVM caller that arrives during the load must attach
  ;; to the existing Load (same identity) and receive the published value via
  ;; SpecialPromise.await, rather than starting a second load.
  (let [l (create-loader)
        k :coalesce-key
        gate (promise)
        s (Segment. (fn [] (deref gate 10000 :timeout) "VAL") nil "" {})
        loader-fut (future (.get l {} s nil (util/test-key k)))
        _ (Thread/sleep 200)
        first-load (load-for l k)
        joiner-fut (future (.get l {} s nil (util/test-key k)))
        _ (Thread/sleep 200)
        joiner-load (load-for l k)]
    ;; While the user fn is blocked, the joiner must observe the same Load
    ;; instance — proving putIfAbsent coalesced it onto our promise.
    (is (some? first-load))
    (is (identical? first-load joiner-load))
    (deliver gate :go)
    (is (= "VAL" (deref loader-fut 5000 :loader-timeout)))
    (is (= "VAL" (deref joiner-fut 5000 :joiner-timeout)))))

