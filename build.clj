(ns build
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.build.api :as b] ; for b/git-count-revs
            [org.corfield.build :as bb]))

(def lib 'org.clojars.roklenarcic/memento-redis)
(def version (format "0.2.%s" (b/git-count-revs nil)))

(defn add-defaults [opts]
  (assoc opts :lib lib :version version))

(defn test "Run the tests." [opts]
  (bb/run-tests opts))

(defn ci "Run the CI pipeline of tests (and build the JAR)." [opts]
  (-> opts
      add-defaults
      (bb/run-tests)
      (bb/clean)
      (bb/jar)))

(defn install "Install the JAR locally." [opts]
  (-> opts
      add-defaults
      (bb/install)))

(defn deploy "Deploy the JAR to Clojars." [opts]
  (-> opts
      (assoc :sign-releases? true)
      add-defaults
      (bb/deploy)))
