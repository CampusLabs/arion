(defproject arion "0.1.0-SNAPSHOT"
  :description "Talks to Kafka so you don't have to"
  :url "https://github.com/orgsync/arion"
  :license {:name "Copyright 2016 OrgSync."}
  :dependencies [[aleph "0.4.1-beta3"]
                 [bidi "1.25.0"]
                 [byte-streams "0.2.1-alpha1"]
                 [com.basistech/metrics-statsd "3.0.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/timbre "4.2.1"]
                 [danlentz/clj-uuid "0.1.6"]
                 [environ "1.0.2"]
                 [factual/durable-queue "0.1.5"]
                 [manifold "0.1.1"]
                 [metrics-clojure "2.6.1"]
                 [org.apache.kafka/kafka-clients "0.9.0.0"]
                 [org.clojure/clojure "1.8.0"]
                 [pjson "0.3.1"]]
  :main arion.core
  :uberjar-name "arion.jar"
  :profiles {:dev     {:dependencies [[criterium "0.4.3"]]}
             :uberjar {:jvm-opts ["-Dclojure.compiler.direct-linking=true"]
                       :aot :all}})
