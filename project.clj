;; Copyright 2016 OrgSync
;;
;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;;
;; http://www.apache.org/licenses/LICENSE-2.0
;;
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(defproject arion "1.2.0"
  :description "Talks to Kafka so you don't have to"
  :url "https://github.com/orgsync/arion"
  :license {:name "Apache 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"}
  :dependencies [[aleph "0.4.1"]
                 [bidi "2.0.9"]
                 [byte-streams "0.2.2"]
                 [camel-snake-kebab "0.4.0"]
                 [com.stuartsierra/component "0.3.1"]
                 [com.taoensso/timbre "4.3.1"]
                 [danlentz/clj-uuid "0.1.6"]
                 [environ "1.0.2"]
                 [factual/durable-queue "0.1.5"]
                 [gloss "0.2.5"]
                 [manifold "0.1.4"]
                 [metrics-clojure "2.6.1"]
                 [metrics-clojure-jvm "2.6.1"]
                 [metrics-statsd "0.1.2"]
                 [org.apache.kafka/kafka-clients "0.9.0.1"]
                 [org.clojure/clojure "1.8.0"]
                 [pjson "0.3.2"]]
  :main arion.core
  :uberjar-name "arion.jar"
  :repl-options {:host "0.0.0.0"}
  :profiles {:dev     {:dependencies [[criterium "0.4.4"]]}
             :uberjar {:jvm-opts ["-Dclojure.compiler.direct-linking=true"]
                       :aot :all}})
