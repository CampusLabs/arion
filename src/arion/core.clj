(ns arion.core
  (:gen-class)
  (:require [arion
             [api :refer [new-api]]
             [broadcast :refer [new-broadcaster]]
             [kafka :refer [new-producer]]
             [metrics :refer [new-metrics]]
             [partitioner :refer [new-partitioner]]
             [queue :refer [new-durable-queue]]]
            [com.stuartsierra.component :as component]
            [environ.core :refer [env]])
  (:import [java.lang Runnable Runtime Thread]))

(defn create-system []
  (-> (component/system-map
        :api (new-api (Integer. ^String (env :arion-port "80")))
        :broadcaster (new-broadcaster)
        :metrics (new-metrics
                   (env :statsd-host "localhost")
                   (Integer. ^String (env :statsd-port "8125")))
        :partitioner (new-partitioner)
        :producer (new-producer (env :kafka-bootstrap "localhost:9092"))
        :queue (new-durable-queue
                 (env :arion-queue-path "/var/arion")))

      (component/system-using
        {:api [:metrics :producer :queue]
         :broadcaster [:metrics :partitioner :producer]
         :partitioner [:metrics :producer :queue]
         :producer [:metrics]
         :queue [:metrics]})))

(defn -main [& _]
  (let [s (-> (create-system)
              component/start)]
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable #(component/stop s)))))
