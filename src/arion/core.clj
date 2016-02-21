(ns arion.core
  (:gen-class)
  (:require [arion
             [api :refer [new-api]]
             [broadcast :refer [new-broadcaster]]
             [gauge-reporter :refer [new-gauge-reporter]]
             [kafka :refer [new-producer]]
             [metrics :refer [new-metrics]]
             [partitioner :refer [new-partitioner]]
             [protocols :as p]
             [queue :refer [new-durable-queue]]]
            [com.stuartsierra.component :as component]
            [environ.core :refer [env]]
            [metrics.meters :as meter])
  (:import [java.lang Runnable Runtime Thread]))

(defn create-system []
  (-> (component/system-map
        :api (new-api (Integer. ^String (env :arion-port "80"))
                      (Integer. ^String (env :arion-idle-timeout "15")))
        :broadcaster (new-broadcaster)
        :gauge-reporter (new-gauge-reporter)
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
         :gauge-reporter [:metrics :queue :producer]
         :partitioner [:metrics :producer :queue]
         :producer [:metrics]
         :queue [:metrics]})))

(defn -main [& _]
  (let [{:keys [metrics] :as s} (-> (create-system)
                                    component/start)
        registry    (p/get-registry metrics)
        make-meter  #(meter/meter registry ["arion" %])
        start-meter (make-meter "start")
        stop-meter  (make-meter "stop")]
    (meter/mark! start-meter)
    (.addShutdownHook (Runtime/getRuntime)
                      (Thread. ^Runnable (fn []
                                           (meter/mark! stop-meter)
                                           (component/stop s))))))
