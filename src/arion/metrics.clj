(ns arion.metrics
  (:require [com.stuartsierra.component :as component]
            [arion.protocols :as p]
            [metrics.core :refer [new-registry]]
            [taoensso.timbre :refer [info error warn]])
  (:import [com.basistech.metrics.reporting Statsd StatsdReporter]
           [java.util.concurrent TimeUnit]))

(defrecord Metrics [host port registry reporter]
  component/Lifecycle
  (start [component]
    (let [statsd (Statsd. host port)
          registry (new-registry)
          reporter (-> (StatsdReporter/forRegistry registry)
                       (.convertDurationsTo TimeUnit/MILLISECONDS)
                       (.convertRatesTo TimeUnit/SECONDS)
                       (.build statsd))]
      (info "starting statsd metrics reporter")
      (.start reporter 5 TimeUnit/SECONDS)
      (assoc component :registry registry :reporter reporter)))

  (stop [component]
    (info "stopping statsd metrics reporter")
    (.stop reporter)
    (assoc component :registry nil :reporter nil))

  p/MetricRegistry
  (get-registry [_] registry))

(defn new-metrics [host port]
  (Metrics. host port nil nil))
