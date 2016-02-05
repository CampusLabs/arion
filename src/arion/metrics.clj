(ns arion.metrics
  (:require [arion.protocols :as p]
            [com.stuartsierra.component :as component]
            [metrics.core :refer [new-registry]]
            [taoensso.timbre :refer [info]])
  (:import [com.basistech.metrics.reporting Statsd StatsdReporter]
           java.util.concurrent.TimeUnit))

(defrecord Metrics [host port registry reporter]
  component/Lifecycle
  (start [component]
    (let [statsd (Statsd. host port)
          registry (new-registry)
          reporter (-> (StatsdReporter/forRegistry registry)
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
