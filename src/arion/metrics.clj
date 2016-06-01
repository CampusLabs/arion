(ns arion.metrics
  (:require [arion.protocols :as p]
            [com.stuartsierra.component :as component]
            [metrics.core :refer [new-registry]]
            [metrics-statsd.core :refer [new-statsd-reporter]]
            [taoensso.timbre :refer [info]])
  (:import java.util.concurrent.TimeUnit
           metrics_statsd.reporter.StatsDReporter))

(defrecord Metrics [host port registry reporter]
  component/Lifecycle
  (start [component]
    (let [registry (new-registry)
          reporter (new-statsd-reporter registry {:host host :port port})]
      (info "starting statsd metrics reporter")
      (.start ^StatsDReporter reporter 10 TimeUnit/SECONDS)
      (assoc component :registry registry :reporter reporter)))

  (stop [component]
    (info "stopping statsd metrics reporter")
    (.stop reporter)
    (assoc component :registry nil :reporter nil))

  p/MetricRegistry
  (get-registry [_] registry))

(defn new-metrics [host port]
  (Metrics. host port nil nil))
