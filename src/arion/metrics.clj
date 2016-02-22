(ns arion.metrics
  (:require [arion.protocols :as p]
            [com.stuartsierra.component :as component]
            [metrics.core :refer [new-registry]]
            [taoensso.timbre :refer [info]])
  (:import com.readytalk.metrics.StatsDReporter
           java.util.concurrent.TimeUnit))

(defrecord Metrics [host port registry reporter]
  component/Lifecycle
  (start [component]
    (info "starting statsd metrics reporter")
    (let [registry (new-registry)
          reporter (-> (StatsDReporter/forRegistry registry)
                       (.build host port))]
      (.start reporter 10 TimeUnit/SECONDS)
      (assoc component :registry registry :reporter reporter)))

  (stop [component]
    (info "stopping statsd metrics reporter")
    (.stop reporter)
    (assoc component :registry nil :reporter nil))

  p/MetricRegistry
  (get-registry [_] registry))

(defn new-metrics [host port]
  (Metrics. host port nil nil))
