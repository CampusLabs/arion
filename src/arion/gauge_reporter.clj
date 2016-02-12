(ns arion.gauge-reporter
  (:require [arion.api.broadcast :refer [queue-name]]
            [arion.protocols :as p]
            [camel-snake-kebab.core :refer [->snake_case]]
            [com.stuartsierra.component :as component]
            [metrics.gauges :as gauge]
            [metrics.jvm.core :as jm]))

(defn format-metric [metric]
  (->snake_case (name metric)))

(defn register-queue-gauges! [registry queue]
  (doseq [metric [:num-slabs :num-active-slabs :enqueued :retried :completed
                  :in-progress]]
    (let [metric-name ["arion" "queue" (format-metric metric)]
          path        [queue-name metric]]
      (gauge/gauge-fn registry metric-name
                      #(get-in (p/metrics queue) path)))))

(defn register-producer-gauges! [registry producer]
  (doseq [metric (keys (p/metrics producer))]
    (let [metric-name ["arion" "kafka_producer" (format-metric metric)]]
      (gauge/gauge-fn registry metric-name
                      #(get (p/metrics producer) metric)))))

(defrecord GaugeReporter [metrics queue producer]
  component/Lifecycle
  (start [component]
    (let [registry (p/get-registry metrics)
          prefix   #(conj ["arion" "jvm"] %)]

      (jm/register-jvm-attribute-gauge-set registry (prefix "attributes"))
      (jm/register-memory-usage-gauge-set registry (prefix "memory"))
      (jm/register-file-descriptor-ratio-gauge-set registry (prefix "fd"))
      (jm/register-garbage-collector-metric-set registry (prefix "gc"))
      (jm/register-thread-state-gauge-set registry (prefix "threads"))

      (register-queue-gauges! registry queue)
      (register-producer-gauges! registry producer))
    component)

  (stop [component]
    component))

(defn new-gauge-reporter []
  (GaugeReporter. nil nil nil))
