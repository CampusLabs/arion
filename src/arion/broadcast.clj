(ns arion.broadcast
  (:require [arion.protocols :as p]
            [com.stuartsierra.component :as component]
            [manifold
             [deferred :as d]
             [stream :as s]]
            [metrics
             [counters :as counter]
             [meters :as meter]
             [timers :as timer]]
            [taoensso.timbre :refer [info warn error]])
  (:import java.time.Instant
           org.apache.kafka.common.errors.RetriableException))

(defn broadcast-payload
  [payload producer topic partition
   {:keys [broadcast broadcast-time broadcast-error]}]

  (d/loop []
    (let [{:keys [key message]} @payload
          timer-context (timer/start broadcast-time)]
      (meter/mark! broadcast)

      (-> (d/chain' (p/send! producer topic key partition message)
            (fn [response]
              (let [sent (Instant/now)]
                (timer/stop timer-context)
                (p/complete! payload (assoc response :sent sent)))))

          (d/catch' RetriableException
            (fn [e]
              (warn "exception while broadcasting:" (.getMessage e))
              (meter/mark! broadcast-error)
              (d/recur)))

          (d/catch'
            (fn [e]
              (error "fatal exception while broadcasting:" (.getMessage e))
              (meter/mark! broadcast-error)))))))

(defn broadcast-partition
  [{:keys [topic partition stream]} producer {:keys [partitions] :as metrics}]
  (info "broadcasting messages for topic" topic "partition" partition)
  (counter/inc! partitions)

  (d/loop []
    (d/chain' (s/take! stream)
      #(when % (broadcast-payload % producer topic partition metrics))
      #(if %
        (d/recur)
        (do (info "stopped broadcasts for topic" topic "partition" partition)
            (counter/dec! partitions))))))

(defn consume-partitions [partition-stream producer metrics]
  (s/consume
    #(broadcast-partition % producer metrics)
    partition-stream))

(defrecord Broadcaster [metrics partitioner producer]
  component/Lifecycle
  (start [component]
    (let [registry     (p/get-registry metrics)
          prefix       ["arion" "broadcast"]
          make-counter #(counter/counter registry (conj prefix %))
          make-meter   #(meter/meter registry (conj prefix %))
          make-timer   #(timer/timer registry (conj prefix %))
          mreg         {:partitions      (make-counter "partitions")
                        :broadcast-time  (make-timer "time")
                        :broadcast       (make-meter "attempt")
                        :broadcast-error (make-meter "error")}]

      (info "starting broadcaster")
      (consume-partitions (p/partitions partitioner) producer mreg)

      component))

  (stop [component]
    component))

(defn new-broadcaster []
  (Broadcaster. nil nil nil))
