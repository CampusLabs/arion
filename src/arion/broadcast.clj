(ns arion.broadcast
  (:require [arion.protocols :as p]
            [com.stuartsierra.component :as component]
            [manifold
             [deferred :as d]
             [stream :as s]]
            [taoensso.timbre :refer [info warn]])
  (:import java.time.Instant))

(defn broadcast-payload [payload producer topic partition]
  (d/loop []
    (let [{:keys [key message]} @payload]
      (-> (d/chain' (p/send! producer topic key partition message)
            (fn [response]
              (let [sent (Instant/now)]
                (p/complete! payload (assoc response :sent sent)))))

          (d/catch'
            (fn [e]
              (warn "exception while broadcasting:" (.getMessage e) "- retrying")
              (d/recur)))))))

(defn broadcast-partition [{:keys [topic partition stream]} producer]
  (info "broadcasting messages for topic" topic "partition" partition)
  (d/loop []
    (d/chain' (s/take! stream)
      #(when % (broadcast-payload % producer topic partition))
      #(if %
        (d/recur)
        (info "stopped broadcasts for topic" topic "partition" partition)))))

(defn consume-partitions [partition-stream producer]
  (s/consume
    #(broadcast-partition % producer)
    partition-stream))

(defrecord Broadcaster [metrics partitioner producer]
  component/Lifecycle
  (start [component]
    (info "starting broadcaster")
    (consume-partitions (p/partitions partitioner) producer)
    component)

  (stop [component]
    component))

(defn new-broadcaster []
  (Broadcaster. nil nil nil))
