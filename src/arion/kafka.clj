(ns arion.kafka
  (:require [arion.protocols :as p]
            [clojure.walk :refer [stringify-keys]]
            [com.stuartsierra.component :as component]
            [manifold.deferred :as d]
            [taoensso.timbre :refer [info warn]])
  (:import [java.util
            Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry
            HashMap]
           java.util.concurrent.TimeUnit
           [org.apache.kafka.clients.producer
            Callback KafkaProducer ProducerRecord]
           [org.apache.kafka.common Metric MetricName]
           [org.apache.kafka.common.serialization
            ByteArraySerializer StringSerializer]))

(defn metric->name
  [^Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry m]
  (keyword (.name ^MetricName (.getKey m))))

(defn metric->value
  [^Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry m]
  (.value ^Metric (.getValue m)))

(defn producer-callback [response]
  (reify Callback
    (onCompletion [_ metadata exception]
      (if exception
        (d/error! response exception)
        (d/success! response
                    {:topic     (.topic metadata)
                     :partition (.partition metadata)
                     :offset    (.offset metadata)})))))

(defn initialize-producer [config]
  (if-let [producer (try (KafkaProducer. ^HashMap config)
                         (catch Exception e
                           (warn "error initializing Kafka producer:"
                                 (.getMessage e))))]
    producer
    (do (Thread/sleep 5000)
        (info "retrying")
        (recur config))))

(defrecord Producer [config metrics producer closed?]
  component/Lifecycle
  (start [component]
    (info "starting kafka producer")
    (assoc component
      :producer (initialize-producer config)
      :closed? (atom false)))

  (stop [component]
    (info "closing kafka producer; waiting for queued tasks to complete")
    (reset! closed? true)
    (.close producer 10 TimeUnit/SECONDS)
    (info "kafka producer closed")
    (assoc component :producer nil))

  p/Broadcast
  (send! [_ topic key partition message]
    (let [response (d/deferred)
          record   (ProducerRecord. topic partition key message)]
      (.send producer record (producer-callback response))
      response))

  p/Partition
  (key->partition [_ topic key]
    (d/loop []
      (when (not @closed?)
        (-> (d/future
              (let [partitions (.partitionsFor producer topic)]
                (-> (if key
                      (nth partitions (mod (.hashCode key) (.size partitions)))
                      (rand-nth partitions))
                    (.partition))))
            (d/timeout! 30000)
            (d/catch'
              (fn [e]
                (warn "Unable find partitions for topic" topic
                      "-" (.getMessage e))
                (d/recur)))))))

  p/Measurable
  (metrics [_]
    (into {} (map (juxt metric->name metric->value)
                  (.metrics producer)))))

(defn new-producer [brokers max-message-size]
  (Producer. (-> {:bootstrap.servers    brokers
                  :key.serializer       StringSerializer
                  :value.serializer     ByteArraySerializer
                  :acks                 "all"
                  :compression.type     "gzip"
                  :retries              (int 2147483647)
                  :linger.ms            (int 5)
                  :block.on.buffer.full true
                  :max.request.size     (int (+ max-message-size 128))}
                 stringify-keys)
             nil nil nil))
