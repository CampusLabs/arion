(ns arion.api.broadcast
  (:require [aleph.http :as http]
            [arion.api
             [routes :as r]
             [validation :as v]]
            [arion.protocols :as p]
            [byte-streams :as b]
            [manifold.deferred :as d]
            [metrics.timers :as timer]
            [taoensso.timbre :refer [info]]
            [manifold.stream :as s]
            [cheshire.core :as json])
  (:import java.time.Instant))

(def ^:const queue-name "messages")
(def ^:const max-inflight-requests 1000)

(defn compose-payload [topic key message]
  {:topic    topic
   :key      key
   :enqueued (Instant/now)
   :message  message})

(defn send-sync! [topic key message queue closed sync-timer]
  (let [payload           (compose-payload topic key message)
        timer-context     (timer/start sync-timer)
        response-deferred (p/put-and-complete! queue queue-name payload)]

    (d/chain closed
      (fn [_] (when-not (d/realized? response-deferred)
                (info "client connection closed before response delivered")
                (d/success! response-deferred {:status 201}))))

    (d/chain' response-deferred
      (fn [response]
        (timer/stop timer-context)
        {:status 201 :body (-> {:status :sent :key key}
                               (merge response))}))))

(defn send-async! [topic key message queue async-timer]
  (let [payload       (compose-payload topic key message)
        timer-context (timer/start async-timer)]
    (d/chain' (p/put! queue queue-name payload)
      (fn [id]
        (timer/stop timer-context)
        {:status 202 :body (-> {:status :enqueued}
                               (merge payload)
                               (dissoc :message)
                               (assoc :id id))}))))

(defn enqueue-message [queue topic key message response-stream websocket-timer]
  (let [timer-context (timer/start websocket-timer)
        payload       (compose-payload topic key (b/to-byte-array message))
        response      (p/put-and-complete! queue queue-name payload)]
    (d/let-flow' [success? (s/put! response-stream response)]
      (timer/stop timer-context)
      success?)))

(defn enqueue-messages [socket queue topic key response-stream websocket-timer]
  (d/loop []
    (d/let-flow' [message  (s/take! socket)
                  success? (when message
                             (enqueue-message queue topic key message
                                              response-stream
                                              websocket-timer))]
      (when success? (d/recur)))))

(defn return-responses [socket response-stream]
  (d/loop []
    (d/let-flow' [status   (s/take! response-stream)
                  response (when status (-> (assoc status :status :sent)
                                            json/generate-string))
                  success? (when response (s/put! socket response))]
      (when success? (d/recur)))))

(defn send-websocket! [request topic key queue max-message-size websocket-timer]
  (d/let-flow' [response-stream (s/stream max-inflight-requests)
                socket          (http/websocket-connection
                                  request {:max-frame-size max-message-size
                                           :raw-stream?    true})]
    (-> (d/zip'
          (enqueue-messages socket queue topic key response-stream
                            websocket-timer)
          (return-responses socket response-stream))
        (d/chain' (fn [_] {:status 200}))
        (d/catch'
          (fn [e]
            (s/close! socket)
            (throw
              (ex-info (str "socket connection terminated: " (.getMessage e))
                       {:status 400
                        :body   {:status :error :error (.getMessage e)}})))))))

(defmethod r/dispatch-route :broadcast
  [{{:keys [mode topic key]} :route-params
    :keys                    [body closed]
    :as                      request}
   queue _ max-message-size {:keys [sync-timer async-timer websocket-timer]}]

  (let [topic (v/validate-topic topic)
        key   (when key (v/validate-key key))
        body  (v/validate-body body max-message-size)]

    (case mode
      "sync" (send-sync! topic key body queue closed sync-timer)
      "async" (send-async! topic key body queue async-timer)
      "websocket" (send-websocket! request topic key queue max-message-size
                                   websocket-timer)
      (throw
        (ex-info "unsupported broadcast mode"
                 {:status 400 :body {:status :error
                                     :error  "unsupported broadcast mode"}})))))
