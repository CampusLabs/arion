(ns arion.api.broadcast
  (:require [arion.api.routes :as r]
            [arion.protocols :as p]
            [manifold.deferred :as d]
            [metrics.timers :as timer]
            [taoensso.timbre :refer [info]])
  (:import java.net.URLDecoder
           java.time.Instant))

(def ^:const queue-name "messages")

(def topic-pattern #"(?!^\.{1,2}$)^[a-zA-Z0-9\._\-]{1,255}$")

(defn compose-payload [topic key message]
  {:topic topic
   :key key
   :enqueued (Instant/now)
   :message message})

(defn send-sync! [topic key message queue closed sync-timer]
  (let [payload (compose-payload topic key message)
        timer-context (timer/start sync-timer)
        response-deferred (p/put-and-complete! queue queue-name payload)]

    (d/chain' closed
      (fn [_] (when-not (d/realized? response-deferred)
                (info "client connection closed before response delivered")
                (d/success! response-deferred {:status 201}))))

    (d/chain' response-deferred
      (fn [response]
        (timer/stop timer-context)
        {:status 201 :body (-> {:status :sent :key key}
                               (merge response))}))))

(defn send-async! [topic key message queue async-timer]
  (let [payload (compose-payload topic key message)
        timer-context (timer/start async-timer)
        id (p/put! queue queue-name payload)]
    (timer/stop timer-context)
    {:status 202 :body (-> {:status :enqueued}
                           (merge payload)
                           (dissoc :message)
                           (assoc :id id))}))

(defmethod r/dispatch-route :broadcast
  [{{:keys [mode topic key]} :route-params
    :keys [body closed]}
   queue _ {:keys [sync-timer async-timer]}]

  (let [topic (URLDecoder/decode topic)
        key   (when key (URLDecoder/decode key))]

    (when-not (re-find topic-pattern topic)
      (throw
        (ex-info "malformed topic"
                 {:status 400 :body {:status :error
                                     :error  "malformed topic"}})))

    (case mode
      "sync" (send-sync! topic key body queue closed sync-timer)
      "async" (send-async! topic key body queue async-timer)
      (throw
        (ex-info "unsupported broadcast mode"
                 {:status 400 :body {:status :error
                                     :error  "unsupported broadcast mode"}})))))
