(ns arion.api
  (:require [aleph
             [http :as http]
             [netty :as netty]]
            [arion.api
             [broadcast]
             [health]
             [routes :as r]
             [stats]]
            [arion.protocols :as p]
            [bidi.bidi :refer [match-route*]]
            [byte-streams :as bs]
            [com.stuartsierra.component :as component]
            [manifold.deferred :as d]
            [metrics
             [counters :as counter]
             [meters :as meter]
             [timers :as timer]]
            [pjson.core :as json]
            [taoensso.timbre :refer [info warn]])
  (:import clojure.lang.ExceptionInfo
           [io.netty.channel Channel ChannelDuplexHandler ChannelPipeline]
           [io.netty.handler.timeout IdleState IdleStateEvent IdleStateHandler]
           io.netty.util.AttributeKey
           java.io.Closeable
           [java.net URI URISyntaxException]))

(defn validate-url [{:keys [uri] :as req}]
  (try
    (URI. uri)
    req

    (catch URISyntaxException e
      (throw (ex-info "malformed URL"
                      {:status 400
                       :body   {:status  :error
                                :error   "malformed URL"
                                :details (.getMessage e)}})))))

(defn parse-route [{:keys [uri] :as req}]
  (match-route* r/routes uri req))

(defn add-close-deferred [req]
  (assoc req :closed (-> req ^Channel .ch .closeFuture netty/wrap-future)))

(defn format-response [{:keys [request-method] :as res}]
  (if (= request-method :head)
    (assoc res :body nil)
    (-> res
        (update :headers assoc :content-type "application/json")
        (update :body json/write-str))))

(defn make-handler
  [queue producer {:keys [success client-error server-error] :as metrics}]

  (let [decode-body  #(and % (bs/to-byte-array %))
        dispatch     #(r/dispatch-route % queue producer metrics)
        mark-success #(do (meter/mark! success) %)]

    (fn [req]
      (d/chain'
        (-> (d/chain' (update req :body decode-body)
              validate-url
              parse-route
              add-close-deferred
              dispatch
              mark-success)

            (d/catch' ExceptionInfo
              (fn [e]
                (warn "request failed with:" (.getMessage e))
                (let [{:keys [status] :as response} (ex-data e)]
                  (if (<= 400 status 499)
                    (meter/mark! client-error)
                    (meter/mark! server-error))
                  response)))

            (d/catch'
              (fn [e]
                (warn "request failed due to uncaught exception:" e)
                (meter/mark! server-error)
                {:status 500
                 :body   {:status  :error
                          :message "server encountered an error processing request"
                          :details (.getMessage e)}})))

        format-response))))

(defn ^ChannelDuplexHandler idle-handler [idle-meter]
  (proxy [ChannelDuplexHandler] []
    (userEventTriggered [ctx e]
      (when (and (instance? IdleStateEvent e)
                 (= (.state e) (IdleState/ALL_IDLE)))
        (meter/mark! idle-meter)
        (.close ctx)))))

(def connection-timer-key (AttributeKey/valueOf "connection_duration"))

(defn ^ChannelDuplexHandler connection-metrics [conn-count conn-timer]
  (proxy [ChannelDuplexHandler] []
    (channelActive [ctx]
      (let [attr (.attr ctx connection-timer-key)]
        (.set attr (timer/start conn-timer))
        (counter/inc! conn-count)))
    (channelInactive [ctx]
      (let [attr (.attr ctx connection-timer-key)]
        (counter/dec! conn-count)
        (timer/stop (.get attr))))))

(defrecord Api [port timeout metrics producer queue server]
  component/Lifecycle
  (start [component]
    (info "starting http server")
    (let [registry     (p/get-registry metrics)
          prefix       ["arion" "api"]
          make-timer   #(timer/timer registry (conj prefix %))
          make-meter   #(meter/meter registry (conj prefix %))
          make-counter #(counter/counter registry (conj prefix %))

          mreg         {:sync-timer     (make-timer "sync_put_time")
                        :async-timer    (make-timer "async_put_time")
                        :success        (make-meter "success")
                        :client-error   (make-meter "client_error")
                        :server-error   (make-meter "server_error")}

          idle-meter   (make-meter "idle_close")
          conn-count   (make-counter "connections")
          conn-timer   (make-timer "connection_duration")

          pipeline-xf  (fn [^ChannelPipeline pipeline]
                         (doto pipeline
                           (.addFirst "connection-metrics" (connection-metrics conn-count conn-timer))
                           (.addLast "idle-state" (IdleStateHandler. 0 0 timeout))
                           (.addLast "idle-handler" (idle-handler idle-meter))))

          handler     (make-handler queue producer mreg)]

      (assoc component
        :server (http/start-server
                  handler {:port port
                           :pipeline-transform pipeline-xf}))))

  (stop [component]
    (info "stopping http server")
    (.close ^Closeable server)
    (assoc component :server nil)))

(defn new-api [port timeout]
  (Api. port timeout nil nil nil nil))
