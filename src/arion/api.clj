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
             [meters :as meter]
             [timers :as timer]]
            [cheshire
             [core :as json]
             [generate :as gen]]
            [taoensso.timbre :refer [info warn]])
  (:import clojure.lang.ExceptionInfo
           [io.netty.channel Channel ChannelDuplexHandler ChannelPipeline]
           [io.netty.handler.timeout IdleState IdleStateEvent IdleStateHandler]
           java.io.Closeable
           [java.net URI URISyntaxException]
           java.time.Instant))

(gen/add-encoder Instant gen/encode-str)

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
  (if-let [matched (match-route* r/routes uri req)]
    matched
    (throw (ex-info "unknown resource"
                    {:status 404
                     :body   {:status  :error
                              :error   "unknown resource"
                              :details {:uri uri}}}))))

(defn add-close-deferred [req]
  (assoc req :closed (-> req ^Channel .ch .closeFuture netty/wrap-future)))

(defn format-response [{:keys [request-method body] :as res}]
  (if (= request-method :head)
    (assoc res :body nil)
    (if body
      (-> res
          (update :headers assoc :content-type "application/json")
          (update :body json/generate-string))
      res)))

(defn make-handler
  [queue producer max-message-size
   {:keys [success client-error server-error] :as metrics}]

  (let [decode-body  #(and % (bs/to-byte-array %))
        dispatch     #(r/dispatch-route % queue producer max-message-size
                                        metrics)
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

(defn make-rejected-handler [{:keys [server-error]}]
  (fn [_]
    (warn "executor queue full; unable to enqueue request")
    (meter/mark! server-error)
    (-> {:status 503
         :body   {:status  :error
                  :message "too many concurrent requests; backoff and retry"
                  :details "executor queue is full and the request cannot be processed"}}
        format-response)))

(defrecord Api [port timeout max-message-size metrics producer queue server]
  component/Lifecycle
  (start [component]
    (info "starting http server")
    (let [registry    (p/get-registry metrics)
          prefix      ["arion" "api"]
          make-timer  #(timer/timer registry (conj prefix %))
          make-meter  #(meter/meter registry (conj prefix %))

          mreg        {:sync-timer      (make-timer "sync_put_time")
                       :async-timer     (make-timer "async_put_time")
                       :websocket-timer (make-timer "websocket_put_time")
                       :success         (make-meter "success")
                       :client-error    (make-meter "client_error")
                       :server-error    (make-meter "server_error")}

          idle-meter  (make-meter "idle_close")

          pipeline-xf (fn [^ChannelPipeline pipeline]
                        (doto pipeline
                          (.addLast "idle-state" (IdleStateHandler. 0 0 timeout))
                          (.addLast "idle-handler" (idle-handler idle-meter))))

          handler     (make-handler queue producer max-message-size mreg)]

      (assoc component
        :server (http/start-server
                  handler {:port               port
                           :raw-stream?        true
                           :rejected-handler   (make-rejected-handler mreg)
                           :pipeline-transform pipeline-xf}))))

  (stop [component]
    (info "stopping http server")
    (.close ^Closeable server)
    (assoc component :server nil)))

(defn new-api [port timeout max-message-size]
  (Api. port timeout max-message-size nil nil nil nil))
