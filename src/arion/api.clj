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
            [pjson.core :as json]
            [taoensso.timbre :refer [info warn]])
  (:import clojure.lang.ExceptionInfo
           io.netty.channel.Channel
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

(defn route-parser [{:keys [uri] :as req}]
  (match-route* r/routes uri req))

(defn add-close-deferred [req]
  (assoc req :closed (-> req ^Channel .ch .closeFuture netty/wrap-future)))

(defn make-handler
  [queue producer {:keys [success client-error server-error] :as metrics}]
  (fn [{:keys [request-method] :as req}]
    (d/chain'
      (-> (d/chain' (update req :body #(and % (bs/to-byte-array %)))
            validate-url
            route-parser
            #(add-close-deferred %)
            #(r/dispatch-route % queue producer metrics)
            #(do (meter/mark! success) %))

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

      (fn [res]
        (if (= request-method :head)
          (assoc res :body nil)
          (-> res
              (update :headers assoc :content-type "application/json")
              (update :body json/write-str)))))))

(defrecord Api [port metrics producer queue server]
  component/Lifecycle
  (start [component]
    (info "starting http server")
    (let [registry   (p/get-registry metrics)
          prefix     ["arion" "api"]
          make-timer #(timer/timer registry (conj prefix %))
          make-meter #(meter/meter registry (conj prefix %))
          mreg       {:sync-timer     (make-timer "sync_put_time")
                      :async-timer    (make-timer "async_put_time")
                      :accepted       (make-meter "accepted")
                      :created        (make-meter "created")
                      :success        (make-meter "success")
                      :client-error   (make-meter "client_error")
                      :server-error   (make-meter "server_error")}
          handler (make-handler queue producer mreg)]
      (assoc component :server (http/start-server handler {:port port}))))

  (stop [component]
    (info "stopping http server")
    (.close ^Closeable server)
    (assoc component :server nil)))

(defn new-api [port]
  (Api. port nil nil nil nil))
