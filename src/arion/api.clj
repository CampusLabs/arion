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
            [metrics.timers :as timer]
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

(defn add-close-deferred [req timer-context]
  (let [closed (-> req ^Channel .ch .closeFuture netty/wrap-future)
        close-metrics (fn [_] (timer/stop timer-context))]
    (d/on-realized closed close-metrics close-metrics)
    (assoc req :closed closed)))

(defn make-handler [queue producer response-timer]
  (fn [{:keys [request-method] :as req}]
    (let [timer-context (timer/start response-timer)]
      (d/chain'
        (-> (d/chain' (update req :body #(and % (bs/to-byte-array %)))
              validate-url
              route-parser
              #(add-close-deferred % timer-context)
              #(r/dispatch-route % queue producer))

            (d/catch' ExceptionInfo
              (fn [e]
                (warn "request failed with:" (.getMessage e))
                (ex-data e)))

            (d/catch'
              (fn [e]
                (warn "request failed due to uncaught exception:" e)
                {:status 500
                 :body   {:status  :error
                          :message "server encountered an error processing request"
                          :details (.getMessage e)}})))

        (fn [res]
          (if (= request-method :head)
            (assoc res :body nil)
            (-> res
                (update :headers assoc :content-type "application/json")
                (update :body json/write-str))))))))

(defrecord Api [port metrics producer queue server]
  component/Lifecycle
  (start [component]
    (info "starting http server")
    (let [registry       (p/get-registry metrics)
          prefix         ["arion" "api"]
          response-timer (timer/timer registry (conj prefix "response_time"))
          handler (make-handler queue producer response-timer)]
      (assoc component :server (http/start-server handler {:port port}))))

  (stop [component]
    (info "stopping http server")
    (.close ^Closeable server)
    (assoc component :server nil)))

(defn new-api [port]
  (Api. port nil nil nil nil))
