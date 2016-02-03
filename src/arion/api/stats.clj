(ns arion.api.stats
  (:require [arion.api
             [broadcast :refer [queue-name]]
             [routes :as r]]
            [arion.protocols :as p]))

(defmethod r/dispatch-route :stats [_ queue producer]
  {:status 200
   :body {:queue (get (p/metrics queue) queue-name {:status :empty})
          :kafka (p/metrics producer)}})
