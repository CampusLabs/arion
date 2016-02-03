(ns arion.api.health
  (:require [arion.api.routes :as r]))

(defmethod r/dispatch-route :health [_ _ _]
  {:status 200 :body {:status :ok}})
