(ns arion.api.validation
  (:require [gloss.core :as g])
  (:import java.net.URLDecoder))

(def topic-pattern #"(?!^\.{1,2}$)^[a-zA-Z0-9\._\-]{1,255}$")

(defn validate-topic [topic]
  (try
    (let [topic (URLDecoder/decode topic)]
      (if-not (re-find topic-pattern topic)
        (throw (Exception. "malformed topic"))
        topic))
    (catch Exception _
      (throw (ex-info "malformed topic"
                      {:status 400 :body {:status :error
                                          :error  "malformed topic"}})))))

(defn validate-key [k]
  (try
    (URLDecoder/decode k)
    (catch Exception _
      (throw (ex-info "malformed key"
                      {:status 400 :body {:status :error
                                          :error  "malformed key"}})))))

(defn validate-body [body max-message-size]
  (when (> (g/byte-count body) max-message-size)
    (throw
      (ex-info "message too large"
               {:status 400 :body {:status :error
                                   :error  "message too large"}})))
  body)
