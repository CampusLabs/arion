(ns arion.api.routes)

(defmulti dispatch-route
          (fn [{:keys [handler] :as _request} _queue _producer
               _max-message-size _metrics]
            handler))

(defmethod dispatch-route :default [_ _ _ _ _]
  {:status 400 :body {:error "bad request"}})

(def mode-body [[["/" [#".+" :key]] :broadcast]
                      [true :broadcast]])

(def routes
  ["/" [["stats" {:get :stats}]
        ["health-check" {:get :health :head :health}]
        [[:mode "/" [#"[^\/]+" :topic]] {:post mode-body
                                         :get  mode-body}]]])
