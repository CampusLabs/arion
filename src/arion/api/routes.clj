(ns arion.api.routes)

(defmulti dispatch-route (fn [{:keys [handler]} _ _] handler))

(defmethod dispatch-route :default [_ _ _]
  {:status 400 :body {:error "bad request"}})

(def routes
  ["/" [["stats" {:get :stats}]
        ["health-check" {:get :health :head :health}]
        [[:mode "/" [#"[^\/]+" :topic]] {:post [[["/" [#".+" :key]] :broadcast]
                                                [true :broadcast]]}]]])
