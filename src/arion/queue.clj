(ns arion.queue
  (:require [arion.protocols :as p]
            [clj-uuid :as uuid]
            [com.stuartsierra.component :as component]
            [durable-queue :as q]
            [manifold.deferred :as d]
            [taoensso.timbre :refer [error info]]
            [manifold.stream :as s])
  (:import clojure.lang.IDeref
           java.io.Closeable))

(defrecord Message [payload complete-fn fail-fn]
  IDeref
  (deref [_] payload)

  p/Complete
  (complete! [_ metadata]
    (complete-fn metadata))

  (fail! [_ error]
    (fail-fn error)))

(defmethod print-method Message [s w]
  ((get-method print-method IDeref) s w))

(defn process-request-stream [request-stream durable-queue closing?]
  (-> (Thread.
        ^Runnable
        (fn []
          (let [{:keys [queue-name entry enqueued?]} @(s/take! request-stream)]
            (when (and entry (not @closing?))
              (if (q/put! durable-queue queue-name entry)
                (d/success! enqueued? true)
                (d/error! enqueued?
                          (ex-info "unable to enqueue message"
                                   {:status 503
                                    :body   {:status :error
                                             :error  "unable to enqueue message"}})))
              (recur))))
        "durable-queue-ingest")
      (.start)))

(defn put-request [request-stream queue-name entry]
  (let [enqueued? (d/deferred)]
    (s/put! request-stream {:queue-name queue-name
                            :entry      entry
                            :enqueued?  enqueued?})
    enqueued?))

(defrecord DurableQueue [path options metrics durable-queue enqueued closing?
                         request-stream]
  component/Lifecycle
  (start [component]
    (let [durable-queue  (q/queues path options)
          enqueued       (atom {})
          closing?       (atom false)
          request-stream (s/stream 1024)]

      (process-request-stream request-stream durable-queue closing?)

      (info "opening durable queue")
      (assoc component
        :durable-queue durable-queue
        :enqueued enqueued
        :closing? closing?
        :request-stream request-stream)))

  (stop [component]
    (reset! closing? true)

    (info "closing durable queue")
    (.close ^Closeable durable-queue)

    (assoc component
      :durable-queue nil
      :enqueued nil
      :closing nil
      :request-stream nil))

  p/Queue
  (put! [queue queue-name message]
    (p/put! queue queue-name message (uuid/v1)))

  (put! [_ queue-name message id]
    (let [entry {:id id :message message}]
      (put-request request-stream queue-name entry)))

  (put-and-complete! [queue queue-name message]
    (let [completed (d/deferred)
          id        (uuid/v1)
          untrack   (fn [_] (swap! enqueued dissoc id))]
      (swap! enqueued assoc id completed)
      (d/on-realized completed untrack untrack)
      (p/put! queue queue-name message id)
      completed))

  (take! [_ queue-name]
    (let [entry       (q/take! durable-queue queue-name)
          {:keys [id message]} @entry
          completed   (get @enqueued id)
          complete-fn (fn [metadata]
                        (when completed (d/success! completed metadata))
                        (q/complete! entry))
          fail-fn     (fn [error]
                        (when completed (d/error! completed error))
                        (q/complete! entry))]
      (Message. message complete-fn fail-fn)))

  p/Measurable
  (metrics [_] (q/stats durable-queue)))

(defn new-durable-queue [path options]
  (DurableQueue. path options nil nil nil nil nil))
