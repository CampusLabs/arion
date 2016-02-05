(ns arion.queue
  (:require [arion.protocols :as p]
            [clj-uuid :as uuid]
            [com.stuartsierra.component :as component]
            [durable-queue :as q]
            [manifold.deferred :as d]
            [taoensso.timbre :refer [error info]])
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

(defrecord DurableQueue [path metrics durable-queue enqueued closing?]
  component/Lifecycle
  (start [component]
    (info "opening durable queue")
    (assoc component
      :durable-queue (q/queues path {:fsync-take? true})
      :enqueued (atom {})
      :closing? (atom false)))

  (stop [component]
    (reset! closing? true)

    (info "closing durable queue")
    (.close ^Closeable durable-queue)

    (assoc component
      :durable-queue nil
      :enqueued nil
      :closing nil))

  p/Queue
  (put! [queue queue-name message]
    (p/put! queue queue-name message (uuid/v1)))

  (put! [_ queue-name message id]
    (let [entry {:id id :message message}]
      (when (or @closing? (not (q/put! durable-queue queue-name entry)))
        (throw (ex-info "unable to enqueue message"
                        {:status 503
                         :body   {:status :error
                                  :error  "unable to enqueue message"}})))
      id))

  (put-and-complete! [queue queue-name message]
    (let [id (uuid/v1)
          completed (d/deferred)
          _ (swap! enqueued assoc id completed)
          id (p/put! queue queue-name message id)
          untrack (fn [_] (swap! enqueued dissoc id))]
      (d/on-realized completed untrack untrack)
      completed))

  (take! [_ queue-name]
    (let [entry (q/take! durable-queue queue-name)
          {:keys [id message]} @entry
          completed (get @enqueued id)
          complete-fn (fn [metadata]
                        (when completed (d/success! completed metadata))
                        (q/complete! entry))
          fail-fn (fn [error]
                    (when completed (d/error! completed error))
                    (q/complete! entry))]
      (Message. message complete-fn fail-fn)))

  p/Measurable
  (metrics [_] (q/stats durable-queue)))

(defn new-durable-queue [path]
  (DurableQueue. path nil nil nil nil))
