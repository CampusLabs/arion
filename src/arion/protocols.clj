(ns arion.protocols)

(defprotocol Broadcast
  (send! [producer topic key partition message]))

(defprotocol Partition
  (key->partition [producer topic key])
  (partitions [producer]))

(defprotocol Queue
  (put! [queue queue-name message] [queue queue-name message id])
  (put-and-complete! [queue queue-name message])
  (take! [queue queue-name]))

(defprotocol Complete
  (complete! [message metadata])
  (fail! [message error]))

(defprotocol Measurable
  (metrics [subject]))

(defprotocol MetricRegistry
  (get-registry [metrics]))
