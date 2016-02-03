FROM quay.io/orgsync/clojure:2.5.3
WORKDIR /code
ADD . /code/

RUN lein uberjar \
    && mkdir /opt/arion \
    && mv /code/target/arion.jar /opt/arion/arion.jar \
    && rm -Rf /code \
    && rm -Rf /root/.m2

WORKDIR /opt/arion

ENV HEAP_SIZE 200m
ENV ARION_PORT 80
ENV ARION_QUEUE_PATH /var/arion
ENV KAFKA_BOOTSTRAP localhost:9092
ENV STATSD_HOST localhost
ENV STATSD_PORT 8125

EXPOSE 80
VOLUME [ "/var/arion" ]

CMD exec java -Xmx${HEAP_SIZE} -jar arion.jar
