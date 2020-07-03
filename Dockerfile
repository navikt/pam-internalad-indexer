FROM navikt/java:11
RUN apt-get update && apt-get install -y curl
COPY scripts/init-kafka-env.sh /init-scripts/init-kafka-env.sh
COPY build/libs/internalad-indexer-*-all.jar ./app.jar
ENV JAVA_OPTS="-Xms256m -Xmx1024m"
