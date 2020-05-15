FROM navikt/java:11
COPY build/libs/internalad-indexer-*-all.jar ./app.jar
ENV JAVA_OPTS="-Xms256m -Xmx1024m"
