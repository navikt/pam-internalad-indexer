FROM navikt/java:12
COPY build/libs/internalad-indexer-*-all.jar ./app.jar
