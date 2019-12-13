FROM navikt/java:11
COPY build/libs/internalad-indexer-*-all.jar ./app.jar
