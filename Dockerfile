FROM navikt/java:11

COPY scripts/init-pam-env.sh /init-scripts/init-pam-env.sh
COPY build/libs/internalad-indexer-*-all.jar ./app.jar
ENV JAVA_OPTS="-Xms256m -Xmx1024m"
