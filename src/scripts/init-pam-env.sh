export SERVICEUSER=$(cat /secrets/paminternaladindexer/serviceuser/credentials/username)
export SERVICEUSER_PASSWORD=$(cat /secrets/paminternaladindexer/serviceuser/credentials/password)
export KAFKA_SSL_TRUSTSTORE_LOCATION=${NAV_TRUSTSTORE_PATH}
export KAFKA_SSL_TRUSTSTORE_PASSWORD=${NAV_TRUSTSTORE_PASSWORD}
export KAFKA_SASL_JAAS_CONFIG="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${SERVICEUSER}\" password=\"${SERVICEUSER_PASSWORD}\";"
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
