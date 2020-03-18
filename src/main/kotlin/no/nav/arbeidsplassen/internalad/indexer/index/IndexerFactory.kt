package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Factory
import net.javacrumbs.shedlock.core.LockProvider
import org.elasticsearch.client.RestHighLevelClient
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.inject.Singleton

@Factory
class IndexerFactory(val highLevelClient: RestHighLevelClient, val objectMapper: ObjectMapper) {

    @Singleton
    fun lockProvider(): LockProvider {
        return ElasticsearchLockProvider(highLevelClient=highLevelClient, objectMapper = objectMapper);
    }
}

const val INTERNALAD = "internalad"
val datePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss")

fun internalAdIndexWithTimestamp(): String {
    return INTERNALAD +LocalDateTime.now().format(datePattern)
}

