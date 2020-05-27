package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Factory
import no.nav.pam.yrkeskategorimapper.StyrkCodeConverter
import org.elasticsearch.client.RestHighLevelClient
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import javax.inject.Singleton

@Factory
class IndexerFactory(val highLevelClient: RestHighLevelClient, val objectMapper: ObjectMapper) {

    @Singleton
    fun lockProvider(): ElasticsearchLockProvider {
        return ElasticsearchLockProvider(highLevelClient=highLevelClient, objectMapper = objectMapper);
    }

    @Singleton
    fun styrkCodeConverter(): StyrkCodeConverter {
        return StyrkCodeConverter()
    }
}

const val INTERNALAD = "internalad"
val datePattern: DateTimeFormatter = DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss")

fun internalAdIndexWithTimestamp(): String {
    return INTERNALAD + LocalDateTime.now().format(datePattern)
}
