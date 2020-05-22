package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.elasticsearch.rest.RestStatus
import javax.inject.Singleton

data class IndexResponse(val hasFailures: Boolean,
                         val status: RestStatus,
                         val numItems: Int,
                         val failureMessage: String)

interface AdIndexer {
    fun index(ads :List<AdTransport>): IndexResponse
}

@Factory
class AdIndexerFactory {
    @Bean
    @Singleton
    fun adIndexer(indexerService: IndexerService): AdIndexer {
        return ElasticIndexer(indexerService)
    }
}

class ElasticIndexer(private val indexerService: IndexerService) : AdIndexer {
    override fun index(ads: List<AdTransport>): IndexResponse {
        val bulkResponse = indexerService.bulkIndex(ads);
        return IndexResponse(bulkResponse.hasFailures(),
                bulkResponse.status(),
                bulkResponse.items.size,
                bulkResponse.buildFailureMessage())
    }
}

