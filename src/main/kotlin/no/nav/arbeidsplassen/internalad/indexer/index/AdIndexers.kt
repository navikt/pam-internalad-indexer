package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.context.annotation.Bean
import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.elasticsearch.rest.RestStatus
import javax.inject.Named
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
    fun adIndexer(@Value("\${adlistener.useDummyIndexer:true}") useDummyIndexer: Boolean,
            indexerService: IndexerService): AdIndexer {
        if (useDummyIndexer)
            return DummyIndexer()
        else
            return ElasticIndexer(indexerService)
    }

}

class ElasticIndexer(private val indexerService: IndexerService) : AdIndexer {
    override fun index(ads: List<AdTransport>): IndexResponse {
        val bulkResponse = indexerService.bulkIndex(ads);
        val response = IndexResponse(bulkResponse.hasFailures(),
                bulkResponse.status(),
                bulkResponse.items.size,
                bulkResponse.buildFailureMessage())
        return response
    }
}


open class DummyIndexer(private val failureFactor : Double = 1.0/20) : AdIndexer {
    override fun index(ads: List<AdTransport>): IndexResponse {
        var i : Int = 0;
        if (Math.random() < failureFactor) {
            return IndexResponse(true,
                    RestStatus.BAD_REQUEST,
                    ads.size,
                    "random failure")
        } else {
            return IndexResponse(false,
                    RestStatus.OK,
                    ads.size,
                    "")
        }
    }
}
