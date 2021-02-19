package no.nav.arbeidsplassen.internalad.indexer.index

import org.elasticsearch.rest.RestStatus
import java.time.LocalDateTime

data class IndexResponse(val hasFailures: Boolean,
                         val status: RestStatus,
                         val numItems: Int,
                         val failureMessage: String)

interface AdIndexer {
    fun index(ads: List<AdTransport>): IndexResponse
    fun createIndex(indexName: String): Boolean
    fun fetchLastUpdatedTimeForIndex(indexName: String): LocalDateTime
}

