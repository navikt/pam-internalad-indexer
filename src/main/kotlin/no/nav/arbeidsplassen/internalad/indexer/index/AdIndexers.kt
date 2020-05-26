package no.nav.arbeidsplassen.internalad.indexer.index

import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.elasticsearch.rest.RestStatus

data class IndexResponse(val hasFailures: Boolean,
                         val status: RestStatus,
                         val numItems: Int,
                         val failureMessage: String)

interface AdIndexer {
    fun index(ads :List<AdTransport>): IndexResponse
}

