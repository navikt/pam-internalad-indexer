package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Put
import io.micronaut.http.annotation.QueryValue
import java.time.LocalDateTime

@Controller("/internal/")
class IndexerController(val indexerService: IndexerService) {

    @Post("/reindex")
    fun reindex(@QueryValue indexName: String?,
                @QueryValue from: LocalDateTime?): IndexerResponse {
        val nameOrDefault = indexName ?: internalAdIndexWithTimestamp()
        val fromOrDefault = from ?: indexerService.getDefaultStartTime()
        val updatedSince = if (indexerService.createIndex(nameOrDefault)) fromOrDefault else indexerService.fetchLastUpdatedTimeForIndex(nameOrDefault)
        return IndexerResponse(nameOrDefault, indexerService.fetchFeedIndexAdsUntilNow(updatedSince, nameOrDefault))
    }

    @Put("/aliases")
    fun updateAliases(@QueryValue indexName: String): Boolean {
        return indexerService.updateAlias(indexName)
    }
}

data class IndexerResponse(val indexName: String, val lastUpdated: LocalDateTime)