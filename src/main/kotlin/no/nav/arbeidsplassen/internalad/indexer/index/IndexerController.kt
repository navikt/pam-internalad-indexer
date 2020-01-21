package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.core.convert.format.Format
import io.micronaut.http.annotation.*
import no.nav.arbeidsplassen.internalad.indexer.feed.ElasticsearchFeedRepository
import no.nav.arbeidsplassen.internalad.indexer.feed.FeedTask
import java.time.LocalDateTime

@Controller("/internal")
class IndexerController(val indexerService: IndexerService,
                        val feedTaskRepository: ElasticsearchFeedRepository) {

    @Post("/reindex")
    fun reindex(@QueryValue indexName: String): IndexerResponse {
        val from = indexerService.getDefaultStartTime()
        val updatedSince = if (indexerService.createIndex(indexName)) from else indexerService.fetchLastUpdatedTimeForIndex(indexName)
        return IndexerResponse(indexName, indexerService.fetchFeedIndexAdsUntilNow(updatedSince, indexName))
    }

    @Put("/aliases")
    fun updateAliases(@QueryValue indexName: String): Boolean {
        return indexerService.updateAlias(indexName)
    }

    @Get("/feedtasks")
    fun resetLastRunDate(): List<FeedTask> {
        return feedTaskRepository.findAllFeedTask()
    }

    @Put("/feedtasks/{name}")
    fun setFeedTaskLastRunDate(@PathVariable name: String,
                               @QueryValue @Format("yyyy-MM-dd'T'HH:mm:ss.SSS") lastRun: LocalDateTime): FeedTask? {
        return feedTaskRepository.save(FeedTask(name, lastRun))
    }
}

data class IndexerResponse(val indexName: String, val lastUpdated: LocalDateTime)