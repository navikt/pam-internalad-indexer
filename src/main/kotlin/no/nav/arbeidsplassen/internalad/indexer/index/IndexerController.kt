package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.JsonNode
import io.micronaut.core.convert.format.Format
import io.micronaut.http.annotation.*
import net.javacrumbs.shedlock.core.LockConfiguration
import net.javacrumbs.shedlock.core.SimpleLock
import no.nav.arbeidsplassen.internalad.indexer.feed.ElasticsearchFeedRepository
import no.nav.arbeidsplassen.internalad.indexer.feed.FeedTask
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.util.*

@Controller("/internal")
class IndexerController(val indexerService: IndexerService,
                        val feedTaskRepository: ElasticsearchFeedRepository,
                        val lockProvider: ElasticsearchLockProvider) {

    companion object {
        private val LOG = LoggerFactory.getLogger(IndexerController::class.java)
    }

    @Get("/lastupdated")
    fun getLastUpdateTimeForIndex(@QueryValue indexName: String): LocalDateTime {
        return indexerService.fetchLastUpdatedTimeForIndex(indexName)
    }

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

    @Put("/feedtasks")
    fun setFeedTaskLastRunDate(@QueryValue name: String,
                               @QueryValue @Format("yyyy-MM-dd'T'HH:mm:ss.SSS") lastRun: LocalDateTime): FeedTask? {
        return feedTaskRepository.save(FeedTask(name, lastRun))
    }

    @Get("/schedulerlocks")
    fun getAllSchedulerLocks(): List<JsonNode>{
        return lockProvider.getAllLocks()
    }

    @Put("/schedulerlocks")
    fun stopScheduler(@QueryValue name: String, @QueryValue minutes: Long):  Boolean {
        LOG.info("Locking sheduler ${name} for ${minutes} minutes")
        val lock = lockProvider.lock(LockConfiguration(name, Instant.now().plusSeconds(minutes * 60)))
        return lock.isPresent
    }

}

data class IndexerResponse(val indexName: String, val lastUpdated: LocalDateTime)