package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.JsonNode
import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.core.annotation.Nullable
import io.micronaut.http.annotation.*
import net.javacrumbs.shedlock.core.LockConfiguration
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime

@Controller("/internal")
class IndexerController(private val indexerService: IndexerService,
                        private val lockProvider: ElasticsearchLockProvider,
                        private val consumerRegistry: ConsumerRegistry) {

    companion object {
        private val LOG = LoggerFactory.getLogger(IndexerController::class.java)
    }

    @Get("/lastupdated")
    fun getLastUpdateTimeForIndex(@Nullable @QueryValue indexName: String = indexerService.indexName): LocalDateTime {
        return indexerService.fetchLastUpdatedTimeForIndex(indexName)
    }

    @Put("/aliases")
    fun updateAliases(@Nullable @QueryValue indexName: String = indexerService.indexName): Boolean {
        LOG.info("Switching alias to $indexName")
        return indexerService.updateAlias(indexName)
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

    @Get("/consumers")
    fun getConsumerIds(): MutableSet<String> {
        return consumerRegistry.consumerIds
    }

    @Put("/indexer/pause")
    fun pauseIndexer(): String {
        LOG.info("Pausing indexer")
        consumerRegistry.consumerIds
                .filter { it.startsWith(AD_LISTENER_CLIENT_ID) }
                .forEach {
                    LOG.info("Pausing consumer $it")
                    consumerRegistry.pause(it)
                }
        return "OK, resume with /indexer/resume"
    }

    @Put("/indexer/resume")
    fun resumeIndexer(): String {
        LOG.info("Resuming indexer")
        consumerRegistry.consumerIds
                .filter { it.startsWith(AD_LISTENER_CLIENT_ID) }
                .forEach {
                    LOG.info("Resuming consumer $it ")
                    consumerRegistry.resume(it)
                }
        return "OK"
    }

}
