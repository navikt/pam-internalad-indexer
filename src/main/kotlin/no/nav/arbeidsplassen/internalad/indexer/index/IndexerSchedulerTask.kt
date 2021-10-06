package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.aop.Around
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import net.javacrumbs.shedlock.micronaut.SchedulerLock
import jakarta.inject.Singleton

@Around
@Singleton
@Requires(property = "indexer.scheduler.delete.enabled", value = "true")
class DeleteIndexerSchedulerTask(val indexerService: IndexerService) {

    @Scheduled(cron = "0 30 0 * * *")
    @SchedulerLock(name = "deleteOldAdsFromIndex")
    fun deleteOldAdsFromIndex() {
        indexerService.deleteOldAds()
    }

}
