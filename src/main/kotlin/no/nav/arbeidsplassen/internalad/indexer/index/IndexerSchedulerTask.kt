package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.aop.Around
import io.micronaut.context.annotation.Requires
import io.micronaut.scheduling.annotation.Scheduled
import net.javacrumbs.shedlock.micronaut.SchedulerLock
import javax.inject.Singleton

@Around
@Singleton
@Requires(property = "indexer.scheduler.enabled", value = "true")
class IndexerSchedulerTask(val indexerService: IndexerService) {

    @Scheduled(cron = "5,20,35,50 * * * * *")
    @SchedulerLock(name = "fetchFeedAndIndexAds")
    fun fetchAdsAndIndex() {
        indexerService.fetchFeedIndexAds()
    }

    @Scheduled(cron = "0 30 0 * * *")
    @SchedulerLock(name = "deleteOldAdsFromIndex")
    fun deleteOldAdsFromIndex() {
        indexerService.deleteOldAds()
    }

}