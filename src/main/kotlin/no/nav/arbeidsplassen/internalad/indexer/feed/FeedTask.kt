package no.nav.arbeidsplassen.internalad.indexer.feed

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.LocalDateTime

@JsonIgnoreProperties(ignoreUnknown = true)
data class FeedTask(val name: String, val lastRun: LocalDateTime = LocalDateTime.now())

class FeedTaskService(val feedRepository: ElasticsearchFeedRepository) {

    fun fetchLastRunDateForJob(name: String): LocalDateTime? {
        return feedRepository.findByFeedName(name)?.lastRun
    }

    fun save(jobName: String, lastRunDate: LocalDateTime): FeedTask? {
        return feedRepository.save(FeedTask(jobName, lastRunDate))
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class FeedTransport<T>(val last: Boolean, val totalPages: Int, val totalElements: Int, val size: Int, val number: Int,
                            val first: Boolean, val numberOfElements: Int, val content: List<T>)
