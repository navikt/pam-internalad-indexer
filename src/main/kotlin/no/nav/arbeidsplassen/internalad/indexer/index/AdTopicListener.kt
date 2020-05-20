package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.configuration.kafka.exceptions.KafkaListenerException
import io.micronaut.context.annotation.Value
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory

@KafkaListener(groupId = "\${adlistener.group-id:internalAd}",
        threads = 1,
        offsetReset = OffsetReset.EARLIEST,
        batch = true,
        offsetStrategy = OffsetStrategy.SYNC)
class AdTopicListener(private val indexerService: AdIndexer) {

    companion object {
        private val LOG = LoggerFactory.getLogger(AdTopicListener::class.java)
    }

    @Topic("\${adlistener.topic:StillingIntern}")
    fun receive(ads: List<AdTransport>,
                offsets: List<Long>) {
        LOG.info("Received batch with {} ads", ads.size)
        if (ads.isNotEmpty()) {
            val indexResponse = indexerService.index(ads)
            val last = ads.last()
            if (indexResponse.status == RestStatus.OK && !indexResponse.hasFailures) {
                LOG.info("indexed ${indexResponse.numItems}")
                LOG.info("committing latest offset ${offsets.last()} with ad ${last.uuid} and last updated was ${last.updated}")
            } else {
                throw Exception("Index response has failures, elasticsearch might be down")
            }
        }
     }
}
