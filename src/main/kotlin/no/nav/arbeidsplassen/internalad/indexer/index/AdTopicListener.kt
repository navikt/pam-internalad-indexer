package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.configuration.kafka.annotation.*
import io.micronaut.context.annotation.Value
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory

@KafkaListener(groupId="\${adlistener.group-id:internalAd}",
        threads = 1,
        offsetReset = OffsetReset.EARLIEST,
        batch = true,
        offsetStrategy = OffsetStrategy.DISABLED)
class AdTopicListener(private val indexerService: AdIndexer,
                    private val objectMapper: ObjectMapper) {
    companion object {
        private val LOG = LoggerFactory.getLogger(AdTopicListener::class.java)
    }

    @Topic("\${adlistener.topic:StillingIntern}")
    fun receive(jsonAds: List<String>,
                @KafkaKey ids : List<String>,
                offsets: List<Long>,
                partitions: List<Integer>,
                topics: List<String>,
                consumer: KafkaConsumer<String, String>) {

        LOG.info("Received batch with {} ads with ids: {}",
                ids.size, ids.joinToString())
        try {
            var idx = 0;
            val ads = jsonAds.map { jsonAd -> convertToAd(jsonAd, ids[idx++]) }
                    .filterNotNull()

            val indexResponse = indexerService.index(ads)
            val adTransport = ads[ads.lastIndex]

            if (indexResponse.status == RestStatus.OK && !indexResponse.hasFailures) {
                LOG.info("indexed ${indexResponse.numItems} items received from kafka")
                LOG.info("Last date received: ${adTransport.updated}, partition: {} offset: {}",
                        partitions[partitions.lastIndex],
                        offsets[offsets.lastIndex])
                consumer.commitSync()
            } else {
                LOG.error("We got an error while indexing: ${indexResponse.failureMessage}. " +
                        "Number of ads in batch: {} Last date in batch: {} partition: {} offset: {}. Ads in batch: {}",
                        ids.size, adTransport.updated,
                        partitions[partitions.lastIndex],
                        offsets[offsets.lastIndex], ids.joinToString())
            }

        } catch (e: Exception) {
            // TODO We don'† distinguish between infrastructure errors and application errors
            //      Thus, we need a way to handle poison pills
            LOG.error("We got exception while indexing. " +
                    "Number of ads in batch: {} partition: {} offset: {}. Ads in batch: {}. Errormessage: {}",
                    jsonAds.size, partitions[partitions.lastIndex],
                    offsets[offsets.lastIndex], ids.joinToString(),
                    e.message, e)
        }
    }

    private fun convertToAd(adAsString : String, adId : String) : AdTransport? {
        try {
            val ad = objectMapper.readValue(adAsString, AdTransport::class.java)
            return ad
        } catch (e : java.lang.Exception) {
            LOG.error("Failed to deserialize ad {}. Error: {}, json ad: {}", adId,
                e.message, adAsString)
            return null
        }
    }
}