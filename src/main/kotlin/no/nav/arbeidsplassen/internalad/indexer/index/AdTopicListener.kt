package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.messaging.Acknowledgement
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.index.shard.ShardId
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory
import java.lang.NullPointerException
import java.util.*

@KafkaListener(groupId="\${adlistener.group-id:internalAd}",
        threads = 1,
        offsetReset = OffsetReset.EARLIEST,
        batch = true,
        offsetStrategy = OffsetStrategy.DISABLED)
class AdTopicListener(private val indexerService: IndexerService) {
    companion object {
        private val LOG = LoggerFactory.getLogger(AdTopicListener::class.java)
    }

    private val useBogusIndexer = true

    @Topic("\${adlistener.topic:StillingIntern}")
    fun receive(records: ConsumerRecords<String, AdTransport>, ack: Acknowledgement) {
        val ids = records.map { record -> record.key() }
        LOG.info("Received batch with {} ads with ids: {}",
                ids.size, ids.joinToString())
        try {
            val ads = records.map { record -> record.value() }

            val bulkResponse = if (useBogusIndexer) bogusbulkIndex(ads) else indexerService.bulkIndex(ads)
            val adTransport = ads[ads.size - 1]

            if (bulkResponse.status() == RestStatus.OK && !bulkResponse.hasFailures()) {
                LOG.info("indexed ${bulkResponse.items.size} items received from kafka")
                LOG.info("Last date received: ${adTransport.updated}, partition: {} offset: {}",
                        records.last().partition(), records.last().offset())

                ack.ack()
            } else {
                LOG.error("We got error while indexing: ${bulkResponse.buildFailureMessage()}. " +
                        "Number of ads in batch: {} Last date in batch: {} partition: {} offset: {}. Ads in batch: {}",
                        ids.size, adTransport.updated, records.last().partition(), records.last().offset(), ids.joinToString())
                ack.nack()
            }

        } catch (e: Exception) {
            // TODO We don'† distiguish between infrastructure errors and application errors
            //      Thus we need a way to handle poison pills
            LOG.error("We got exception while indexing. " +
                    "Number of ads in batch: {} partition: {} offset: {}. Ads in batch: {}. Errormessage: {}",
                    ids.size, records.last().partition(), records.last().offset(), ids.joinToString(),
                    e.message, e)
            ack.nack()
        }
    }


    private fun bogusbulkIndex(ads: List<AdTransport>) : BulkResponse {
        val failureFactor = 1.0/20;
        var i : Int = 0;
        if (Math.random() > failureFactor) {
            val responses = List(ads.size) {
                BulkItemResponse(i++, DocWriteRequest.OpType.UPDATE,
                        UpdateResponse(ShardId("id", UUID.randomUUID().toString(),1), "Ad", "$i",
                                i.toLong(), 1L, 1L, DocWriteResponse.Result.UPDATED))
            }.toTypedArray()
            return BulkResponse(responses, 1234L)
        } else {
            return BulkResponse(arrayOf(
            BulkItemResponse(1, DocWriteRequest.OpType.UPDATE,
                BulkItemResponse.Failure("idc", "ads", "id1", NullPointerException(), RestStatus.BAD_REQUEST))), 123L)
        }
    }
}