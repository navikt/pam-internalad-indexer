package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory

@KafkaListener(
    clientId = AD_LISTENER_CLIENT_ID,
    groupId = "\${adlistener.reindex.group-id:internalad-reindexer}",
    threads = 1,
    offsetReset = OffsetReset.EARLIEST,
    batch = true,
    offsetStrategy = OffsetStrategy.SYNC
)
@Requires(property = "indexer.reindex", value = "true")
class AdReIndexTopicListener(
    private val indexerService: IndexerService,
    @Value("\${indexer.reindex.indexname:reindex-internalad-1}") val indexName: String
) : ConsumerRebalanceListener, ConsumerAware<Any, Any> {

    private lateinit var consumer: Consumer<Any, Any>

    companion object {
        private val LOG = LoggerFactory.getLogger(AdReIndexTopicListener::class.java)

    }

    init {
        try {
            initIndex()
        } catch (e: Exception) {
            LOG.error("Elasticsearch might not be ready ${e.message}, will wait 20s and retry")
            Thread.sleep(20000)
            initIndex()
        }
    }

    private fun initIndex() {
        indexerService.initIndex(indexName)
        LOG.info("Will reindex to $indexName")
    }

    @Topic("\${adlistener.topic:StillingIntern}")
    fun receive(ads: List<AdTransport>, offsets: List<Long>, partitions: List<Int>) {
        LOG.info("Received batch with {} ads", ads.size)
        if (ads.isNotEmpty()) {
            val indexResponse = indexerService.index(ads, indexName)
            val last = ads.last()
            if (indexResponse.status == RestStatus.OK && !indexResponse.hasFailures) {
                LOG.info("indexed ${indexResponse.numItems}")
                LOG.info("committing latest offset ${offsets.last()} partition ${partitions.last()} with ad ${last.uuid} and last updated was ${last.updated}")
            } else {
                throw Exception("Index response has failures, elasticsearch might be down")
            }
        }
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        LOG.info("onPartitionsAssigned is not implemented")
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        LOG.info("onPartionsRevoked is not implemented")
    }

    override fun setKafkaConsumer(consumer: Consumer<Any, Any>) {
        this.consumer = consumer
    }
}
