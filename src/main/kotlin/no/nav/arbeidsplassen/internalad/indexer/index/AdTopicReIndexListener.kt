package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.core.convert.format.Format
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory
import java.time.LocalDateTime

@KafkaListener(groupId = "\${adlistener.reindex.group-id:internalAdReindex}", threads = 1, offsetReset = OffsetReset.EARLIEST,
        batch = true, offsetStrategy = OffsetStrategy.SYNC)
@Requires(property = "indexer.reindex-mode.enabled", value = "true")
class AdTopicReIndexListener(private val indexerService: AdIndexer,
                             @Value("\${indexer.reindex-mode.indexname}") private var indexName: String,
                             @Value("\${indexer.reindex-mode.from}") @Format("yyyy-MM-dd'T'HH:mm:ss'Z'")
                             private var offsetTimeStamp: LocalDateTime?,
                             @Value("\${indexer.reindex-mode.timestamp-from-index:false}") private val timestampfromindex: Boolean): ConsumerRebalanceListener, ConsumerAware<Any, Any> {

    private lateinit var consumer: Consumer<Any,Any>

    companion object {
        private val LOG = LoggerFactory.getLogger(AdTopicReIndexListener::class.java)
    }

    init {
        LOG.info("Enabling reindex-mode, we are going to index to $indexName")
        if (!indexerService.createIndex(indexName) && timestampfromindex) {
            runCatching {
                offsetTimeStamp = indexerService.fetchLastUpdatedTimeForIndex(indexName)
            }.onFailure { LOG.warn("could not get last timestamp, maybe empty index.") }
        }
    }

    @Topic("\${adlistener.topic:StillingIntern}")
    fun receive(ads: List<AdTransport>, offsets: List<Long>) {
        LOG.info("Re-indexed received batch with {} ads", ads.size)
        if (ads.isNotEmpty()) {
            val indexResponse = indexerService.index(ads, indexName!!)
            val last = ads.last()
            if (indexResponse.status == RestStatus.OK && !indexResponse.hasFailures) {
                LOG.info("Re-indexed ${indexResponse.numItems}")
                LOG.info("Re-indexed committing latest offset ${offsets.last()} with ad ${last.uuid} and last updated was ${last.updated}")
            } else {
                throw Exception("Re-index response has failures, elasticsearch might be down")
            }
        }
     }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        LOG.debug("onPartionsAssigned is called!")
        if (offsetTimeStamp!=null) {
            LOG.info("Resetting offset for timestamp {}", offsetTimeStamp)
            val topicPartitionTimestamp = partitions.map { it to offsetTimeStamp?.toMillis() }.toMap()
            val partitionOffsetMap = consumer.offsetsForTimes(topicPartitionTimestamp)
            partitionOffsetMap.forEach { (topic, timestamp) -> consumer.seek(topic, timestamp.offset()) }
        }
    }

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        LOG.debug("onPartitionsRevoked is called")
    }

    override fun setKafkaConsumer(consumer: Consumer<Any, Any>) {
        LOG.debug("set kafka is called")
        this.consumer = consumer
    }

}
