package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.configuration.kafka.ConsumerAware
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.OffsetReset
import io.micronaut.configuration.kafka.annotation.OffsetStrategy
import io.micronaut.configuration.kafka.annotation.Topic
import io.micronaut.context.annotation.Requires
import io.micronaut.context.annotation.Value
import io.micronaut.core.convert.format.Format
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.common.TopicPartition
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.time.ZoneId

@KafkaListener(groupId = "\${adlistener.group-id:internalad-indexer}", threads = 1, offsetReset = OffsetReset.EARLIEST,
        batch = true, offsetStrategy = OffsetStrategy.SYNC)
@Requires(property = "indexer.enabled", value = "true")
class AdTopicListener(private val indexerService: AdIndexer): ConsumerRebalanceListener, ConsumerAware<Any, Any> {

    private lateinit var consumer: Consumer<Any,Any>

    companion object {
        private val LOG = LoggerFactory.getLogger(AdTopicListener::class.java)
    }

    init {
    }

    @Topic("\${adlistener.topic:StillingIntern}")
    fun receive(ads: List<AdTransport>, offsets: List<Long>, partition: Int) {
        LOG.info("Received batch with {} ads", ads.size)
        if (ads.isNotEmpty()) {
            val indexResponse = indexerService.index(ads)
            val last = ads.last()
            if (indexResponse.status == RestStatus.OK && !indexResponse.hasFailures) {
                LOG.info("indexed ${indexResponse.numItems}")
                LOG.info("committing latest offset ${offsets.last()} partition ${partition} with ad ${last.uuid} and last updated was ${last.updated}")
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

fun LocalDateTime.toMillis(): Long {
    return this.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
}
