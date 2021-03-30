package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import org.slf4j.LoggerFactory

@Controller("/internal")
class StatusController(private val kafkaStateRegistry: KafkaStateRegistry, private val consumerRegistry: ConsumerRegistry) {

    companion object {
        private val LOG = LoggerFactory.getLogger(StatusController::class.java)
    }

    @Get("/isReady")
    fun isReady(): String {
        return "OK"
    }

    @Get("/isAlive")
    fun isAlive(): HttpResponse<String> {
        if (kafkaStateRegistry.hasError()) {
            LOG.error("A Kafka consumer is set to Error, setting all consumers to pause")
            consumerRegistry.consumerIds
                .forEach {
                    LOG.info("Pausing consumer $it")
                    consumerRegistry.pause(it)
                }
            return HttpResponse.serverError("Kafka consumer is not running")
        }
        return HttpResponse.ok("Alive")
    }

}
