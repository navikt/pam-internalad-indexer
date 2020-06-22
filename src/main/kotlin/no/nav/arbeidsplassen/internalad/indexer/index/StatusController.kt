package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.configuration.kafka.ConsumerRegistry
import io.micronaut.context.annotation.Value
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import org.slf4j.LoggerFactory

@Controller("/internal")
class StatusController(private val consumerRegistry: ConsumerRegistry) {

    companion object {
        private val LOG = LoggerFactory.getLogger(StatusController::class.java)
    }

    @Get("/isReady")
    fun isReady(): String {
        return "OK"
    }

    @Get("/isAlive")
    fun isAlive(): HttpResponse<String> {
        consumerRegistry.consumerIds.forEach {
            if ( consumerRegistry.isPaused(it)) {
                LOG.error("Kafka is not responding for consumer $it")
                return HttpResponse.serverError("Kafka is not responding for consumer $it")
            }
        }
        return HttpResponse.ok("Alive")

    }

}
