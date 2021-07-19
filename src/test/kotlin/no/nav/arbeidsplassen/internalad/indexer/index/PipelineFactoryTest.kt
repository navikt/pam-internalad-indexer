package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import no.nav.arbeidsplassen.internalad.indexer.process.PipelineFactory
import org.junit.jupiter.api.Test

@MicronautTest
class PipelineFactoryTest(private val adPipelineFactory: PipelineFactory, private val objectMapper: ObjectMapper) {

    @Test
    fun pipelineFactoryTest() {
        val ad = objectMapper.readValue(PipelineFactoryTest::class.java.getResourceAsStream("/fullAdDTO.json"), AdTransport::class.java)
        val items = adPipelineFactory.toPipelineStream(listOf(ad))
        items.forEach {
            println(objectMapper.writeValueAsString(it))
        }
    }
}
