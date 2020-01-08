package no.nav.arbeidsplassen.internalad.indexer.process

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import java.util.function.Consumer
import java.util.function.Predicate
import java.util.stream.Stream

class PipelineFactory(val filterChain: Predicate<AdTransport>,
                      val processorChain: Consumer<PipelineItem>,
                      val objectMapper: ObjectMapper) {

    fun toPipelineStream(dtoList: List<AdTransport>): Stream<PipelineItem> {
        return dtoList.stream()
                .map { dto: AdTransport ->
                    val item = PipelineItem(dto, objectMapper.valueToTree(dto))
                    if (!filterChain.test(dto)) {
                        item.flagDelete = true
                    }
                    processorChain.accept(item)
                    item
                }
    }
}

data class PipelineItem(val dto: AdTransport,
                           val document: ObjectNode,
                           var flagDelete: Boolean = false)