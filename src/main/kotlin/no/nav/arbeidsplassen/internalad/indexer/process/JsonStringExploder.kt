package no.nav.arbeidsplassen.internalad.indexer.process

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.function.Consumer

class JsonStringExploder(val objectMapper: ObjectMapper,
                                      val jsonPointer: JsonPointer,
                                      val failOnParseError: Boolean) : Consumer<PipelineItem> {

    override fun accept(item: PipelineItem) {
        val node = item.document.at(jsonPointer)
        if (node.isMissingNode || !node.isTextual) {
            return
        }
        try {
            val explodedValue = objectMapper.readTree(node.textValue())
            val parent = item.document.at(jsonPointer.head()) as ObjectNode
            parent[jsonPointer.last().matchingProperty] = explodedValue
        } catch (e: Exception) {
            if (failOnParseError) {
                throw RuntimeException(e)
            }
        }
    }

}