package no.nav.arbeidsplassen.internalad.indexer.process

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.function.Consumer

class FilterBadSearchtags: Consumer<PipelineItem> {

    companion object {
        const val MIN_SCORE = 0.5
    }

    override fun accept(item: PipelineItem) {
        val searchtags = item.document.path("properties").path("searchtags")
        if (!searchtags.isMissingNode) {
            val i: MutableIterator<*> = searchtags.iterator()
            while (i.hasNext()) {
                val st = i.next() as JsonNode
                if (st.path("score").asDouble() < MIN_SCORE) {
                    i.remove()
                } else if (st.path("label").asText().startsWith("000_")) {
                    i.remove()
                }
            }
            if (searchtags.size() == 0) {
                (item.document.path("properties") as ObjectNode).remove("searchtags")
            }
        }
    }
}