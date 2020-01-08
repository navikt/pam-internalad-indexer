package no.nav.arbeidsplassen.internalad.indexer.process

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Factory
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport

import java.util.function.Consumer
import java.util.function.Predicate
import javax.inject.Singleton

@Factory
class PipelineConfiguration(val objectMapper: ObjectMapper) {

    @Singleton
    fun adPipeline(): PipelineFactory { // Filter chain determines which ads are indexed or deleted from ElasticSearch
        val filterChain: Predicate<AdTransport> = notDeletedAdsFilter()
        val processorChain: Consumer<PipelineItem> = AddGeoPointToLocation()
                .andThen(JsonStringExploder(objectMapper, JsonPointer.valueOf("/properties/searchtags"), true))
                .andThen(JsonStringExploder(objectMapper, JsonPointer.valueOf( "/properties/softrequirements"), true))
                .andThen(JsonStringExploder(objectMapper, JsonPointer.valueOf("/properties/hardrequirements"), true))
                .andThen(JsonStringExploder(objectMapper, JsonPointer.valueOf("/properties/personalattributes"), true))
                .andThen(JsonStringExploder(objectMapper, JsonPointer.valueOf("/employer/properties/nace2"), true))
                .andThen(FilterBadSearchtags())
        return PipelineFactory(filterChain, processorChain, objectMapper)
    }

    companion object {
        private fun notDeletedAdsFilter(): Predicate<AdTransport> {
            return Predicate<AdTransport> { dto: AdTransport -> "DELETED" != dto.status }
        }

        private fun removeFinnContactInfo(): Consumer<in PipelineItem> {
            return Consumer<PipelineItem> { item: PipelineItem ->
                if ("FINN" == item.dto.source) {
                    item.document.remove("contactList")
                }
            }
        }
    }
}