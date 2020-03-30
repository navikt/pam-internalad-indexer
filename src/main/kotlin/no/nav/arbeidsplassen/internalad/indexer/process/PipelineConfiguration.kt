package no.nav.arbeidsplassen.internalad.indexer.process

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Factory
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import no.nav.pam.yrkeskategorimapper.StyrkCodeConverter

import java.util.function.Consumer
import java.util.function.Predicate
import javax.inject.Singleton

@Factory
class PipelineConfiguration(private val objectMapper: ObjectMapper,
                            private val styrkCodeConverter: StyrkCodeConverter) {

    @Singleton
    fun adPipeline(): PipelineFactory {
        val filterChain: Predicate<AdTransport> = allowAllFilter()
        val processorChain: Consumer<PipelineItem> = AddGeoPointToLocation()
                .andThen(AddOccupationList(styrkCodeConverter))
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

        private fun allowAllFilter(): Predicate<AdTransport> {
            return Predicate<AdTransport> {
                true
            }
        }

    }
}