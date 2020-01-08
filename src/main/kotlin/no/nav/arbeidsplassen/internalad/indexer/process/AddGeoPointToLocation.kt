package no.nav.arbeidsplassen.internalad.indexer.process

import java.util.function.Consumer

class AddGeoPointToLocation: Consumer<PipelineItem> {
    override fun accept(item: PipelineItem) {
        val location = item.document.path("location")
        if (location.isMissingNode
                || location.path("latitude").isMissingNode
                || location.path("latitude").isNull
                || location.path("longitude").isMissingNode
                || location.path("longitude").isNull) {
            return
        }
        item.document.with("geopoint")
                .put("lat", location.path("latitude").asDouble())
                .put("lon", location.path("longitude").asDouble())
    }
}