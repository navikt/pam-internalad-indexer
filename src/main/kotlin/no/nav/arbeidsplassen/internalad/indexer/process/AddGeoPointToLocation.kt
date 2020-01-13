package no.nav.arbeidsplassen.internalad.indexer.process

import java.util.function.Consumer

class AddGeoPointToLocation: Consumer<PipelineItem> {
    override fun accept(item: PipelineItem) {
        val locationList = item.document.path("locationList")
        if (locationList.isMissingNode || locationList[0] == null
                || locationList[0].path("latitude").isMissingNode
                || locationList[0].path("latitude").isNull
                || locationList[0].path("longitude").isMissingNode
                || locationList[0].path("longitude").isNull) {
            return
        }
        item.document.with("geopoint")
                .put("lat", locationList[0].path("latitude").asDouble())
                .put("lon", locationList[0].path("longitude").asDouble())
    }
}