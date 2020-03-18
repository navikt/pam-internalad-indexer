package no.nav.arbeidsplassen.internalad.indexer.process

import no.nav.pam.yrkeskategorimapper.StyrkCodeConverter
import java.util.function.Consumer
import javax.inject.Singleton

class AddOccupationList(val converter: StyrkCodeConverter): Consumer<PipelineItem> {

    private val occupation_list = "occupationList"
    private val occupation_level1_key = "level1"
    private val occupation_level2_key = "level2"

    override fun accept(item: PipelineItem) {
        val occupationList = item.document.putArray(occupation_list)
        item.dto.categoryList.stream()
                .filter { "STYRK08NAV" == it.categoryType }
                .map { converter.lookup(it.code) }
                .filter { it.isPresent }
                .map { it.get() }.filter { "0" != it.styrkCode }
                .distinct()
                .forEachOrdered {
                    occupationList.addObject()
                            .put(occupation_level1_key, it.categoryLevel1)
                            .put(occupation_level2_key, it.categoryLevel2)
                }
        if (occupationList.size() == 0) {
            val unidentified = converter.lookup("0").get()
            occupationList.addObject()
                    .put(occupation_level1_key, unidentified.categoryLevel1)
                    .put(occupation_level2_key, unidentified.categoryLevel2)
        }
    }
}