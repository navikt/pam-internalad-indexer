package no.nav.arbeidsplassen.internalad.indexer

import io.micronaut.runtime.Micronaut

object Application {

    @JvmStatic
    fun main(args: Array<String>) {
        Micronaut.build()
                .packages("no.nav.arbeidsplassen.internalad.indexer")
                .mainClass(Application.javaClass)
                .start()
    }
}