package no.nav.arbeidsplassen.internalad.indexer.index

import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get

@Controller("/internal")
class StatusController {

    @Get("/isReady")
    fun isReady(): String {
        return "OK"
    }

    @Get("/isAlive")
    fun isAlive(): String {
        return "Alive"
    }

}