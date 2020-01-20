package no.nav.arbeidsplassen.internalad.indexer

import io.micronaut.http.client.DefaultHttpClient
import io.micronaut.http.client.RxHttpClient
import io.micronaut.test.annotation.MicronautTest
import io.micronaut.test.annotation.MockBean
import io.reactivex.Flowable
import no.nav.arbeidsplassen.internalad.indexer.index.IndexerService

import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import javax.inject.Inject

@MicronautTest
class IndexerServiceIT {

    @Inject
    lateinit var indexerService: IndexerService

    @Inject
    lateinit var client: RxHttpClient

    @Test
    fun fetchAdsAndIndexTest() {
        Mockito.`when`(client.retrieve(ArgumentMatchers.any())).thenReturn(Flowable.just(feedTransportJSON))
        indexerService.fetchFeedIndexAds()
    }

    @MockBean(DefaultHttpClient::class)
    fun mockClient(): RxHttpClient {
        return Mockito.mock(RxHttpClient::class.java)
    }

}