package no.nav.arbeidsplassen.internalad.indexer

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import io.micronaut.http.client.DefaultHttpClient
import io.micronaut.http.client.RxHttpClient
import io.micronaut.test.annotation.MicronautTest
import io.micronaut.test.annotation.MockBean
import io.micronaut.test.support.TestPropertyProvider
import io.reactivex.Flowable
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import no.nav.arbeidsplassen.internalad.indexer.feed.FeedConnector
import no.nav.arbeidsplassen.internalad.indexer.index.AdTopicListener
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.`when`
import org.mockito.Mockito.mock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import java.time.LocalDateTime
import javax.inject.Inject

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
class FeedConnectorTest : TestPropertyProvider {

    @Inject
    lateinit var feedConnector: FeedConnector

    @Inject
    lateinit var client: RxHttpClient

    @Inject
    lateinit var kafkaListener: AdTopicListener

    companion object {
        private val wireMockServer: WireMockServer = WireMockServer(9001)
        private val LOG: Logger = LoggerFactory.getLogger(FeedConnectorTest::class.java)
        private val kafkaContainer: KafkaContainer = KafkaContainer()
    }

    init {
        wireMockServer.stubFor(head(urlMatching("/feedtask-internalad.*"))
                .willReturn(aResponse()
                        .withStatus(200)))
        wireMockServer.stubFor(put(urlMatching("/internalad.*"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"index\": \"foo\", \"acknowledged\": \"true\", \"shards_acknowledged\": \"true\"}")
                        .withStatus(200)))
        wireMockServer.stubFor(post(urlMatching("/_aliases.*"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"index\": \"foo\", \"acknowledged\": \"true\", \"shards_acknowledged\": \"true\"}")
                        .withStatus(200)))
        wireMockServer.start()

        kafkaContainer.start()
        LOG.info("Kafka bootstrap servers: " + kafkaContainer.bootstrapServers)

    }

    override fun getProperties(): Map<String, String> {
        return hashMapOf(Pair("kafka.bootstrap.servers", kafkaContainer.bootstrapServers))
    }


    @BeforeAll
    fun initKafka() {
    }
    @AfterAll
    fun tearDownWiremock() {
        wireMockServer.shutdownServer()
        kafkaContainer.close()
    }


    @Test
    fun feedConnectorTest() {
        `when`(client.retrieve(ArgumentMatchers.any())).thenReturn(Flowable.just(feedTransportJSON))
        val fetchContentList = feedConnector.fetchContentList("http://localhost:9001/api/v1/ads", LocalDateTime.now(), AdTransport::class.java)
        assertEquals(fetchContentList.size, 2)
    }

    @Test
    fun kafkaListenerTest() {
        //KafkaCl
    }

    @MockBean(DefaultHttpClient::class)
    fun mockClient(): RxHttpClient {
        return mock(RxHttpClient::class.java)
    }
}

val feedTransportJSON = """
    {
      "content": [
        {
          "id": 6676,
          "uuid": "42d050eb-87ea-4057-8dd8-27827ec3cd74",
          "created": "2018-11-20T12:51:11.943",
          "createdBy": "pam-ad",
          "updated": "2019-05-16T00:00:00.108",
          "updatedBy": "nss-admin",
          "mediaList": [],
          "contactList": [],
          "location": {
            "address": null,
            "postalCode": "0552",
            "county": "OSLO",
            "municipal": "OSLO",
            "municipalCode": "0301",
            "city": "OSLO",
            "country": "NORGE",
            "latitude": null,
            "longitude": null
          },
          "locationList": [
            {
              "address": null,
              "postalCode": "0552",
              "county": "OSLO",
              "municipal": "OSLO",
              "municipalCode": "0301",
              "city": "OSLO",
              "country": "NORGE",
              "latitude": null,
              "longitude": null
            }
          ],
          "properties": {
            "extent": "Heltid",
            "sourceurl": "http://someurl",
            "searchtags": "[{\"label\":\"Selger\",\"score\":0.056459412},{\"label\":\"Prosjektstyrer med spesialkompetanse på planlegging\",\"score\":0.04513244},{\"label\":\"Callsentermedarbeider\",\"score\":0.04375312},{\"label\":\"Møbel- og interiørbutikkmedarbeider\",\"score\":0.04369063},{\"label\":\"Selger Business-to-Business\",\"score\":0.043468766}]",
            "author": "author",
            "engagementtype": "Annet",
            "classification_styrk08_score": "0.057036243",
            "employer": "24onoff",
            "location": "\n        Trondheim",
            "classification_input_source": "title",
            "adtext": "Dette er adtext",
            "sector": "Ikke oppgitt"
          },
          "title": "Salgskonsulent - sulten på utfordringer med gode lønnsbetingelser?",
          "status": "INACTIVE",
          "privacy": "SHOW_ALL",
          "source": "DEXI",
          "medium": "TheHub",
          "reference": "\n        5be137cc7552527840d32779",
          "published": "2019-05-14T11:23:13.954",
          "expires": "2019-05-15T01:00:00",
          "employer": {
            "id": 126603,
            "uuid": "51832e93-d63c-4a34-8477-6ce3016225bb",
            "created": "2019-05-14T11:23:13.953",
            "createdBy": "pam-ad",
            "updated": "2019-05-14T11:23:13.953",
            "updatedBy": "pam-ad",
            "mediaList": [],
            "contactList": [],
            "location": {
              "address": "Industriveien 5",
              "postalCode": "7072",
              "county": "TRØNDELAG",
              "municipal": "TRONDHEIM",
              "municipalCode": "5001",
              "city": "HEIMDAL",
              "country": "NORGE",
              "latitude": null,
              "longitude": null
            },
            "locationList": [
              {
                "address": "Industriveien 5",
                "postalCode": "7072",
                "county": "TRØNDELAG",
                "municipal": "TRONDHEIM",
                "municipalCode": "5001",
                "city": "HEIMDAL",
                "country": "NORGE",
                "latitude": null,
                "longitude": null
              }
            ],
            "properties": {
              "nace2": "[{\"code\":\"62.010\",\"name\":\"Programmeringstjenester\"}]"
            },
            "name": "24ONOFF AS",
            "orgnr": "911817411",
            "status": "ACTIVE",
            "parentOrgnr": "911801663",
            "publicName": "24ONOFF AS",
            "deactivated": null,
            "orgform": "BEDR",
            "employees": 6
          },
          "categoryList": [
            {
              "id": 3966,
              "code": "5223.04",
              "categoryType": "STYRK08NAV",
              "name": "Selger (detalj)",
              "description": null,
              "parentId": 3962
            }
          ],
          "administration": {
            "id": 5512,
            "status": "DONE",
            "comments": null,
            "reportee": "reportee",
            "remarks": [],
            "navIdent": null
          },
          "publishedByAdmin": "2019-05-14T11:23:13.954",
          "businessName": "24ONOFF AS",
          "firstPublished": true,
          "deactivatedByExpiry": true,
          "activationOnPublishingDate": false
        },
        {
          "id": 34904,
          "uuid": "83a08d2c-b0e2-4d5d-88dc-02736b9c6be1",
          "created": "2018-12-24T08:06:52.402",
          "createdBy": "pam-ad",
          "updated": "2019-05-16T00:00:00.123",
          "updatedBy": "pam-ad",
          "mediaList": [],
          "contactList": [],
          "location": {
            "address": null,
            "postalCode": "8005",
            "county": "NORDLAND",
            "municipal": "BODØ",
            "municipalCode": "1804",
            "city": "BODØ",
            "country": "NORGE",
            "latitude": null,
            "longitude": null
          },
          "locationList": [
            {
              "address": null,
              "postalCode": "8005",
              "county": "NORDLAND",
              "municipal": "BODØ",
              "municipalCode": "1804",
              "city": "BODØ",
              "country": "NORGE",
              "latitude": null,
              "longitude": null
            }
          ],
          "properties": {
            "extent": "Heltid",
            "searchtags": "[{\"label\":\"Anestesisykepleier\",\"score\":1.0},{\"label\":\"Pediatric Critical Care Nurse\",\"score\":1.0},{\"label\":\"Smertesykepleier\",\"score\":1.0},{\"label\":\"Sykepleier akuttavdelingen\",\"score\":1.0},{\"label\":\"Palliativ sykepleier\",\"score\":1.0}]",
            "jobtitle": "Anestesisykepleier",
            "engagementtype": "Annet",
            "classification_styrk08_score": "1.0",
            "employer": "Nordlandssykehuset",
            "location": "Bodø",
            "employerdescription": "description",
            "classification_input_source": "jobtitle",
            "adtext": "adtext",
            "sector": "Ikke oppgitt"
          },
          "title": "Nordlandssykehuset Bodø søker ferievikarer innen sykepleie",
          "status": "INACTIVE",
          "privacy": "SHOW_ALL",
          "source": "stillingsolr",
          "medium": "Overført fra arbeidsgiver",
          "reference": "9946267",
          "published": "2018-12-20T01:00:00",
          "expires": "2019-05-15T01:00:00",
          "employer": {
            "id": 1176,
            "uuid": "e7d15d3b-346f-4353-8389-8f5821072457",
            "created": "2018-11-19T12:05:46.841",
            "createdBy": "pam-ad",
            "updated": "2019-05-31T01:31:05.775",
            "updatedBy": "pam-ad",
            "mediaList": [],
            "contactList": [],
            "location": {
              "address": "Prinsens gate 164",
              "postalCode": "8005",
              "county": "NORDLAND",
              "municipal": "BODØ",
              "municipalCode": "1804",
              "city": "BODØ",
              "country": "NORGE",
              "latitude": null,
              "longitude": null
            },
            "locationList": [
              {
                "address": "Prinsens gate 164",
                "postalCode": "8005",
                "county": "NORDLAND",
                "municipal": "BODØ",
                "municipalCode": "1804",
                "city": "BODØ",
                "country": "NORGE",
                "latitude": null,
                "longitude": null
              }
            ],
            "properties": {
              "nace2": "[{\"code\":\"47.112\",\"name\":\"Kioskhandel med bredt vareutvalg med hovedvekt på nærings- og nytelsesmidler\"}]"
            },
            "name": "NARVESEN AVD 659 NORDLANDSSYKEHUSET",
            "orgnr": "972113913",
            "status": "ACTIVE",
            "parentOrgnr": "998543975",
            "publicName": "NARVESEN AVD 659 NORDLANDSSYKEHUSET",
            "deactivated": null,
            "orgform": "BEDR",
            "employees": 9
          },
          "categoryList": [
            {
              "id": 1420,
              "code": "2221.03",
              "categoryType": "STYRK08NAV",
              "name": "Anestesisykepleier",
              "description": null,
              "parentId": 1417
            }
          ],
          "administration": {
            "id": 29312,
            "status": "DONE",
            "comments": null,
            "reportee": "System",
            "remarks": [],
            "navIdent": null
          },
          "publishedByAdmin": "2018-12-20T01:00:00",
          "businessName": "Nordlandssykehuset",
          "firstPublished": true,
          "deactivatedByExpiry": true,
          "activationOnPublishingDate": false
        }
      ],
      "pageable": {
        "sort": {
          "sorted": true,
          "unsorted": false,
          "empty": false
        },
        "offset": 0,
        "pageSize": 2,
        "pageNumber": 0,
        "paged": true,
        "unpaged": false
      },
      "last": false,
      "totalPages": 44539,
      "totalElements": 89077,
      "number": 0,
      "sort": {
        "sorted": true,
        "unsorted": false,
        "empty": false
      },
      "size": 2,
      "first": true,
      "numberOfElements": 2,
      "empty": false
    }
""".trimIndent()