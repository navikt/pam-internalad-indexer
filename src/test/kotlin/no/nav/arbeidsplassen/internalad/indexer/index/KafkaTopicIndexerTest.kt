package no.nav.arbeidsplassen.internalad.indexer

import io.micronaut.context.annotation.Primary
import io.micronaut.context.annotation.Property
import io.micronaut.context.annotation.Replaces
import io.micronaut.test.extensions.junit5.annotation.MicronautTest
import io.micronaut.test.support.TestPropertyProvider
import no.nav.arbeidsplassen.internalad.indexer.index.AdIndexer
import no.nav.arbeidsplassen.internalad.indexer.index.AdTransport
import no.nav.arbeidsplassen.internalad.indexer.index.IndexResponse
import no.nav.arbeidsplassen.internalad.indexer.index.IndexerService
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.elasticsearch.rest.RestStatus
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.LocalDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import javax.inject.Singleton

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@MicronautTest
class KafkaTopicIndexerTest : TestPropertyProvider {

    lateinit var kafkaProducer: Producer<String, String>

    companion object {
        private val LOG: Logger = LoggerFactory.getLogger(KafkaTopicIndexerTest::class.java)
        private val kafkaContainer: KafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("5.4.3"))
    }



    init {
        kafkaContainer.start()
        LOG.info("Kafka bootstrap servers: " + kafkaContainer.bootstrapServers)

    }

    override fun getProperties(): Map<String, String> {
        return hashMapOf(Pair("kafka.bootstrap.servers", kafkaContainer.bootstrapServers))
    }


    @BeforeAll
    fun initKafka() {
        val props: Map<String, String> = hashMapOf(Pair("bootstrap.servers", kafkaContainer.bootstrapServers))
        kafkaProducer = KafkaProducer<String, String>(props,
            StringSerializer(), StringSerializer())
    }

    @AfterAll
    fun tearDown() {
        kafkaContainer.close()
    }

    @Test
    @Property(name = "indexer.scheduler.enabled", value = "false")
    fun kafkaListenerTest() {
        kafkaProducer.send(ProducerRecord("StillingIntern", "83a08d2c-b0e2-4d5d-88dc-02736b9c6be1", adTransportJSON))
        kafkaProducer.flush()
        val isIndexed = adIndexerLatch.await(20L, TimeUnit.SECONDS)
        assert(isIndexed)
    }
}

@Primary
@Replaces(IndexerService::class)
@Singleton
class MyIndexerService: AdIndexer {
    override fun index(ads: List<AdTransport>): IndexResponse {
        adIndexerLatch.countDown()
        return IndexResponse(hasFailures = false, status = RestStatus.OK, numItems = 1, failureMessage = "")
    }

    override fun createIndex(indexName: String): Boolean {
        return true
    }

    override fun fetchLastUpdatedTimeForIndex(indexName: String): LocalDateTime {
        return LocalDateTime.now()
    }
}

val adIndexerLatch = CountDownLatch(1)

val adTransportJSON = """
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
""".trimIndent()
