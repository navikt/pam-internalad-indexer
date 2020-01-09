package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Value
import no.nav.arbeidsplassen.internalad.indexer.feed.AdTransport
import no.nav.arbeidsplassen.internalad.indexer.feed.FeedConnector
import no.nav.arbeidsplassen.internalad.indexer.feed.FeedTaskService
import no.nav.arbeidsplassen.internalad.indexer.process.PipelineFactory
import no.nav.arbeidsplassen.internalad.indexer.process.PipelineItem
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.index.reindex.DeleteByQueryRequest
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.stream.Stream
import javax.inject.Singleton


@Singleton
class IndexerService(val feedTaskService: FeedTaskService,
                     val feedConnector: FeedConnector,
                     val client: RestHighLevelClient,
                     val objectMapper: ObjectMapper,
                     @Value("\${indexer.ads.from}") val months: Long = 12,
                     @Value("\${feed.ad.url}") val adUrl: String,
                     val adPipelineFactory: PipelineFactory) {

    companion object {
        private const val FETCH_INTERNAL_ADS = "fetchInternalAds"
        private val LOG = LoggerFactory.getLogger(IndexerService::class.java)
    }

    init {
        val defaultIndexRequest = GetIndexRequest(INTERNALAD)
        if (!client.indices().exists(defaultIndexRequest, RequestOptions.DEFAULT)) {
            val indexName = internalAdIndexWithTimestamp()
            createIndex(indexName)
            updateAlias(indexName)
        }

    }

    fun createIndex(indexName: String): Boolean {
        val indexRequest = GetIndexRequest(indexName)
        if (!client.indices().exists(indexRequest, RequestOptions.DEFAULT)) {
            LOG.info("Creating index {} ", indexName)
            val request = CreateIndexRequest(indexName)
                    .source(INTERNALAD_COMMON_SETTINGS, XContentType.JSON)
            client.indices().create(request, RequestOptions.DEFAULT)
            val putMappingRequest = PutMappingRequest(indexName)
                    .source(INTERNALAD_MAPPING, XContentType.JSON)
            client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
            return true
        }
        return false
    }

    fun updateAlias(indexName: String): Boolean {
        val remove = IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                .index("*")
                .alias(INTERNALAD)
        val add = IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                .index(indexName)
                .alias(INTERNALAD)
        val request = IndicesAliasesRequest()
                .addAliasAction(remove)
                .addAliasAction(add)
        LOG.info("updateAlias for alias $INTERNALAD and pointing to $indexName ")
        return client.indices().updateAliases(request, RequestOptions.DEFAULT).isAcknowledged

    }

    fun fetchFeedIndexAds() {
        val lastRunDate = feedTaskService.fetchLastRunDateForJob(FETCH_INTERNAL_ADS) ?: getDefaultStartTime()
        val ads = feedConnector.fetchContentList( adUrl, lastRunDate, AdTransport::class.java)
        if (ads.isNotEmpty()) {
            val adStream = adPipelineFactory.toPipelineStream(ads)
            val bulkResponse = indexBulk(adStream, INTERNALAD)
            if (bulkResponse.status() == RestStatus.OK && !bulkResponse.hasFailures()) {
                LOG.info("indexed ${bulkResponse.items.size} items")
                val adTransport = ads[ads.size - 1]
                feedTaskService.save(FETCH_INTERNAL_ADS, adTransport.updated)
                LOG.info("Last date is set to ${adTransport.updated}")
            }
            else {
                LOG.error("We got error while indexing: ${bulkResponse.buildFailureMessage()}")
            }
        }
    }

    fun fetchFeedIndexAdsUntilNow(from: LocalDateTime, indexName: String): LocalDateTime {
        LOG.info("Fetch feed from ${from} and index to ${indexName}")
        var ads = feedConnector.fetchContentList( adUrl, from, AdTransport::class.java)
        var lastUpdated = from
        while(ads.isNotEmpty())   {
            val adStream = adPipelineFactory.toPipelineStream(ads)
            val bulkResponse = indexBulk(adStream, indexName)
            if (bulkResponse.status() == RestStatus.OK && !bulkResponse.hasFailures() && ads[ads.size - 1].updated.isAfter(lastUpdated)) {
                lastUpdated = ads[ads.size - 1].updated
                LOG.info("Last updated time set to $lastUpdated")
                ads = feedConnector.fetchContentList(adUrl, lastUpdated, AdTransport::class.java)
            }
            else {
                if (bulkResponse.hasFailures()) {
                    LOG.error("We got error while indexing: ${bulkResponse.buildFailureMessage()}")
                }
                ads = listOf()
            }
        }
        return lastUpdated
    }

    private fun indexBulk(ads: Stream<PipelineItem>, indexName: String): BulkResponse {
        val bulkRequest = BulkRequest()
        ads.forEach {
            bulkRequest.add(IndexRequest(indexName)
                    .id(it.dto.uuid)
                    .source(objectMapper.writeValueAsString(it.document), XContentType.JSON))
        }
        return client.bulk(bulkRequest, RequestOptions.DEFAULT)
    }


    fun fetchLastUpdatedTimeForIndex(indexName: String): LocalDateTime {
        val searchRequest = SearchRequest(indexName)
        val sourceBuilder = SearchSourceBuilder()
                            .size(1)
                            .sort(SortBuilders
                                    .fieldSort("updated")
                                    .order(SortOrder.DESC))
        sourceBuilder.query(MatchAllQueryBuilder())
        searchRequest.source(sourceBuilder)
        val searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
        return objectMapper.readValue(searchResponse.hits.hits[0].sourceAsString, AdTransport::class.java).updated
    }

    fun deleteOldAds() {
        val deleteRequest = DeleteByQueryRequest(INTERNALAD)
        val adsOlderThan = getDefaultStartTime()
        LOG.info("Deleting ads older than $adsOlderThan from index")
        val oldAdsRange = RangeQueryBuilder("updated").lt(adsOlderThan)
        deleteRequest.setQuery(oldAdsRange)
        val response = client.deleteByQuery(deleteRequest, RequestOptions.DEFAULT)
        LOG.info("Deleted ${response.deleted} ads")
    }

    fun getDefaultStartTime(): LocalDateTime {
        return LocalDateTime.now().minusMonths(months)
    }
}