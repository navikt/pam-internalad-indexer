package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import no.nav.arbeidsplassen.internalad.indexer.process.PipelineFactory
import no.nav.arbeidsplassen.internalad.indexer.process.PipelineItem
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.client.indices.PutMappingRequest
import org.elasticsearch.cluster.metadata.AliasMetadata
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
import jakarta.inject.Singleton
import org.elasticsearch.common.xcontent.XContentType

@Singleton
class IndexerService(val client: RestHighLevelClient,
                     val objectMapper: ObjectMapper,
                     val adPipelineFactory: PipelineFactory) {

    companion object {
        private val LOG = LoggerFactory.getLogger(IndexerService::class.java)
        private val months = 30L
        private val INTERNALAD_COMMON_SETTINGS = IndexerService::class.java
                .getResource("/internalad-common.json").readText()
        private val INTERNALAD_MAPPING = IndexerService::class.java
                .getResource("/internalad-mapping.json").readText()
    }

    fun initIndex(indexName: String) {
        val indexRequest = GetIndexRequest(indexName)
        if (!client.indices().exists(indexRequest, RequestOptions.DEFAULT)) {
            LOG.info("Index does not exist, creating index: $indexName")
            createIndex(indexName);
        }
    }

    fun createIndex(indexName: String): Boolean {
        val indexRequest = GetIndexRequest(indexName)
        if (!client.indices().exists(indexRequest, RequestOptions.DEFAULT)) {
            LOG.info("Creating index {} ", indexName)
            val request = CreateIndexRequest(indexName).source(INTERNALAD_COMMON_SETTINGS, XContentType.JSON)
            val response = client.indices().create(request, RequestOptions.DEFAULT)
            LOG.info("CreateIndex request isAcknowledged: ${response.isAcknowledged} - SettingsJSON: $INTERNALAD_COMMON_SETTINGS")

            val putMappingRequest = PutMappingRequest(indexName).source(INTERNALAD_MAPPING, XContentType.JSON)
            val putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT)
            LOG.info("PutMapping request isAcknowledged: ${putMappingResponse.isAcknowledged} - MappingJSON: $INTERNALAD_MAPPING")

            return true
        }
        return false
    }

    fun updateAlias(indexName: String): Boolean {
        val remove = IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                .index("$INTERNALAD*")
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


    fun getAlias(): MutableMap<String, MutableSet<AliasMetadata>>? {
        val aliasesRequest = GetAliasesRequest(INTERNALAD)
        return client.indices().getAlias(aliasesRequest, RequestOptions.DEFAULT).aliases
    }


    fun index(ads: List<AdTransport>, indexName: String): IndexResponse {
        val bulkResponse = bulkIndex(ads, indexName)
        return IndexResponse(bulkResponse.hasFailures(),
                bulkResponse.status(),
                bulkResponse.items.size,
                bulkResponse.buildFailureMessage())
    }

    private fun bulkIndex(ads: List<AdTransport>, indexName: String): BulkResponse {
        val adStream = adPipelineFactory.toPipelineStream(ads)
        return  indexBulk(adStream, indexName)
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
        val sourceMap = searchResponse.hits.hits[0].sourceAsMap
        return LocalDateTime.parse(sourceMap["updated"] as String)
    }

    fun deleteOldAds() {
        val adsOlderThan = LocalDateTime.now().minusMonths(months)
        LOG.info("Deleting ads older than $adsOlderThan from index")
        val oldAdsRange = RangeQueryBuilder("updated").lt(adsOlderThan)
        val deleteRequest = DeleteByQueryRequest(INTERNALAD).apply {
            setQuery(oldAdsRange)
            batchSize = 1000
        }
        val response = client.deleteByQuery(deleteRequest, RequestOptions.DEFAULT)
        LOG.info("Deleted ${response.deleted} ads")
    }

    fun initAlias(indexName: String) {
        val aliasIndexRequest = GetAliasesRequest(INTERNALAD)
        val response = client.indices().getAlias(aliasIndexRequest, RequestOptions.DEFAULT)
        if ((response.status() == RestStatus.OK && response.aliases[indexName] == null) || response.status() == RestStatus.NOT_FOUND)  {
            LOG.warn("Alias ${INTERNALAD} is not pointing to $indexName, will try to update alias again")
            updateAlias(indexName)
        }
    }

}
data class IndexResponse(val hasFailures: Boolean,
                         val status: RestStatus,
                         val numItems: Int,
                         val failureMessage: String)
