package no.nav.arbeidsplassen.internalad.indexer.index

import com.fasterxml.jackson.databind.ObjectMapper
import io.micronaut.context.annotation.Value
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
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.LoggerFactory
import java.time.LocalDateTime
import java.util.stream.Stream
import javax.inject.Singleton


@Singleton
class IndexerService(
                     val client: RestHighLevelClient,
                     val objectMapper: ObjectMapper,
                     @Value("\${indexer.ads.from:36}") val months: Long,
                     val adPipelineFactory: PipelineFactory,
                     @Value("\${indexer.indexname:internalad-1}") val indexName: String): AdIndexer {

    companion object {
        private val LOG = LoggerFactory.getLogger(IndexerService::class.java)
        private val INTERNALAD_COMMON_SETTINGS = IndexerService::class.java
                .getResource("/internalad-common.json").readText()
        private val INTERNALAD_MAPPING = IndexerService::class.java
                .getResource("/internalad-mapping.json").readText()
    }

    init {
        // create index if not exist
        val indexRequest = GetIndexRequest(indexName)
        if (!client.indices().exists(indexRequest, RequestOptions.DEFAULT)) {
            createIndex(indexName);
        }
        val aliasIndexRequest = GetIndexRequest(INTERNALAD)
        if (!client.indices().exists(aliasIndexRequest, RequestOptions.DEFAULT)) {
            updateAlias(indexName)
        }
        LOG.info("Will index to indexName $indexName")
    }

    override fun createIndex(indexName: String): Boolean {
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

    override fun index(ads: List<AdTransport>): IndexResponse {
        val bulkResponse = bulkIndex(ads)
        return IndexResponse(bulkResponse.hasFailures(),
                bulkResponse.status(),
                bulkResponse.items.size,
                bulkResponse.buildFailureMessage())
    }

    private fun bulkIndex(ads: List<AdTransport>): BulkResponse {
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


    override fun fetchLastUpdatedTimeForIndex(indexName: String): LocalDateTime {
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

}
