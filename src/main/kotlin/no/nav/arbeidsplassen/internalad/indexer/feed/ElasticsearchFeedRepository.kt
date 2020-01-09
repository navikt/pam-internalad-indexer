package no.nav.arbeidsplassen.internalad.indexer.feed

import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import org.slf4j.LoggerFactory
import java.lang.Exception
import javax.inject.Singleton

@Singleton
class ElasticsearchFeedRepository(val client: RestHighLevelClient,
                                  val objectMapper: ObjectMapper) {

    companion object {
        private val FEEDTASK_INDEX = "feedtask-internalad"
        private val LOG = LoggerFactory.getLogger(ElasticsearchFeedRepository::class.java)
    }

    init {
        val indexRequest = GetIndexRequest(FEEDTASK_INDEX)
        if (!client.indices().exists(indexRequest, RequestOptions.DEFAULT)) {
            LOG.info("Creating index {} ", FEEDTASK_INDEX)
            val request = CreateIndexRequest(FEEDTASK_INDEX)
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
            )
            client.indices().create(request, RequestOptions.DEFAULT)
        }
    }

    fun save(task: FeedTask): FeedTask? {
        val request = IndexRequest(FEEDTASK_INDEX).apply {
            id(task.name)
            source(objectMapper.writeValueAsString(task), XContentType.JSON)
            refreshPolicy =  WriteRequest.RefreshPolicy.IMMEDIATE
        }
        val indexResponse = client.index(request, RequestOptions.DEFAULT)
        return if ( indexResponse.status() == RestStatus.OK ) task else null
    }

    fun findByFeedName(name: String): FeedTask? {
        val getRequest = GetRequest(FEEDTASK_INDEX, name)
        try {
            val response = client.get(getRequest, RequestOptions.DEFAULT)
            if (!response.isSourceEmpty) {
                return objectMapper.readValue(response.sourceAsString, FeedTask::class.java)
            }
        }
        catch (e: Exception) {
            LOG.error("Got exception while lookup: $name",e)
        }
        return null
    }


}