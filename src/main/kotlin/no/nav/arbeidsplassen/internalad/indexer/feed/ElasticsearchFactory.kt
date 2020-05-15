package no.nav.arbeidsplassen.internalad.indexer.feed

import io.micronaut.context.annotation.Factory
import io.micronaut.context.annotation.Value
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.DefaultHostnameVerifier
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import javax.inject.Singleton

@Factory
class ElasticsearchFactory(@Value("\${elasticsearch.url:http://localhost:9200}") val esUrl: String,
                           @Value("\${elasticsearch.user:foo}") val user: String,
                           @Value("\${elasticsearch.password:bar}") val password: String) {

    @Singleton
    fun feedTaskService(elasticsearchFeedRepository: ElasticsearchFeedRepository): FeedTaskService {
        return FeedTaskService(elasticsearchFeedRepository);
    }

    @Singleton
    fun restHigLevelClient(): RestHighLevelClient {
        val credentialsProvider: CredentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY,
                UsernamePasswordCredentials(user, password))

        return RestHighLevelClient(RestClient
                    .builder(HttpHost.create(esUrl))
                    .setRequestConfigCallback {
                        requestConfigBuilder: RequestConfig.Builder -> requestConfigBuilder
                            .setConnectionRequestTimeout(5000)
                            .setConnectTimeout(10000)
                            .setSocketTimeout(20000)
                    }
                    .setHttpClientConfigCallback { httpAsyncClientBuilder: HttpAsyncClientBuilder ->
                        httpAsyncClientBuilder
                                .setSSLHostnameVerifier(DefaultHostnameVerifier())
                                .setMaxConnTotal(256)
                                .setMaxConnPerRoute(256)
                                .setDefaultCredentialsProvider(credentialsProvider)
                        }
                )
    }
}
