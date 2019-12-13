package no.nav.arbeidsplassen.internalad.indexer.index

import net.javacrumbs.shedlock.core.AbstractSimpleLock
import net.javacrumbs.shedlock.core.LockConfiguration
import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.core.SimpleLock
import net.javacrumbs.shedlock.support.LockException
import net.javacrumbs.shedlock.support.Utils.getHostname
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Instant
import java.util.*
import kotlin.collections.HashMap

// Copied, converted to kotlin and made 7.x compatible
class ElasticsearchLockProvider(val highLevelClient: RestHighLevelClient, val hostname: String = getHostname()) : LockProvider {

    companion object {
        const val SHEDLOCK_DEFAULT_INDEX = "shedlock"
        const val LOCK_UNTIL = "lockUntil"
        const val LOCKED_AT = "lockedAt"
        const val LOCKED_BY = "lockedBy"
        const val NAME = "name"
        private const val UPDATE_SCRIPT = "if (ctx._source." + LOCK_UNTIL + " <= " + "params." + LOCKED_AT + ") { " +
                "ctx._source." + LOCKED_BY + " = params." + LOCKED_BY + "; " +
                "ctx._source." + LOCKED_AT + " = params." + LOCKED_AT + "; " +
                "ctx._source." + LOCK_UNTIL + " =  params." + LOCK_UNTIL + "; " +
                "} else { " +
                "ctx.op = 'none' " +
                "}"
        private val LOG = LoggerFactory.getLogger(ElasticsearchLockProvider::class.java)
    }

    init {
        val indexRequest = GetIndexRequest(SHEDLOCK_DEFAULT_INDEX)
        if (!highLevelClient.indices().exists(indexRequest, RequestOptions.DEFAULT)) {
            LOG.info("Creating index {} ", SHEDLOCK_DEFAULT_INDEX)
            val request = CreateIndexRequest(SHEDLOCK_DEFAULT_INDEX)
            request.settings(Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 1)
            )
            highLevelClient.indices().create(request, RequestOptions.DEFAULT)
        }
    }

    override fun lock(lockConfiguration: LockConfiguration): Optional<SimpleLock> {
        return try {
            val lockObject = lockObject(lockConfiguration.name,
                    lockConfiguration.lockAtMostUntil,
                    now())
            val ur = UpdateRequest()
                    .index(SHEDLOCK_DEFAULT_INDEX)
                    .id(lockConfiguration.name)
                    .script(Script(ScriptType.INLINE,
                            "painless",
                            UPDATE_SCRIPT,
                            lockObject)
                    )
                    .upsert(lockObject)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            val res = highLevelClient.update(ur, RequestOptions.DEFAULT)
            if (res.result != DocWriteResponse.Result.NOOP) {
                Optional.of(ElasticsearchSimpleLock(lockConfiguration))
            } else {
                Optional.empty()
            }
        } catch (e: IOException) {
            if (e is ElasticsearchException && e.status() == RestStatus.CONFLICT) {
                Optional.empty()
            } else {
                throw LockException("Unexpected exception occurred", e)
            }
        } catch (e: ElasticsearchException) {
            if (e.status() == RestStatus.CONFLICT) {
                Optional.empty()
            } else {
                throw LockException("Unexpected exception occurred", e)
            }
        }
    }

    private fun now(): Instant {
        return Instant.now()
    }

    private fun lockObject(name: String, lockUntil: Instant, lockedAt: Instant): Map<String, Any> {
        val lock: MutableMap<String, Any> = HashMap()
        lock[NAME] = name
        lock[LOCKED_BY] = hostname
        lock[LOCKED_AT] = lockedAt.toEpochMilli()
        lock[LOCK_UNTIL] = lockUntil.toEpochMilli()
        return lock
    }

    private inner class ElasticsearchSimpleLock(lockConfiguration: LockConfiguration) : AbstractSimpleLock(lockConfiguration) {
        public override fun doUnlock() { // Set lockUtil to now or lockAtLeastUntil whichever is later
            try {
                val ur = UpdateRequest()
                        .index(SHEDLOCK_DEFAULT_INDEX)
                        .id(lockConfiguration.name)
                        .script(Script(ScriptType.INLINE,
                                "painless",
                                "ctx._source.lockUntil = params.unlockTime",
                                Collections.singletonMap<String, Any>("unlockTime", lockConfiguration.unlockTime.toEpochMilli())))
                highLevelClient.update(ur, RequestOptions.DEFAULT)
            } catch (e: IOException) {
                throw LockException("Unexpected exception occurred", e)
            } catch (e: ElasticsearchException) {
                throw LockException("Unexpected exception occurred", e)
            }
        }
    }
}