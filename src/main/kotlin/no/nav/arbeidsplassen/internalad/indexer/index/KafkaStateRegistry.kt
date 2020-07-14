package no.nav.arbeidsplassen.internalad.indexer.index

import javax.inject.Singleton

@Singleton
class KafkaStateRegistry {

    private val stateRegistry = hashMapOf<String, KafkaState>()

    fun setConsumerToError(consumer:String) {
        stateRegistry[consumer] = KafkaState(consumer, State.ERROR)
    }

    fun setConsumerToPaused(consumer: String) {
        stateRegistry[consumer] = KafkaState(consumer, State.PAUSED)
    }

    fun isError(): Boolean {
       return (stateRegistry.isNotEmpty() && stateRegistry.values.isNotEmpty())
    }

}

data class KafkaState(val consumer: String, val state: State)

enum class State {
    RUNNING, PAUSED, ERROR
}


