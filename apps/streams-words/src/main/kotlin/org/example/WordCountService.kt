package org.example

import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import java.time.Instant

private val logger = KotlinLogging.logger {  }

@Service
class WordCountService(
    private val kafkaStreamsFactoryBean: StreamsBuilderFactoryBean,
    private val metadataService: MetadataService,
    private val kafkaConfig: KafkaConfig
) {
    fun getCountFromStore(word: String): Pair<String, Long>? {
        val hostInfo = metadataService.metadataForStore("word-count", word, Serdes.String().serializer())
        return if (hostInfo.host() == kafkaConfig.applicationServerHost &&
            hostInfo.port() == kafkaConfig.applicationServerPort) {
            val store = kafkaStreamsFactoryBean.kafkaStreams.store(
                StoreQueryParameters.fromNameAndType<ReadOnlyKeyValueStore<String, Long>>
                    ("word-count", QueryableStoreTypes.keyValueStore())
            )

            store?.get(word)?.let { count ->
                Pair(word, count)
            }
        } else {
            logger.warn("$word not found in current  ${kafkaConfig.applicationServerHost}:${kafkaConfig.applicationServerPort}")
            null
        }
    }

    fun getCountFromWindowStore(
        word: String,
        from: Long,
        to: Long
    ): List<Pair<String, Long>> {
        val hostInfo =
            metadataService.metadataForStore("windowed-word-count", word, Serdes.String().serializer())
        return if (hostInfo.host() == kafkaConfig.applicationServerHost && hostInfo.port() == kafkaConfig.applicationServerPort) {
            val store = kafkaStreamsFactoryBean.kafkaStreams.store(
                StoreQueryParameters.fromNameAndType<ReadOnlyWindowStore<String, Long>>
                    ("windowed-word-count", QueryableStoreTypes.windowStore())
            )

            val windowStoreIterator = store?.fetch(word, Instant.ofEpochMilli(from), Instant.ofEpochMilli(to))
            val result = mutableListOf<Pair<String, Long>>()
            val hasNext = windowStoreIterator?.hasNext()
            while (hasNext != null && hasNext) {
                val next: KeyValue<Long, Long> = windowStoreIterator.next()
                result.add(Pair("$word@${next.key}", next.value))
            }
            result
        } else {
            emptyList()
        }
    }
}
