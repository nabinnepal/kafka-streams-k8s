package org.example

import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.kstream.TimeWindowedSerializer
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.kstream.internals.WindowedSerializer
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import java.time.Duration
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
            val webClient = WebClient.builder()
                .baseUrl("http://${hostInfo.host()}:${hostInfo.port()}")
                .build()

            return try {
                val entry = webClient.get()
                    .uri("/state/count/$word")
                    .retrieve()
                    .bodyToMono<Map<String, Any>>()
                    .toFuture()
                    .get()

                Pair(entry?.get("first") as String, (entry["second"] as Int).toLong())
            } catch (ex:Exception) {
                logger.error("Trying to retrieve word from a different host failed", ex)
                null
            }
        }
    }

    fun getCountFromWindowStore(
        word: String,
        from: Long,
        to: Long
    ): List<Pair<String, Long>> {
        val windowedWord: Windowed<String> = Windowed(word, TimeWindow(from, to))
        val hostInfo =
            metadataService.metadataForStore("windowed-word-count", windowedWord, TimeWindowedSerializer<String>(StringSerializer()))

        logger.info("Host: ${hostInfo.host()}, Port: ${hostInfo.port()}")
        return if (hostInfo.host() == kafkaConfig.applicationServerHost && hostInfo.port() == kafkaConfig.applicationServerPort) {
            val store = kafkaStreamsFactoryBean.kafkaStreams.store(
                StoreQueryParameters.fromNameAndType<ReadOnlyWindowStore<String, Long>>
                    ("windowed-word-count", QueryableStoreTypes.windowStore())
            )

            val start = Instant.ofEpochMilli(from)
            val end = Instant.ofEpochMilli(to)

            logger.info("Fetching from $start to $end")

            val result = mutableListOf<Pair<String, Long>>()
            val windowStoreIterator = store?.fetch(word, start, end)
            windowStoreIterator?.let {
                while (windowStoreIterator.hasNext()) {
                    val next = windowStoreIterator.next()
                    logger.info("Next is $next")
                    result.add(Pair("$word@${next.key}", next.value))
                }
            }

            result
        } else {
            val webClient = WebClient.builder()
                .baseUrl("http://${hostInfo.host()}:${hostInfo.port()}")
                .build()

            return try {
                return webClient.get()
                    .uri("/state/count/$word")
                    .retrieve()
                    .bodyToMono<List<Map<String, Any>>>()
                    .toFuture()
                    .get()
                    .map { entry ->
                        Pair(entry["first"] as String, (entry["second"] as Int).toLong())

                    }
            } catch (ex:Exception) {
                logger.error("Trying to retrieve word from a different host failed", ex)
                emptyList()
            }
        }
    }
}
