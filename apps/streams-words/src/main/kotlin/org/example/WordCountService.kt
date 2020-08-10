package org.example

import mu.KotlinLogging
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.kstream.TimeWindowedSerializer
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.ReadOnlyWindowStore
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToFlux
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

private val logger = KotlinLogging.logger {  }

@Service
class WordCountService(
    private val kafkaStreamsFactoryBean: StreamsBuilderFactoryBean,
    private val metadataService: MetadataService,
    private val kafkaConfig: KafkaConfig
) {
    fun getCountFromStore(word: String): Mono<WordCounter> {
        val hostInfo = metadataService.metadataForStore("word-count", word, Serdes.String().serializer())
        return if (isHostSame(hostInfo)) {
            val store = kafkaStreamsFactoryBean.kafkaStreams.store(
                StoreQueryParameters.fromNameAndType<ReadOnlyKeyValueStore<String, Long>>
                    ("word-count", QueryableStoreTypes.keyValueStore())
            )

            store?.get(word)?.let { count ->
                Mono.just(WordCounter(word, count = count))
            } ?: Mono.empty()
        } else {
            logger.warn("$word not found in current ${kafkaConfig.applicationServerHost}:${kafkaConfig.applicationServerPort}")
            val webClient = WebClient.builder()
                .baseUrl("http://${hostInfo.host()}:${hostInfo.port()}")
                .build()

            return try {
                webClient.get()
                    .uri("/state/count/$word")
                    .retrieve()
                    .bodyToMono()

            } catch (ex:Exception) {
                logger.error("Trying to retrieve word from a different host failed", ex)
                Mono.empty()
            }
        }
    }

    fun getCountFromWindowStore(
        word: String,
        from: Long,
        to: Long
    ): Flux<WordCounter> {
        val windowedWord: Windowed<String> = Windowed(word, TimeWindow(from, to))
        val hostInfo =
            metadataService.metadataForStore(
                "windowed-word-count",
                windowedWord,
                TimeWindowedSerializer<String>(StringSerializer())
            )

        logger.info("Host: ${hostInfo.host()}, Port: ${hostInfo.port()}")
        return if (isHostSame(hostInfo)) {
            val store = kafkaStreamsFactoryBean.kafkaStreams.store(
                StoreQueryParameters.fromNameAndType<ReadOnlyWindowStore<String, Long>>
                    ("windowed-word-count", QueryableStoreTypes.windowStore())
            )

            val start = Instant.ofEpochMilli(from)
            val end = Instant.ofEpochMilli(to)

            logger.info("Fetching from $start to $end")

            val result = mutableListOf<WordCounter>()
            val windowStoreIterator = store?.fetch(word, start, end)
            windowStoreIterator?.let {
                while (windowStoreIterator.hasNext()) {
                    val next = windowStoreIterator.next()
                    logger.info("Next is $next")
                    result.add(WordCounter("$word@${next.key}", next.value))
                }
            }
            Flux.fromIterable(result)
        } else {
            val webClient = WebClient.builder()
                .baseUrl("http://${hostInfo.host()}:${hostInfo.port()}")
                .build()

            return try {
                webClient.get()
                    .uri("/state/count/$word/$from/$to")
                    .retrieve()
                    .bodyToFlux()
            } catch (ex:Exception) {
                logger.error("Trying to retrieve word from a different host failed", ex)
                Flux.empty()
            }
        }
    }

    private fun isHostSame(hostInfo: HostInfo) =
        hostInfo.host() == kafkaConfig.applicationServerHost &&
                hostInfo.port() == kafkaConfig.applicationServerPort
}
