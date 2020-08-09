package org.example

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics
import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import org.springframework.boot.ExitCodeGenerator
import org.springframework.boot.SpringApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBeanCustomizer
import java.time.Duration


private val logger = KotlinLogging.logger {}

@Configuration
@EnableKafkaStreams
@EnableConfigurationProperties
class WordCountConfiguration {
    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun defaultKafkaStreamsConfig(kafkaConfig: KafkaConfig): KafkaStreamsConfiguration {
        logger.info("Application Server: ${kafkaConfig.applicationServerHost} and Port: ${kafkaConfig.applicationServerPort}")

        return KafkaStreamsConfiguration(
            mutableMapOf<String, Any>(
                StreamsConfig.APPLICATION_ID_CONFIG to kafkaConfig.clientId,
                StreamsConfig.CLIENT_ID_CONFIG to kafkaConfig.clientId,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaConfig.bootstrapServers,
                StreamsConfig.APPLICATION_SERVER_CONFIG to "${kafkaConfig.applicationServerHost}:${kafkaConfig.applicationServerPort}",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG to Serdes.String().javaClass.name,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "latest",
                StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG to "DEBUG"
            )
        )
    }

    @Bean
    fun kafkaStreamsBeanCustomizer(
        applicationContext: ApplicationContext,
        meterRegistry: MeterRegistry
    ): StreamsBuilderFactoryBeanCustomizer {
        return StreamsBuilderFactoryBeanCustomizer { fb ->
            fb.setKafkaStreamsCustomizer { kafkaStreams ->
                kafkaStreams.setStateListener { newState, oldState ->
                    logger.debug("State transitioned from $oldState to $newState")
                    if (oldState == KafkaStreams.State.REBALANCING && newState == KafkaStreams.State.ERROR) {
                        logger.error("Kafka Streams transitioned to an ERROR state, exiting Spring Application")
                        SpringApplication.exit(applicationContext, ExitCodeGenerator { 1 })
                    }
                }

                KafkaStreamsMetrics(kafkaStreams).bindTo(meterRegistry)
            }
        }
    }

    @Bean
    fun stream(
        streamsBuilder: StreamsBuilder,
        kafkaConfig: KafkaConfig
    ): KTable<Windowed<String>, Long> {
        val textLines = streamsBuilder.stream(kafkaConfig.inputTopic, Consumed.with(Serdes.String(), Serdes.String()))

        val groupedByWord: KGroupedStream<String, String> = textLines
            .flatMapValues { value -> value.toLowerCase().split(" ") }
            .groupBy({ _, word -> word }, Grouped.with(Serdes.String(), Serdes.String()))

        groupedByWord.count(
            Materialized.`as`<String, Long, KeyValueStore<Bytes, ByteArray>>(
                "word-count"
            )
                .withValueSerde(Serdes.Long())
        )


        return groupedByWord.windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
            .count(
                Materialized.`as`<String, Long, WindowStore<Bytes, ByteArray>>("windowed-word-count")
                    .withValueSerde(Serdes.Long())
            )
    }
}