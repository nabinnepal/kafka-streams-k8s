package org.example

import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.state.HostInfo
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.stereotype.Service

@Service
class MetadataService(
    private val kafkaStreamsBuilderFactoryBean: StreamsBuilderFactoryBean
) {
    fun <K> metadataForStore(
        storeName: String,
        key: K,
        serializer: Serializer<K>
    ): HostInfo {
        val queryMetadataForKey =
            kafkaStreamsBuilderFactoryBean.kafkaStreams.queryMetadataForKey(storeName, key, serializer)
        return queryMetadataForKey.activeHost
    }
}