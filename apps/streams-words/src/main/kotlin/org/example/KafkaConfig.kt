package org.example

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConfigurationProperties(prefix = "kafka")
@ConstructorBinding
data class KafkaConfig(
    var inputTopic: String,
    var clientId: String,
    var bootstrapServers: String,
    var applicationServerHost: String,
    var applicationServerPort: Int
)