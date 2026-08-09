package org.radomskii.rabbit.model

import java.time.Duration
import java.time.Instant
import java.util.UUID

data class MessageMetadata(
    val messageId: String = UUID.randomUUID().toString(),
    val timestamp: Instant = Instant.now(),
    val headers: Map<String, Any> = emptyMap(),
    val correlationId: String? = null,
    val priority: Int? = null,
    val expiration: Duration? = null
) {
    init {
        require(messageId.isNotBlank()) { "messageId must not be blank" }
        priority?.let { require(it in 0..255) { "priority must be between 0 and 255" } }
    }
}
