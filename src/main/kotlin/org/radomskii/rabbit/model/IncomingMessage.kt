package org.radomskii.rabbit.model

/**
 * Wrapper delivered to a [org.radomskii.rabbit.consumer.MessageHandler] for a single message
 * received from RabbitMQ, holding the deserialized payload alongside its metadata and
 * delivery information needed to acknowledge it.
 */
data class IncomingMessage<T>(
    val payload: T,
    val metadata: MessageMetadata,
    val exchange: String,
    val routingKey: String,
    val deliveryTag: Long,
    val redelivered: Boolean
)
