package org.radomskii.rabbit.model

data class IncomingMessage<T>(
    val payload: T,
    val metadata: MessageMetadata,
    val exchange: String,
    val routingKey: String,
    val deliveryTag: Long,
    val redelivered: Boolean
)
