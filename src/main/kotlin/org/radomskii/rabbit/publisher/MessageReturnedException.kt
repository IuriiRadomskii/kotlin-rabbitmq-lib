package org.radomskii.rabbit.publisher

import org.radomskii.rabbit.RabbitClientException

/**
 * Thrown when a mandatory publish was returned by the broker because it could not be routed
 * to any queue.
 */
class MessageReturnedException(
    val replyCode: Int,
    val replyText: String,
    val exchange: String,
    val routingKey: String
) : RabbitClientException(
    "Message returned by broker: $replyText (code=$replyCode, exchange='$exchange', routingKey='$routingKey')"
)
