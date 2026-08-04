package org.radomskii.rabbit.publisher

import org.radomskii.rabbit.RabbitClientException

/**
 * Thrown when a message could not be published due to a connection, channel or broker failure.
 */
class RabbitPublishException(
    val exchange: String,
    val routingKey: String,
    message: String,
    cause: Throwable? = null
) : RabbitClientException("$message (exchange='$exchange', routingKey='$routingKey')", cause)
