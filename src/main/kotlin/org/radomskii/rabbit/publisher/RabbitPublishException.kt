package org.radomskii.rabbit.publisher

import org.radomskii.rabbit.RabbitClientException

class RabbitPublishException(
    exchange: String,
    routingKey: String,
    message: String,
    cause: Throwable? = null
) : RabbitClientException("$message (exchange='$exchange', routingKey='$routingKey')", cause)
