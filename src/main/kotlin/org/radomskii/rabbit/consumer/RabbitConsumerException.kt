package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.RabbitClientException

class RabbitConsumerException(message: String, cause: Throwable? = null) : RabbitClientException(message, cause)
