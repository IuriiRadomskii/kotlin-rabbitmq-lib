package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.RabbitClientException

/**
 * Thrown when a consumer fails to start or its workers cannot be set up.
 */
class RabbitConsumerException(message: String, cause: Throwable? = null) : RabbitClientException(message, cause)
