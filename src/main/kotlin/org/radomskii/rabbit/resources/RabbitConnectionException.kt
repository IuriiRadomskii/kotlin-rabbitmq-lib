package org.radomskii.rabbit.resources

import org.radomskii.rabbit.RabbitClientException

class RabbitConnectionException(message: String, cause: Throwable? = null) : RabbitClientException(message, cause)
