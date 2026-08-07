package org.radomskii.rabbit.resources

import org.radomskii.rabbit.RabbitClientException

/**
 * Thrown when a connection or channel cannot be obtained, e.g. no connection is open or the
 * pool isn't initialized.
 */
class RabbitConnectionException(message: String, cause: Throwable? = null) : RabbitClientException(message, cause)
