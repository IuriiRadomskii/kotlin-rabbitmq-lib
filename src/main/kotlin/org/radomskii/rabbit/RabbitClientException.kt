package org.radomskii.rabbit

/**
 * Base type for all exceptions thrown by this library.
 * Catch this type to handle any failure raised by connections, channels, publishers or consumers.
 */
open class RabbitClientException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
