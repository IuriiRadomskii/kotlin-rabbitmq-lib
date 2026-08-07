package org.radomskii.rabbit.config

import java.time.Duration

/**
 * Retry strategy used when opening a connection to RabbitMQ fails, e.g. because the broker
 * is not yet available when the application starts.
 *
 * @param maxAttempts number of times to attempt opening a connection before giving up
 * @param retryInterval delay between attempts
 */
data class ReconnectionConfig(
    val maxAttempts: Int = 3,
    val retryInterval: Duration = Duration.ofSeconds(10)
) {
    init {
        require(maxAttempts > 0) { "maxAttempts must be positive" }
        require(!retryInterval.isNegative) { "retryInterval must not be negative" }
    }
}
