package org.radomskii.rabbit.config

import java.time.Duration

data class ReconnectionConfig(
    val maxAttempts: Int = 3,
    val retryInterval: Duration = Duration.ofSeconds(10)
) {
    init {
        require(maxAttempts > 0) { "maxAttempts must be positive" }
        require(!retryInterval.isNegative) { "retryInterval must not be negative" }
    }
}
