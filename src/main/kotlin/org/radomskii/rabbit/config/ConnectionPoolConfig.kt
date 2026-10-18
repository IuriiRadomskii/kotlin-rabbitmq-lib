package org.radomskii.rabbit.config

import java.time.Duration

data class ConnectionPoolConfig(
    val connectionCount: Int = 1,
    val reconnectionConfig: ReconnectionConfig = ReconnectionConfig(),
    val scaleUpThresholdRatio: Double = 0.75,
    val schedulerShutdownTimeout: Duration = Duration.ofMillis(2_000)
) {
    init {
        require(connectionCount > 0) { "connectionCount must be positive" }
        require(scaleUpThresholdRatio > 0.0 && scaleUpThresholdRatio <= 1.0) {
            "scaleUpThresholdRatio must be in (0.0, 1.0]"
        }
        require(!schedulerShutdownTimeout.isNegative) { "schedulerShutdownTimeout must not be negative" }
    }
}
