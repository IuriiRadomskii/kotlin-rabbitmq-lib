package org.radomskii.rabbit.config

import java.time.Duration

/**
 * Configuration for the per-connection channel pool used by publishers.
 *
 * @param maxChannelsPerConnection maximum number of concurrently open channels per connection
 * @param acquisitionTimeout maximum time to wait for a channel to become available
 */
data class ChannelPoolConfig(
    val maxChannelsPerConnection: Int = 10,
    val acquisitionTimeout: Duration = Duration.ofSeconds(5)
) {
    init {
        require(maxChannelsPerConnection > 0) { "maxChannelsPerConnection must be positive" }
        require(!acquisitionTimeout.isNegative) { "acquisitionTimeout must not be negative" }
    }
}
