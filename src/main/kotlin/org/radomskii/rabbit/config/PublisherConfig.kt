package org.radomskii.rabbit.config

import org.radomskii.rabbit.serialization.MessageSerializer
import java.time.Duration

/**
 * Configuration for a [org.radomskii.rabbit.publisher.RabbitPublisher]. Exchange and routing key
 * are intentionally not part of this configuration - they are supplied per call to `publish`.
 *
 * @param serializer converts the publisher's payload type into wire bytes
 * @param mandatory whether published messages must be routable; when true, publishing waits for
 *   a broker return notification (bounded by [returnListenerTimeout]) before the channel is released
 * @param returnListenerTimeout maximum time to wait for a return notification when [mandatory] is true
 * @param closeTimeout maximum time [org.radomskii.rabbit.publisher.RabbitPublisher.close] waits for in-flight publishes to finish
 */
data class PublisherConfig<T>(
    val serializer: MessageSerializer<T>,
    val mandatory: Boolean = false,
    val returnListenerTimeout: Duration = Duration.ofSeconds(5),
    val closeTimeout: Duration = Duration.ofSeconds(10)
) {
    init {
        require(!returnListenerTimeout.isNegative) { "returnListenerTimeout must not be negative" }
        require(!closeTimeout.isNegative) { "closeTimeout must not be negative" }
    }
}
