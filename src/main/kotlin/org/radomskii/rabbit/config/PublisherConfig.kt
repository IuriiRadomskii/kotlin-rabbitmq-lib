package org.radomskii.rabbit.config

import org.radomskii.rabbit.serialization.MessageSerializer
import java.time.Duration

data class PublisherConfig<T>(
    val serializer: MessageSerializer<T>,
    val returnListenerTimeout: Duration = Duration.ofSeconds(5),
    val closeTimeout: Duration = Duration.ofSeconds(10)
) {
    init {
        require(!returnListenerTimeout.isNegative) { "returnListenerTimeout must not be negative" }
        require(!closeTimeout.isNegative) { "closeTimeout must not be negative" }
    }
}
