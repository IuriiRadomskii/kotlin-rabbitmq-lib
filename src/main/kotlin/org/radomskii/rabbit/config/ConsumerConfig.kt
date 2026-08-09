package org.radomskii.rabbit.config

import org.radomskii.rabbit.serialization.MessageSerializer
import java.time.Duration

data class ConsumerConfig<T>(
    val queues: List<String>,
    val deserializer: MessageSerializer<T>,
    val workerPoolSize: Int = 1,
    val prefetchCount: Int = 1,
    val autoAck: Boolean = false,
    val queueCapacity: Int = 1000,
    val gracefulShutdownTimeout: Duration = Duration.ofSeconds(30),
    val supervisorPollInterval: Duration = Duration.ofSeconds(1)
) {
    init {
        require(queues.isNotEmpty()) { "queues must not be empty" }
        require(queues.none { it.isBlank() }) { "queues must not contain blank entries" }
        require(workerPoolSize > 0) { "workerPoolSize must be positive" }
        require(prefetchCount > 0) { "prefetchCount must be positive" }
        require(queueCapacity > 0) { "queueCapacity must be positive" }
        require(!gracefulShutdownTimeout.isNegative) { "gracefulShutdownTimeout must not be negative" }
        require(!supervisorPollInterval.isNegative) { "supervisorPollInterval must not be negative" }
    }
}
