package org.radomskii.rabbit.config

import org.radomskii.rabbit.serialization.MessageSerializer
import java.time.Duration

/**
 * Configuration for a [org.radomskii.rabbit.consumer.RabbitConsumer].
 *
 * @param queues queues consumed from at startup
 * @param deserializer converts wire bytes back into the consumer's payload type
 * @param workerPoolSize number of dedicated worker threads (and channels), one per thread
 * @param prefetchCount QoS prefetch count applied to each worker's channel
 * @param autoAck whether messages are acknowledged automatically by the broker on delivery
 * @param queueCapacity capacity of the handoff queue between the broker delivery callback and a worker thread
 * @param gracefulShutdownTimeout maximum time [org.radomskii.rabbit.consumer.RabbitConsumer.stop] waits for in-flight handler calls to finish
 * @param supervisorPollInterval how often the supervisor checks worker health
 */
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
