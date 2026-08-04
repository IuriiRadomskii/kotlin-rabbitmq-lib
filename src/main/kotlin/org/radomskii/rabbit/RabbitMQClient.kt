package org.radomskii.rabbit

import org.radomskii.rabbit.config.ChannelPoolConfig
import org.radomskii.rabbit.config.ConnectionConfig
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.consumer.RabbitConsumer
import org.radomskii.rabbit.publisher.RabbitPublisher
import org.radomskii.rabbit.resources.ConnectionPool
import java.io.Closeable
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Entry point for this library. Owns the underlying connection pool and creates publishers
 * and consumers that share it. Build one with [builder], create publishers/consumers from it,
 * and [close] it once when the application shuts down to release all network resources.
 */
class RabbitMQClient private constructor(
    connectionConfig: ConnectionConfig,
    channelPoolConfig: ChannelPoolConfig
) : Closeable {

    private val connectionPool = ConnectionPool(connectionConfig, channelPoolConfig)
    private val publishers = CopyOnWriteArrayList<RabbitPublisher<*>>()
    private val consumers = CopyOnWriteArrayList<RabbitConsumer<*>>()
    private val closed = AtomicBoolean(false)

    /**
     * Create a new publisher sharing this client's connection pool.
     */
    fun <T> createPublisher(config: PublisherConfig<T>): RabbitPublisher<T> {
        check(!closed.get()) { "RabbitMQClient is closed" }
        val publisher = RabbitPublisher(connectionPool, config)
        publishers.add(publisher)
        return publisher
    }

    /**
     * Create a new consumer sharing this client's connection pool. Call [RabbitConsumer.start]
     * to begin consuming.
     */
    fun <T> createConsumer(config: ConsumerConfig<T>): RabbitConsumer<T> {
        check(!closed.get()) { "RabbitMQClient is closed" }
        val consumer = RabbitConsumer(connectionPool, config)
        consumers.add(consumer)
        return consumer
    }

    /**
     * Close every publisher and consumer created by this client (draining in-flight work first),
     * then close all pooled connections and channels. Idempotent.
     */
    override fun close() {
        if (closed.compareAndSet(false, true)) {
            publishers.forEach { it.close() }
            publishers.clear()

            consumers.forEach { if (it.isRunning()) it.stop() }
            consumers.clear()

            connectionPool.close()
        }
    }

    /**
     * Builds a [RabbitMQClient].
     */
    class Builder {
        private var connectionConfig: ConnectionConfig? = null
        private var channelPoolConfig: ChannelPoolConfig = ChannelPoolConfig()

        fun connectionConfig(config: ConnectionConfig) = apply { this.connectionConfig = config }

        fun channelPoolConfig(config: ChannelPoolConfig) = apply { this.channelPoolConfig = config }

        fun build(): RabbitMQClient {
            val resolvedConnectionConfig = requireNotNull(connectionConfig) { "connectionConfig must be set" }
            return RabbitMQClient(resolvedConnectionConfig, channelPoolConfig)
        }
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
