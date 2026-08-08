package org.radomskii.rabbit

import com.rabbitmq.client.Address
import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.config.ReconnectionConfig
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
    connectionFactory: ConnectionFactory,
    connectionCount: Int,
    private val reconnectionConfig: ReconnectionConfig
) : Closeable {

    private val connectionPool = ConnectionPool(connectionFactory, connectionCount, reconnectionConfig)
    private val publishers = CopyOnWriteArrayList<RabbitPublisher<*>>()
    private val consumers = CopyOnWriteArrayList<RabbitConsumer<*>>()
    private val closed = AtomicBoolean(false)

    init {
        connectionPool.init()
    }

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
        val consumer = RabbitConsumer(connectionPool, config, reconnectionConfig)
        consumers.add(consumer)
        return consumer
    }

    /**
     * Close every publisher and consumer created by this client (draining in-flight work first),
     * then close all pooled connections. Idempotent.
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
        private var connectionFactory: ConnectionFactory? = null
        private var connectionCount: Int = 1
        private var reconnectionConfig: ReconnectionConfig = ReconnectionConfig()

        fun connectionFactory(factory: ConnectionFactory) = apply { this.connectionFactory = factory }

        fun connectionCount(count: Int) = apply { this.connectionCount = count }

        fun reconnectionConfig(config: ReconnectionConfig) = apply { this.reconnectionConfig = config }

        fun build(): RabbitMQClient {
            val resolvedConnectionFactory = requireNotNull(connectionFactory) { "connectionFactory must be set" }
            return RabbitMQClient(resolvedConnectionFactory, connectionCount, reconnectionConfig)
        }
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
