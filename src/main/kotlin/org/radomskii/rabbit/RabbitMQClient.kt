package org.radomskii.rabbit

import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.config.ReconnectionConfig
import org.radomskii.rabbit.consumer.RabbitConsumer
import org.radomskii.rabbit.publisher.RabbitPublisher
import org.radomskii.rabbit.resources.ConnectionPool
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

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
        log.trace("Initializing RabbitMQClient: connectionCount={}", connectionCount)
        connectionPool.init()
        log.trace("RabbitMQClient initialized")
    }

    fun <T> createPublisher(config: PublisherConfig<T>): RabbitPublisher<T> {
        check(!closed.get()) { "RabbitMQClient is closed" }
        val publisher = RabbitPublisher(connectionPool, config)
        publishers.add(publisher)
        log.trace("Publisher created: {}", publisher)
        return publisher
    }

    fun <T> createConsumer(config: ConsumerConfig<T>): RabbitConsumer<T> {
        check(!closed.get()) { "RabbitMQClient is closed" }
        val consumer = RabbitConsumer(connectionPool, config, reconnectionConfig)
        consumers.add(consumer)
        log.trace("Consumer created: {}", consumer)
        return consumer
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            log.trace("Closing RabbitMQClient: consumers={}", consumers.size)

            consumers.forEach { if (it.isRunning()) it.stop() }
            consumers.clear()

            connectionPool.close()
            log.trace("RabbitMQClient closed")
        } else {
            log.trace("RabbitMQClient already closed")
        }
    }

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
        private val log = LoggerFactory.getLogger(RabbitMQClient::class.java)

        @JvmStatic
        fun builder(): Builder = Builder()
    }
}
