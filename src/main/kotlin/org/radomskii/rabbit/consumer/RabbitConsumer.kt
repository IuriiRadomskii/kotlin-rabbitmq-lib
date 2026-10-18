package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.resources.ConnectionPool
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RabbitConsumer<T> internal constructor(
    private val connectionPool: ConnectionPool,
    private val config: ConsumerConfig<T>
) {
    private val lifecycleLock = ReentrantLock()
    private val running = AtomicBoolean(false)
    private var container: ConsumerWorkerContainer<T>? = null

    fun start(handler: MessageHandler<T>) {
        lifecycleLock.withLock {
            log.trace("Starting RabbitConsumer: queues={}, workerPoolSize={}", config.queues, config.workerPoolSize)
            check(running.compareAndSet(false, true)) { "RabbitConsumer already started" }
            val newContainer = ConsumerWorkerContainer(connectionPool, config, handler)
            container = newContainer
            newContainer.start()
            log.trace("RabbitConsumer started: queues={}", config.queues)
        }
    }

    fun stop() {
        lifecycleLock.withLock {
            if (running.compareAndSet(true, false)) {
                log.trace("Stopping RabbitConsumer: queues={}", config.queues)
                container?.stop(config.gracefulShutdownTimeout)
                container = null
                log.trace("RabbitConsumer stopped: queues={}", config.queues)
            } else {
                log.trace("RabbitConsumer already stopped: queues={}", config.queues)
            }
        }
    }

    fun isRunning(): Boolean = running.get()

    class Builder<T> {
        private var connectionPool: ConnectionPool? = null
        private var config: ConsumerConfig<T>? = null

        fun connectionPool(pool: ConnectionPool) = apply { this.connectionPool = pool }

        fun config(config: ConsumerConfig<T>) = apply { this.config = config }

        fun build(): RabbitConsumer<T> {
            val resolvedConnectionPool = requireNotNull(connectionPool) { "connectionPool must be set" }
            val resolvedConfig = requireNotNull(config) { "config must be set" }
            return RabbitConsumer(resolvedConnectionPool, resolvedConfig)
        }
    }

    companion object {
        @JvmStatic
        fun <T> builder(): Builder<T> = Builder()

        private val log = LoggerFactory.getLogger(RabbitConsumer::class.java)
    }
}
