package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.ReconnectionConfig
import org.radomskii.rabbit.resources.ConnectionPool
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class RabbitConsumer<T> internal constructor(
    private val connectionPool: ConnectionPool,
    private val config: ConsumerConfig<T>,
    private val reconnectionConfig: ReconnectionConfig = ReconnectionConfig()
) {
    private val lifecycleLock = ReentrantLock()
    private val running = AtomicBoolean(false)
    private var container: ConsumerWorkerContainer<T>? = null

    private companion object {
        val log = LoggerFactory.getLogger(RabbitConsumer::class.java)
    }

    fun start(handler: MessageHandler<T>) {
        lifecycleLock.withLock {
            log.trace("Starting RabbitConsumer: queues={}, workerPoolSize={}", config.queues, config.workerPoolSize)
            check(running.compareAndSet(false, true)) { "RabbitConsumer already started" }
            val newContainer = ConsumerWorkerContainer(connectionPool, config, handler, reconnectionConfig)
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
}
