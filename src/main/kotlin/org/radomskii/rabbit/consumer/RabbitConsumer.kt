package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.ReconnectionConfig
import org.radomskii.rabbit.resources.ConnectionPool
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

    fun start(handler: MessageHandler<T>) {
        lifecycleLock.withLock {
            check(running.compareAndSet(false, true)) { "RabbitConsumer already started" }
            val newContainer = ConsumerWorkerContainer(connectionPool, config, handler, reconnectionConfig)
            container = newContainer
            newContainer.start()
        }
    }

    fun stop() {
        lifecycleLock.withLock {
            if (running.compareAndSet(true, false)) {
                container?.stop(config.gracefulShutdownTimeout)
                container = null
            }
        }
    }

    fun isRunning(): Boolean = running.get()
}
