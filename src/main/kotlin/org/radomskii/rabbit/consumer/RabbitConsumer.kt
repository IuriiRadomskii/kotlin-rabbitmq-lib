package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.ReconnectionConfig
import org.radomskii.rabbit.resources.ConnectionPool
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Consumes messages from the queues configured in [ConsumerConfig]. Not started until [start]
 * is called with a [MessageHandler]. Thread-safe: [start]/[stop]/[isRunning] may be called from
 * any thread, but a single instance can only be started once (create a new instance to restart
 * with a different handler).
 *
 * @param T type of the message payload
 */
class RabbitConsumer<T> internal constructor(
    private val connectionPool: ConnectionPool,
    private val config: ConsumerConfig<T>,
    private val reconnectionConfig: ReconnectionConfig = ReconnectionConfig()
) {
    private val lifecycleLock = ReentrantLock()
    private val running = AtomicBoolean(false)
    private var container: ConsumerWorkerContainer<T>? = null

    /**
     * Start consuming, dispatching each received message to [handler] on a dedicated worker thread.
     */
    fun start(handler: MessageHandler<T>) {
        lifecycleLock.withLock {
            check(running.compareAndSet(false, true)) { "RabbitConsumer already started" }
            val newContainer = ConsumerWorkerContainer(connectionPool, config, handler, reconnectionConfig)
            container = newContainer
            newContainer.start()
        }
    }

    /**
     * Stop consuming, waiting up to [timeout] for in-flight [MessageHandler] invocations to
     * finish before closing worker channels. Idempotent.
     */
    @JvmOverloads
    fun stop(timeout: Duration = config.gracefulShutdownTimeout) {
        lifecycleLock.withLock {
            if (running.compareAndSet(true, false)) {
                container?.stop(timeout)
                container = null
            }
        }
    }

    fun isRunning(): Boolean = running.get()
}
