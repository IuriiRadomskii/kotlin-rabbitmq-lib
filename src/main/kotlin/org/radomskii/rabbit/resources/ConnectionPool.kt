package org.radomskii.rabbit.resources

import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ConnectionPoolConfig
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Delayed
import java.util.concurrent.DelayQueue
import java.util.concurrent.Executors.newSingleThreadScheduledExecutor
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class ConnectionPool internal constructor(
    private val connectionFactory: ConnectionFactory,
    private val connectionPoolConfig: ConnectionPoolConfig = ConnectionPoolConfig()
) : Closeable {
    init {
        require(connectionFactory.isAutomaticRecoveryEnabled) {
            "connectionFactory must have automatic recovery enabled (isAutomaticRecoveryEnabled = true) - " +
                    "ConnectionPool relies on it to survive network interruptions and broker restarts"
        }
    }

    private val lifecycleLock = ReentrantLock()
    private val initialized = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)
    private val scalingInProgress = AtomicBoolean(false)
    private val roundRobin = AtomicInteger(0)
    private val connections = CopyOnWriteArrayList<ConnectionDecorator>()
    private val scalingScheduler: ScheduledExecutorService = newSingleThreadScheduledExecutor(Thread.ofVirtual().factory())
    private val retryQueue = DelayQueue<ConnectionAttemptTask>()
    private val retryWorker = Thread.ofPlatform().name("connection-pool-retry-worker").unstarted { runRetryLoop() }
    private var capacitySupervisorTask: ScheduledFuture<*>? = null

    fun init() {
        lifecycleLock.withLock {
            check(!closed.get()) { "ConnectionPool is closed" }
            if (!initialized.compareAndSet(false, true)) {
                log.trace("ConnectionPool already initialized, skipping")
                return
            }
            log.trace("Initializing connection pool: connectionCount={}", connectionPoolConfig.connectionCount)
            retryWorker.start()
            enqueueConnectionAttempt(
                attemptNumber = 1,
                onSuccess = { connections.add(it) }
            )
            val pollMillis = connectionPoolConfig.reconnectionConfig.retryInterval.toMillis()
            capacitySupervisorTask = scalingScheduler.scheduleWithFixedDelay(
                ::triggerScaleUpIfNearCapacity,
                pollMillis,
                pollMillis,
                TimeUnit.MILLISECONDS
            )
            log.trace("Capacity supervisor scheduled: pollIntervalMillis={}", pollMillis)
        }
    }

    internal fun nextConnection(): ConnectionDecorator {
        if (closed.get()) throw RabbitConnectionException("ConnectionPool is closed")
        if (!initialized.get()) throw RabbitConnectionException("ConnectionPool is not initialized")

        val snapshot = connections.toList()
        val start = roundRobin.getAndIncrement()
        for (offset in snapshot.indices) {
            val candidate = snapshot[(start + offset).mod(snapshot.size)]
            if (candidate.isOpen) {
                log.trace("Selected connection from pool: {}", candidate)
                return candidate
            }
        }
        log.trace("No open connections available in pool: poolSize={}", snapshot.size)
        throw RabbitConnectionException("No open connections available")
    }

    override fun close() {
        lifecycleLock.withLock {
            if (closed.compareAndSet(false, true)) {
                log.trace("Closing connection pool: connections={}", connections.size)
                capacitySupervisorTask?.cancel(false)
                scalingScheduler.shutdown()
                retryWorker.interrupt()
                val schedulerShutdownMillis = connectionPoolConfig.schedulerShutdownTimeout.toMillis()
                try {
                    scalingScheduler.awaitTermination(schedulerShutdownMillis, TimeUnit.MILLISECONDS)
                    retryWorker.join(schedulerShutdownMillis)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
                connections.forEach { it.close() }
                connections.clear()
                log.trace("Connection pool closed")
            } else {
                log.trace("Connection pool already closed")
            }
        }
    }

    private fun enqueueConnectionAttempt(
        attemptNumber: Int,
        onSuccess: (ConnectionDecorator) -> Unit,
        onDone: () -> Unit = {},
        delay: Duration = Duration.ZERO
    ) {
        log.trace("Enqueueing connection attempt: attempt={}, delay={}", attemptNumber, delay)
        retryQueue.put(ConnectionAttemptTask(attemptNumber, onSuccess, onDone, delay))
    }

    private fun runRetryLoop() {
        while (!closed.get()) {
            val task = try {
                retryQueue.take()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            }
            processConnectionAttempt(task)
        }
    }

    private fun processConnectionAttempt(task: ConnectionAttemptTask) {
        if (closed.get()) {
            task.onDone()
            return
        }
        try {
            val connection = ConnectionDecorator(connectionFactory.newConnection())
            if (closed.get()) {
                connection.close()
            } else {
                log.trace("Connection created: {}", connection)
                task.onSuccess(connection)
            }
            task.onDone()
        } catch (e: Exception) {
            val reconnectionConfig = connectionPoolConfig.reconnectionConfig
            log.warn(
                "Failed to open RabbitMQ connection (attempt {}/{})",
                task.attemptNumber, reconnectionConfig.maxAttempts, e
            )
            if (task.attemptNumber < reconnectionConfig.maxAttempts) {
                enqueueConnectionAttempt(
                    attemptNumber = task.attemptNumber + 1,
                    onSuccess = task.onSuccess,
                    onDone = task.onDone,
                    delay = reconnectionConfig.retryInterval
                )
            } else {
                log.warn(
                    "Giving up opening a RabbitMQ connection after {} attempts",
                    reconnectionConfig.maxAttempts
                )
                task.onDone()
            }
        }
    }

    private fun triggerScaleUpIfNearCapacity() {
        if (closed.get()) return
        if (connections.size >= connectionPoolConfig.connectionCount) return
        if (connections.none { it.isOpen && isNearChannelCapacity(it) }) return
        if (!scalingInProgress.compareAndSet(false, true)) return

        log.trace("Scaling up connections")
        enqueueConnectionAttempt(
            attemptNumber = 1,
            onSuccess = { connections.add(it) },
            onDone = { scalingInProgress.set(false) }
        )
    }

    private fun isNearChannelCapacity(connection: ConnectionDecorator): Boolean {
        val channelMax = connection.channelMax
        if (channelMax == 0) return false
        return connection.channelCount >= channelMax * connectionPoolConfig.scaleUpThresholdRatio
    }

    class Builder {
        private var connectionFactory: ConnectionFactory? = null
        private var connectionPoolConfig: ConnectionPoolConfig = ConnectionPoolConfig()

        fun connectionFactory(factory: ConnectionFactory) = apply { this.connectionFactory = factory }

        fun connectionPoolConfig(config: ConnectionPoolConfig) = apply { this.connectionPoolConfig = config }

        fun build(): ConnectionPool {
            val resolvedConnectionFactory = requireNotNull(connectionFactory) { "connectionFactory must be set" }
            val pool = ConnectionPool(resolvedConnectionFactory, connectionPoolConfig)
            pool.init()
            return pool
        }
    }

    companion object {
        @JvmStatic
        fun builder(): Builder = Builder()

        private val log = LoggerFactory.getLogger(ConnectionPool::class.java)
    }

    private class ConnectionAttemptTask(
        val attemptNumber: Int,
        val onSuccess: (ConnectionDecorator) -> Unit,
        val onDone: () -> Unit,
        delay: Duration
    ) : Delayed {
        private val readyAtNanos = System.nanoTime() + delay.toNanos()

        override fun getDelay(unit: TimeUnit): Long =
            unit.convert(readyAtNanos - System.nanoTime(), TimeUnit.NANOSECONDS)

        override fun compareTo(other: Delayed): Int =
            getDelay(TimeUnit.NANOSECONDS).compareTo(other.getDelay(TimeUnit.NANOSECONDS))
    }

}
