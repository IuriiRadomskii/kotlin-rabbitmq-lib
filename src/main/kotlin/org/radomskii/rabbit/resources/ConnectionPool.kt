package org.radomskii.rabbit.resources

import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ReconnectionConfig
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors.newSingleThreadScheduledExecutor
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal class ConnectionPool(
    private val connectionFactory: ConnectionFactory,
    private val connectionCount: Int = 1,
    private val reconnectionConfig: ReconnectionConfig = ReconnectionConfig()
): Closeable {
    init {
        require(connectionCount > 0) { "connectionCount must be positive" }
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
    private val connectionRetryScheduler: ScheduledExecutorService = newSingleThreadScheduledExecutor(Thread.ofVirtual().factory())
    private var capacitySupervisorTask: ScheduledFuture<*>? = null

    fun init() {
        lifecycleLock.withLock {
            check(!closed.get()) { "ConnectionPool is closed" }
            if (!initialized.compareAndSet(false, true)) {
                log.trace("ConnectionPool already initialized, skipping")
                return
            }
            log.trace("Initializing connection pool: connectionCount={}", connectionCount)
            scheduleConnectionAttempt(
                numberOfAttempt = 1,
                onSuccess = { connections.add(it) }
            )
            val pollMillis = reconnectionConfig.retryInterval.toMillis()
            capacitySupervisorTask = scalingScheduler.scheduleWithFixedDelay(
                ::triggerScaleUpIfNearCapacity,
                pollMillis,
                pollMillis,
                TimeUnit.MILLISECONDS
            )
            log.trace("Capacity supervisor scheduled: pollIntervalMillis={}", pollMillis)
        }
    }

    fun nextConnection(): ConnectionDecorator {
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
                connectionRetryScheduler.shutdown()
                try {
                    scalingScheduler.awaitTermination(SCHEDULER_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS)
                    connectionRetryScheduler.awaitTermination(SCHEDULER_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS)
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

    private fun scheduleConnectionAttempt(
        numberOfAttempt: Int,
        onSuccess: (ConnectionDecorator) -> Unit,
        onDone: () -> Unit = {}
    ) {
        log.trace("Schedule connection task: attempts = $numberOfAttempt")
        connectionRetryScheduler.execute {
            if (closed.get()) {
                onDone()
                return@execute
            }
            try {
                val connection = ConnectionDecorator(connectionFactory.newConnection())
                if (closed.get()) {
                    connection.close()
                } else {
                    log.trace("Connection created: {}", connection)
                    onSuccess(connection)
                }
                onDone()
            } catch (e: Exception) {
                log.warn(
                    "Failed to open RabbitMQ connection (attempt {}/{})",
                    numberOfAttempt, reconnectionConfig.maxAttempts, e
                )
                if (numberOfAttempt < reconnectionConfig.maxAttempts) {
                    connectionRetryScheduler.schedule(
                        { scheduleConnectionAttempt(numberOfAttempt + 1, onSuccess, onDone) },
                        reconnectionConfig.retryInterval.toMillis(),
                        TimeUnit.MILLISECONDS
                    )
                } else {
                    log.warn(
                        "Giving up opening a RabbitMQ connection after {} attempts",
                        reconnectionConfig.maxAttempts
                    )
                    onDone()
                }
            }
        }
    }

    private fun triggerScaleUpIfNearCapacity() {
        if (closed.get()) return
        if (connections.size >= connectionCount) return
        if (connections.none { it.isOpen && isNearChannelCapacity(it) }) return
        if (!scalingInProgress.compareAndSet(false, true)) return

        log.trace("Scaling up connections")
        scheduleConnectionAttempt(
            numberOfAttempt = 1,
            onSuccess = { connections.add(it) },
            onDone = { scalingInProgress.set(false) }
        )
    }

    private fun isNearChannelCapacity(connection: ConnectionDecorator): Boolean {
        val channelMax = connection.channelMax
        if (channelMax == 0) return false
        return connection.channelCount >= channelMax * SCALE_UP_THRESHOLD_RATIO
    }

    private companion object {
        val log = LoggerFactory.getLogger(ConnectionPool::class.java)
        const val SCALE_UP_THRESHOLD_RATIO = 0.75
        const val SCHEDULER_SHUTDOWN_MILLIS = 2_000L
    }

}
