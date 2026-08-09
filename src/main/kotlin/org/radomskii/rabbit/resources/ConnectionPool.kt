package org.radomskii.rabbit.resources

import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ReconnectionConfig
import org.slf4j.LoggerFactory
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
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
) {
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
    private val connections = CopyOnWriteArrayList<ManagedConnection>()
    private val scalingScheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory())
    private val connectionRetryScheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory())
    private var capacitySupervisorTask: ScheduledFuture<*>? = null

    fun init() {
        lifecycleLock.withLock {
            check(!closed.get()) { "ConnectionPool is closed" }
            if (!initialized.compareAndSet(false, true)) return

            scheduleConnectionAttempt(attempt = 1, onSuccess = { connections.add(it) })

            val pollMillis = reconnectionConfig.retryInterval.toMillis()
            capacitySupervisorTask = scalingScheduler.scheduleWithFixedDelay(
                ::triggerScaleUpIfNearCapacity, pollMillis, pollMillis, TimeUnit.MILLISECONDS
            )
        }
    }

    fun nextConnection(): ManagedConnection {
        if (closed.get()) throw RabbitConnectionException("ConnectionPool is closed")
        if (!initialized.get()) throw RabbitConnectionException("ConnectionPool is not initialized")

        val snapshot = connections.toList()
        val start = roundRobin.getAndIncrement()
        for (offset in snapshot.indices) {
            val candidate = snapshot[(start + offset).mod(snapshot.size)]
            if (candidate.isOpen) return candidate
        }
        throw RabbitConnectionException("No open connections available")
    }

    fun close() {
        lifecycleLock.withLock {
            if (closed.compareAndSet(false, true)) {
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
            }
        }
    }

    private fun scheduleConnectionAttempt(
        attempt: Int,
        onSuccess: (ManagedConnection) -> Unit,
        onDone: () -> Unit = {}
    ) {
        connectionRetryScheduler.execute {
            if (closed.get()) {
                onDone()
                return@execute
            }
            try {
                val connection = ManagedConnection(connectionFactory.newConnection())
                if (closed.get()) {
                    connection.close()
                } else {
                    onSuccess(connection)
                }
                onDone()
            } catch (e: Exception) {
                log.warn(
                    "Failed to open RabbitMQ connection (attempt {}/{})",
                    attempt, reconnectionConfig.maxAttempts, e
                )
                if (attempt < reconnectionConfig.maxAttempts) {
                    connectionRetryScheduler.schedule(
                        { scheduleConnectionAttempt(attempt + 1, onSuccess, onDone) },
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

        scheduleConnectionAttempt(
            attempt = 1,
            onSuccess = { connections.add(it) },
            onDone = { scalingInProgress.set(false) }
        )
    }

    private fun isNearChannelCapacity(connection: ManagedConnection): Boolean {
        val channelMax = connection.channelMax
        if (channelMax <= 0) return false
        return connection.channelCount >= channelMax * SCALE_UP_THRESHOLD_RATIO
    }

    private companion object {
        val log = LoggerFactory.getLogger(ConnectionPool::class.java)
        const val SCALE_UP_THRESHOLD_RATIO = 0.75
        const val SCHEDULER_SHUTDOWN_MILLIS = 2_000L
    }

}
