package org.radomskii.rabbit.resources

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ReconnectionConfig
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Pool of [ManagedConnection]s, distributed round-robin. Must be [init]ialized before
 * [nextConnection] can be used. Relies on the underlying RabbitMQ client's automatic
 * connection/channel recovery to survive network interruptions or broker restarts once open;
 * [init] itself retries per [ReconnectionConfig] to ride out a broker that is briefly
 * unavailable at application startup.
 *
 * @param connectionFactory configured by the caller (credentials, timeouts, heartbeat, etc.);
 *   must have automatic recovery enabled, since the pool relies on it for resilience
 * @param addresses broker addresses to connect to; when empty, connections are opened via
 *   [ConnectionFactory.newConnection] (no-arg), using the factory's own host/port
 * @param connectionCount number of physical connections to open and round-robin across
 */
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
    private val roundRobin = AtomicInteger(0)// TODO instead of round-robin use balancing by number of channels per connection at any time number of channels per connection should be almost equal. If number of channels approaching to threshold value so throw warn log. threshold Connection#channelMax
    private lateinit var connections: List<ManagedConnection>

    /**
     * Open [connectionCount] connections, retrying per [ReconnectionConfig] when the broker is
     * unavailable. Must be called once before [nextConnection] is used. Idempotent while the
     * pool stays open; throws if called after [close].
     */
    fun init() {
        lifecycleLock.withLock {
            check(!closed.get()) { "ConnectionPool is closed" }
            if (initialized.get()) return

            //TODO no need to create connections eagerly. create new connection if number of channels per connection is about 75% of Connection#channelMax
            val opened = mutableListOf<ManagedConnection>()
            try {
                repeat(connectionCount) {
                    opened.add(ManagedConnection(connectWithRetry()))
                }
            } catch (e: Exception) {
                opened.forEach { it.close() }
                throw e
            }
            connections = opened
            initialized.set(true)
        }
    }

    private fun connectWithRetry(): Connection {
        var lastError: Exception? = null
        repeat(reconnectionConfig.maxAttempts) { attempt ->
            try {
                return connectionFactory.newConnection()
            } catch (e: Exception) {
                lastError = e
                log.warn(
                    "Failed to open RabbitMQ connection (attempt {}/{})",
                    attempt + 1, reconnectionConfig.maxAttempts, e
                )
                if (attempt < reconnectionConfig.maxAttempts - 1) {
                    try {
                        Thread.sleep(reconnectionConfig.retryInterval.toMillis())
                    } catch (ie: InterruptedException) {
                        Thread.currentThread().interrupt()
                        throw RabbitConnectionException("Interrupted while retrying RabbitMQ connection", ie)
                    }
                }
            }
        }
        throw RabbitConnectionException(
            "Failed to open RabbitMQ connection after ${reconnectionConfig.maxAttempts} attempts",
            lastError
        )
    }

    /**
     * Return the next connection in round-robin order, skipping any that are currently closed.
     */
    fun nextConnection(): ManagedConnection {
        if (closed.get()) throw RabbitConnectionException("ConnectionPool is closed")
        if (!initialized.get()) throw RabbitConnectionException("ConnectionPool is not initialized")

        val start = roundRobin.getAndIncrement()
        for (offset in connections.indices) {
            val candidate = connections[(start + offset).mod(connections.size)]
            if (candidate.isOpen) return candidate
        }
        throw RabbitConnectionException("No open connections available")
    }

    fun close() {
        lifecycleLock.withLock {
            if (closed.compareAndSet(false, true) && ::connections.isInitialized) {
                connections.forEach { it.close() }
            }
        }
    }

    private companion object {
        val log = LoggerFactory.getLogger(ConnectionPool::class.java)
    }
}
