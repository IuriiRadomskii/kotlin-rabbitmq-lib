package org.radomskii.rabbit.resources

import com.rabbitmq.client.Address
import com.rabbitmq.client.ConnectionFactory
import org.radomskii.rabbit.config.ChannelPoolConfig
import org.radomskii.rabbit.config.ConnectionConfig
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Pool of [ManagedConnection]s opened eagerly at construction time, distributed round-robin.
 * Relies on the underlying RabbitMQ client's automatic connection/channel recovery to survive
 * network interruptions or broker restarts.
 */
internal class ConnectionPool(
    connectionConfig: ConnectionConfig,
    channelPoolConfig: ChannelPoolConfig
) {
    private val connections: List<ManagedConnection>
    private val roundRobin = AtomicInteger(0)
    private val closed = AtomicBoolean(false)

    init {
        val factory = buildFactory(connectionConfig)
        val addresses = connectionConfig.hosts.map { Address(it, connectionConfig.port) }
        connections = List(connectionConfig.connectionCount) {
            ManagedConnection(factory.newConnection(addresses), channelPoolConfig)
        }
    }

    /**
     * Return the next connection in round-robin order, skipping any that are currently closed.
     */
    fun nextConnection(): ManagedConnection {
        if (closed.get()) throw RabbitConnectionException("ConnectionPool is closed")

        val start = roundRobin.getAndIncrement()
        for (offset in connections.indices) {
            val candidate = connections[(start + offset).mod(connections.size)]
            if (candidate.isOpen) return candidate
        }
        throw RabbitConnectionException("No open connections available")
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            connections.forEach { it.close() }
        }
    }

    private fun buildFactory(config: ConnectionConfig): ConnectionFactory = ConnectionFactory().apply {
        username = config.username
        password = config.password
        virtualHost = config.virtualHost
        connectionTimeout = config.connectionTimeout.toMillis().toInt()
        requestedHeartbeat = config.heartbeatInterval.seconds.toInt()
        isAutomaticRecoveryEnabled = true
        isTopologyRecoveryEnabled = true
        networkRecoveryInterval = config.networkRecoveryInterval.toMillis()
    }
}
