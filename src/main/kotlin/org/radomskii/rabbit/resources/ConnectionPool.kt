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
    connectionConfig: ConnectionConfig,//TODO use com.rabbitmq.client.ConnectionFactory as a parameter, no need in ConnectionConfig class. client configure ConnectionFactory by her/him self
    channelPoolConfig: ChannelPoolConfig
) {
    private val connections: List<ManagedConnection>
    private val roundRobin = AtomicInteger(0)// TODO instead of round-robin use balancing by number of channels per connection at any time number of channels per connection should be almost equal. If number of channels approaching to threshold value so throw warn log. threshold Connection#channelMax
    private val closed = AtomicBoolean(false)

    init {
        //TODO need separate public method init which is locked by lifecycleLock
        //Need to make reconnections if rabbitmq is not available on application start
        //Reconnection strategy as separate config. Default 3 tries every 10 seconds
        //Separate AtomicBool initialized
        val factory = buildFactory(connectionConfig)
        val addresses = connectionConfig.hosts.map { Address(it, connectionConfig.port) }
        connections = List(connectionConfig.connectionCount) {
            //TODO no need to create connections eagerly. create new connection if number of channels per connection is about 75% of Connection#channelMax
            ManagedConnection(factory.newConnection(addresses), channelPoolConfig)
        }
    }

    /**
     * Return the next connection in round-robin order, skipping any that are currently closed.
     */
    fun nextConnection(): ManagedConnection {
        if (closed.get()) throw RabbitConnectionException("ConnectionPool is closed")//TODO or if initialized.get() == false

        val start = roundRobin.getAndIncrement()
        for (offset in connections.indices) {
            val candidate = connections[(start + offset).mod(connections.size)]
            if (candidate.isOpen) return candidate
        }
        throw RabbitConnectionException("No open connections available")
    }

    fun close() {//TODO close method under lifecycleLock
        if (closed.compareAndSet(false, true)) {
            connections.forEach { it.close() }//TODO there need to wait some time to all channels which is born by connection to be closed
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
