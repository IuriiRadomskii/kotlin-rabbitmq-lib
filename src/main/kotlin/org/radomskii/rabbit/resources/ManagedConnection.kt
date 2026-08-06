package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import org.radomskii.rabbit.config.ChannelPoolConfig
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Wraps a single RabbitMQ [Connection]. Owns a [ChannelPool] used by publishers for short-lived
 * pooled channels, and separately exposes [createDedicatedChannel] for consumer workers, which
 * need one long-lived channel bound to their own thread for their entire lifecycle.
 */
internal class ManagedConnection(
    private val connection: Connection,
    channelPoolConfig: ChannelPoolConfig
) {
    val id: String = UUID.randomUUID().toString()
    private val channelPool = ChannelPool({ connection.createChannel() }, channelPoolConfig)//TODO no need to pool channels. publish task need to executed and then channel need to closed
    private val closed = AtomicBoolean(false)

    val isOpen: Boolean
        get() = !closed.get() && connection.isOpen

    /**
     * Acquire a pooled channel for a short-lived publish operation.
     */
    fun acquireChannel(): ManagedChannel {
        check(isOpen) { "ManagedConnection $id is closed" }
        return channelPool.acquire()
    }

    /**
     * Create a new raw channel dedicated to the caller's exclusive, long-lived use (consumer workers).
     * Not tracked by the channel pool.
     */
    fun createDedicatedChannel(): Channel {
        check(isOpen) { "ManagedConnection $id is closed" }
        return connection.createChannel()
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            channelPool.close()
            runCatching { if (connection.isOpen) connection.close() }
        }
    }

    override fun toString(): String = "ManagedConnection(id=$id, isOpen=$isOpen)"
}
