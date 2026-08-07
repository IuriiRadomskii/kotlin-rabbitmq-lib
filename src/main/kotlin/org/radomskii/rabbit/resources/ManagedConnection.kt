package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Wraps a single RabbitMQ [Connection]. [createChannel] opens a fresh raw channel for each
 * caller: publishers close it immediately after publishing (one channel per publish), while
 * consumer workers keep theirs open for their entire lifecycle.
 */
internal class ManagedConnection(
    private val delegate: Connection
) {
    val id: String = UUID.randomUUID().toString()
    private val closed = AtomicBoolean(false)

    val isOpen: Boolean
        get() = !closed.get() && delegate.isOpen

    /**
     * Create a new raw channel. The caller owns its lifecycle and is responsible for closing it.
     */
    fun createChannel(): Channel {
        check(isOpen) { "ManagedConnection $id is closed" }
        return delegate.createChannel()
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            runCatching { if (delegate.isOpen) delegate.close() }
        }
    }

    override fun toString(): String = "ManagedConnection(id=$id, isOpen=$isOpen)"
}
