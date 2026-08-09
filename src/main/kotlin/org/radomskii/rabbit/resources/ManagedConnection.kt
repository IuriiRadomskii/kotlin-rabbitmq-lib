package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

internal class ManagedConnection(
    private val delegate: Connection
) {
    val id: String = UUID.randomUUID().toString()
    private val closed = AtomicBoolean(false)

    val isOpen: Boolean
        get() = !closed.get() && delegate.isOpen

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
