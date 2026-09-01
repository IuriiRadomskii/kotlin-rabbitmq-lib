package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

internal class ConnectionDecorator(
    private val delegate: Connection
) {
    val id: String
    private val closed = AtomicBoolean(false)
    private val openChannelCount = AtomicInteger(0)

    init {
        id = delegate.id ?: UUID.randomUUID().toString()
    }

    val isOpen: Boolean
        get() = !closed.get() && delegate.isOpen

    val channelCount: Int
        get() = openChannelCount.get()

    val channelMax: Int
        get() = delegate.channelMax

    fun id(): String {
        return id
    }

    fun createChannel(): Channel {
        check(isOpen) { "ManagedConnection $id is closed" }
        return ChannelDecorator(
            delegate = delegate.createChannel(),
            onClose = { openChannelCount.decrementAndGet() }
        )
            .also { openChannelCount.incrementAndGet() }
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            runCatching { if (delegate.isOpen) delegate.close() }
        }
    }

    override fun toString(): String =
        "ConnectionDecorator(id=$id, isOpen=$isOpen, channels=$channelCount, address = ${delegate.address})"

}
