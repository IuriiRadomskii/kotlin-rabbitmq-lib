package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

internal class ConnectionDecorator(
    private val delegate: Connection
) {
    val id: String
    private val closed = AtomicBoolean(false)
    private val openChannelCount = AtomicInteger(0)

    private companion object {
        val log = LoggerFactory.getLogger(ConnectionDecorator::class.java)
    }

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
        log.trace("Creating channel on connection {}", id)
        return ChannelDecorator(
            delegate = delegate.createChannel(),
            onClose = { openChannelCount.decrementAndGet() }
        )
            .also {
                openChannelCount.incrementAndGet()
                log.trace("Channel created on connection {}: channelNumber={}, openChannels={}", id, it.channelNumber, channelCount)
            }
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            log.trace("Closing connection {}", id)
            runCatching { if (delegate.isOpen) delegate.close() }
            log.trace("Connection {} closed", id)
        } else {
            log.trace("Connection {} already closed", id)
        }
    }

    override fun toString(): String =
        "ConnectionDecorator(id=$id, isOpen=$isOpen, channels=$channelCount, address = ${delegate.address})"

}
