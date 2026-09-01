package org.radomskii.rabbit.resources

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.WriteListener
import org.slf4j.LoggerFactory
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

internal class ChannelDecorator(
    private val delegate: Channel,
    private val onClose: () -> Unit

) : Channel by delegate {

    private val closed = AtomicBoolean(false)

    private companion object {
        val log = LoggerFactory.getLogger(ChannelDecorator::class.java)
    }

    override fun isOpen(): Boolean {
        return closed.get().not() && delegate.isOpen
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            log.trace("Closing channel {}", delegate.channelNumber)
            try {
                delegate.close()
            } finally {
                onClose.invoke()
                log.trace("Channel {} closed", delegate.channelNumber)
            }
        } else {
            log.trace("Channel {} already closed", delegate.channelNumber)
        }
    }

    override fun close(closeCode: Int, closeMessage: String?) {
        if (closed.compareAndSet(false, true)) {
            log.trace("Closing channel {}: closeCode={}, closeMessage={}", delegate.channelNumber, closeCode, closeMessage)
            try {
                delegate.close(closeCode, closeMessage)
            } finally {
                onClose.invoke()
                log.trace("Channel {} closed", delegate.channelNumber)
            }
        } else {
            log.trace("Channel {} already closed", delegate.channelNumber)
        }
    }

    override fun abort() {
        if (closed.compareAndSet(false, true)) {
            log.trace("Aborting channel {}", delegate.channelNumber)
            try {
                delegate.abort()
            } finally {
                onClose.invoke()
                log.trace("Channel {} aborted", delegate.channelNumber)
            }
        } else {
            log.trace("Channel {} already closed", delegate.channelNumber)
        }
    }

    override fun abort(closeCode: Int, closeMessage: String?) {
        if (closed.compareAndSet(false, true)) {
            log.trace("Aborting channel {}: closeCode={}, closeMessage={}", delegate.channelNumber, closeCode, closeMessage)
            try {
                delegate.abort(closeCode, closeMessage)
            } finally {
                onClose.invoke()
                log.trace("Channel {} aborted", delegate.channelNumber)
            }
        } else {
            log.trace("Channel {} already closed", delegate.channelNumber)
        }
    }

    override fun basicPublish(
        exchange: String?,
        routingKey: String?,
        mandatory: Boolean,
        immediate: Boolean,
        props: AMQP.BasicProperties?,
        body: ByteBuffer?,
        listener: WriteListener?
    ) {
        log.trace("basicPublish on channel {}: exchange={}, routingKey={}", delegate.channelNumber, exchange, routingKey)
        delegate.basicPublish(exchange, routingKey, mandatory, immediate, props, body, listener)
    }

}