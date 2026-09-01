package org.radomskii.rabbit.resources

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.WriteListener
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

internal class ChannelDecorator(
    private val delegate: Channel,
    private val onClose: () -> Unit

) : Channel by delegate {

    private val closed = AtomicBoolean(false)

    override fun isOpen(): Boolean {
        return closed.get().not() && delegate.isOpen
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            try {
                delegate.close()
            } finally {
                onClose.invoke()
            }
        }
    }

    override fun close(closeCode: Int, closeMessage: String?) {
        if (closed.compareAndSet(false, true)) {
            try {
                delegate.close(closeCode, closeMessage)
            } finally {
                onClose.invoke()
            }
        }
    }

    override fun abort() {
        if (closed.compareAndSet(false, true)) {
            try {
                delegate.abort()
            } finally {
                onClose.invoke()
            }
        }
    }

    override fun abort(closeCode: Int, closeMessage: String?) {
        if (closed.compareAndSet(false, true)) {
            try {
                delegate.abort(closeCode, closeMessage)
            } finally {
                onClose.invoke()
            }
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
        delegate.basicPublish(exchange, routingKey, mandatory, immediate, props, body, listener)
    }

}