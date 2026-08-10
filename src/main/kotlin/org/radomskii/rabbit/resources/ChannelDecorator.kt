package org.radomskii.rabbit.resources

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.WriteListener
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

internal class ChannelDecorator(
    private val delegate: Channel

) : Channel by delegate {

    private val closed: AtomicBoolean = AtomicBoolean(false)

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        TODO("Not yet implemented")
    }

    override fun close(closeCode: Int, closeMessage: String?) {
        if (!closed.compareAndSet(false, true)) return
        TODO("Not yet implemented")
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