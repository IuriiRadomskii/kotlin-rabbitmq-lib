package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

internal class ManagedConnection(
    private val delegate: Connection
) {
    val id: String = UUID.randomUUID().toString()
    private val closed = AtomicBoolean(false)
    private val openChannelCount = AtomicInteger(0)

    val isOpen: Boolean
        get() = !closed.get() && delegate.isOpen

    val channelCount: Int
        get() = openChannelCount.get()

    val channelMax: Int
        get() = delegate.channelMax

    fun createChannel(): Channel {
        check(isOpen) { "ManagedConnection $id is closed" }
        val channel = delegate.createChannel()
        openChannelCount.incrementAndGet()
        return closeNotifyingProxy(channel)
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            runCatching { if (delegate.isOpen) delegate.close() }
        }
    }

    //TODO implement using Wrapper/Decorator pattern
    private fun closeNotifyingProxy(channel: Channel): Channel {
        val notified = AtomicBoolean(false)
        val handler = InvocationHandler { _, method, args ->
            try {
                method.invoke(channel, *(args ?: EMPTY_ARGS))
            } catch (e: InvocationTargetException) {
                throw e.cause ?: e
            } finally {
                if (isCloseMethod(method.name) && notified.compareAndSet(false, true)) {
                    openChannelCount.decrementAndGet()
                }
            }
        }
        return Proxy.newProxyInstance(
            Channel::class.java.classLoader,
            arrayOf(Channel::class.java),
            handler
        ) as Channel
    }

    private fun isCloseMethod(name: String) = name == "close" || name == "abort"

    override fun toString(): String = "ManagedConnection(id=$id, isOpen=$isOpen, channels=$channelCount)"

    private companion object {
        val EMPTY_ARGS = emptyArray<Any?>()
    }
}
