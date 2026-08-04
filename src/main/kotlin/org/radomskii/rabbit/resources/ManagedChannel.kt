package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import java.io.Closeable
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A pooled RabbitMQ channel obtained from a [ChannelPool]. Intended to be used with Kotlin's
 * `use {}` (try-with-resources): closing it returns the channel to its pool, unless it was
 * [invalidate]d, in which case it is discarded instead.
 */
class ManagedChannel internal constructor(
    private val channel: Channel,
    private val pool: ChannelPool
) : Closeable {

    val id: String = UUID.randomUUID().toString()
    private val invalidated = AtomicBoolean(false)
    private val closed = AtomicBoolean(false)

    val isOpen: Boolean
        get() = channel.isOpen

    /**
     * The underlying RabbitMQ channel, for use by publishers within this module only.
     */
    internal fun rawChannel(): Channel {
        check(isOpen) { "ManagedChannel $id is closed" }
        return channel
    }

    /**
     * Marks this channel as broken so it is discarded (rather than returned to the pool) on [close].
     */
    internal fun invalidate() {
        invalidated.set(true)
    }

    override fun close() {
        if (closed.compareAndSet(false, true)) {
            if (invalidated.get()) {
                pool.discard(this)
            } else {
                pool.release(this)
            }
        }
    }

    internal fun closeQuietly() {
        runCatching { if (channel.isOpen) channel.close() }
    }

    override fun toString(): String = "ManagedChannel(id=$id, isOpen=$isOpen)"
}
