package org.radomskii.rabbit.resources

import org.radomskii.rabbit.config.ChannelPoolConfig
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Bounds the number of concurrently open channels created from a single connection and
 * reuses idle channels across [acquire]/[release] cycles.
 *
 * @param channelFactory creates a new raw channel; invoked while a permit is already held
 */
internal class ChannelPool(
    private val channelFactory: () -> com.rabbitmq.client.Channel,
    private val config: ChannelPoolConfig
) {
    private val permits = Semaphore(config.maxChannelsPerConnection)
    private val idle = ConcurrentLinkedQueue<ManagedChannel>()
    private val closed = AtomicBoolean(false)

    /**
     * Acquire a channel, reusing an idle one if available, otherwise creating a new one up to
     * [ChannelPoolConfig.maxChannelsPerConnection]. Blocks up to [ChannelPoolConfig.acquisitionTimeout].
     */
    fun acquire(): ManagedChannel {
        check(!closed.get()) { "ChannelPool is closed" }

        val acquired = permits.tryAcquire(config.acquisitionTimeout.toMillis(), TimeUnit.MILLISECONDS)
        if (!acquired) {
            throw RabbitConnectionException("Timed out acquiring a channel from the pool")
        }

        try {
            var candidate = idle.poll()
            while (candidate != null && !candidate.isOpen) {
                candidate = idle.poll()
            }
            return candidate ?: ManagedChannel(channelFactory(), this)
        } catch (e: Exception) {
            permits.release()
            throw RabbitConnectionException("Failed to acquire a channel", e)
        }
    }

    internal fun release(channel: ManagedChannel) {
        if (closed.get() || !channel.isOpen) {
            channel.closeQuietly()
        } else {
            idle.offer(channel)
        }
        permits.release()
    }

    internal fun discard(channel: ManagedChannel) {
        channel.closeQuietly()
        permits.release()
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            while (true) {
                val channel = idle.poll() ?: break
                channel.closeQuietly()
            }
        }
    }
}
