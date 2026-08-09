package org.radomskii.rabbit.publisher

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Return
import com.rabbitmq.client.ShutdownSignalException
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.model.MessageMetadata
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.resources.ConnectionPool
import java.io.IOException
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class RabbitPublisher<T> internal constructor(
    private val connectionPool: ConnectionPool,
    private val config: PublisherConfig<T>
) {
    private val closed = AtomicBoolean(false)
    private val inFlight = AtomicInteger(0)
    private val closeLock = Object()

    fun publish(exchange: String, routingKey: String, payload: T, metadata: MessageMetadata = MessageMetadata()) {
        inFlight.incrementAndGet()
        try {
            check(!closed.get()) { "RabbitPublisher is closed" }

            val connection = try {
                connectionPool.nextConnection()
            } catch (e: Exception) {
                throw RabbitPublishException(exchange, routingKey, "Failed to obtain a connection", e)
            }

            val channel = try {
                connection.createChannel()
            } catch (e: Exception) {
                throw RabbitPublishException(exchange, routingKey, "Failed to create a channel", e)
            }

            try {
                val body = config.serializer.serialize(payload)
                val properties = buildProperties(metadata, body).build()

                if (config.mandatory) {
                    publishMandatory(channel, exchange, routingKey, properties, body.bytes)
                } else {
                    channel.basicPublish(exchange, routingKey, false, properties, body.bytes)
                }
            } catch (e: MessageReturnedException) {
                throw e
            } catch (e: IOException) {
                throw RabbitPublishException(exchange, routingKey, "Failed to publish message", e)
            } catch (e: ShutdownSignalException) {
                throw RabbitPublishException(exchange, routingKey, "Failed to publish message", e)
            } finally {
                runCatching { if (channel.isOpen) channel.close() }
            }
        } finally {
            synchronized(closeLock) {
                if (inFlight.decrementAndGet() == 0) {
                    closeLock.notifyAll()
                }
            }
        }
    }

    fun close() {
        if (closed.compareAndSet(false, true)) {
            val deadlineNanos = System.nanoTime() + config.closeTimeout.toNanos()
            synchronized(closeLock) {
                while (inFlight.get() > 0) {
                    val remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime())
                    if (remainingMillis <= 0) break
                    closeLock.wait(remainingMillis)
                }
            }
        }
    }

    fun isClosed(): Boolean = closed.get()

    private fun publishMandatory(
        channel: Channel,
        exchange: String,
        routingKey: String,
        properties: com.rabbitmq.client.AMQP.BasicProperties,
        body: ByteArray
    ) {
        val latch = CountDownLatch(1)
        val returned = AtomicReference<Return?>()
        val listener = channel.addReturnListener { r ->
            returned.set(r)
            latch.countDown()
        }
        try {
            channel.basicPublish(exchange, routingKey, true, properties, body)
            latch.await(config.returnListenerTimeout.toMillis(), TimeUnit.MILLISECONDS)
            returned.get()?.let { r ->
                throw MessageReturnedException(r.replyCode, r.replyText, r.exchange, r.routingKey)
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt();
        } finally {
            channel.removeReturnListener(listener)
        }
    }

    private fun buildProperties(
        metadata: MessageMetadata,
        body: MessagePayload
    ): com.rabbitmq.client.AMQP.BasicProperties.Builder {
        val builder = com.rabbitmq.client.AMQP.BasicProperties.Builder()
            .messageId(metadata.messageId)
            .timestamp(Date.from(metadata.timestamp))
            .contentType(body.contentType)
            .contentEncoding(body.contentEncoding)
            .headers(metadata.headers)
            .deliveryMode(2)
            .correlationId(metadata.correlationId)
            .priority(metadata.priority)
        metadata.expiration?.let { builder.expiration(it.toMillis().toString()) }
        return builder
    }

}
