package org.radomskii.rabbit.consumer

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.model.IncomingMessage
import org.radomskii.rabbit.model.MessageMetadata
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.model.ConsumeResult
import java.io.IOException
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

internal class ConsumerWorker<T>(
    private val id: Int,
    private val channel: Channel,
    private val config: ConsumerConfig<T>,
    private val handler: MessageHandler<T>
) {
    private val deliveryQueue = LinkedBlockingQueue<Delivery>(config.queueCapacity)
    private val running = AtomicBoolean(false)
    private val failed = AtomicBoolean(false)
    private val consumerTags = mutableListOf<String>()
    private val thread = Thread.ofPlatform().name("rabbit-consumer-worker-$id").unstarted { runMainLoop() }

    val isAlive: Boolean get() = thread.isAlive
    val hasFailed: Boolean get() = failed.get()

    fun start() {
        channel.basicQos(config.prefetchCount)

        val deliverCallback = DeliverCallback { _, delivery ->
            try {
                deliveryQueue.put(delivery)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IOException("Interrupted while queueing delivery for worker $id", e)
            }
        }
        val cancelCallback = CancelCallback { failed.set(true) }

        config.queues.forEach { queue ->
            consumerTags += channel.basicConsume(queue, config.autoAck, deliverCallback, cancelCallback)
        }

        running.set(true)
        thread.start()
    }

    fun stop(timeout: Duration) {
        running.set(false)
        consumerTags.forEach { tag -> runCatching { channel.basicCancel(tag) } }
        if (thread.isAlive) {
            thread.join(timeout.toMillis())
        }
        forceClose()
    }

    fun forceClose() {
        runCatching { if (channel.isOpen) channel.close() }
    }

    private fun runMainLoop() {
        try {
            while (running.get()) {
                // TODO: Question: What happens if deliveryQueue.poll throws InterruptedException. Should Thread.currentThread().interrupt() be called?
                val delivery = deliveryQueue.poll(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS) ?: continue
                processDelivery(delivery)
            }
        } catch (e: Exception) {
            failed.set(true)
        }
    }

    private fun processDelivery(delivery: Delivery) {
        try {
            val incomingMessage = map(delivery)
            val result = handler.handle(incomingMessage)
            if (!config.autoAck) settle(incomingMessage.deliveryTag, result)
        } catch (e: Exception) {
            if (!config.autoAck) {
                runCatching { channel.basicNack(delivery.envelope.deliveryTag, false, true) }
            }
        }
    }

    private fun map(delivery: Delivery): IncomingMessage<T> {
        val deliveryTag = delivery.envelope.deliveryTag
        val payload = MessagePayload(
            bytes = delivery.body,
            contentType = delivery.properties.contentType ?: "application/octet-stream",
            contentEncoding = delivery.properties.contentEncoding ?: "UTF-8"
        )
        val typedPayload = config.deserializer.deserialize(payload)
        val metadata = MessageMetadata(
            messageId = delivery.properties.messageId ?: UUID.randomUUID().toString(),
            timestamp = delivery.properties.timestamp?.toInstant() ?: Instant.now(),
            headers = normalizeHeaders(delivery.properties.headers),
            correlationId = delivery.properties.correlationId,
            priority = delivery.properties.priority
        )
        return IncomingMessage(
            payload = typedPayload,
            metadata = metadata,
            exchange = delivery.envelope.exchange,
            routingKey = delivery.envelope.routingKey,
            deliveryTag = deliveryTag,
            redelivered = delivery.envelope.isRedeliver
        )
    }

    private fun settle(deliveryTag: Long, result: ConsumeResult) {
        when (result) {
            is ConsumeResult.Ack -> channel.basicAck(deliveryTag, false)
            is ConsumeResult.Nack -> channel.basicNack(deliveryTag, false, result.requeue)
            is ConsumeResult.Reject -> channel.basicReject(deliveryTag, result.requeue)
        }
    }

    private companion object {
        const val POLL_INTERVAL_MILLIS = 200L
    }
}
