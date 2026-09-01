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
import org.slf4j.LoggerFactory
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

    private companion object {
        val log = LoggerFactory.getLogger(ConsumerWorker::class.java)
        const val POLL_INTERVAL_MILLIS = 200L
    }

    fun start() {
        log.trace("Starting consumer worker {}: queues={}, prefetchCount={}, autoAck={}", id, config.queues, config.prefetchCount, config.autoAck)
        channel.basicQos(config.prefetchCount)

        val deliverCallback = DeliverCallback { consumerTag, delivery ->
            log.trace(
                "Worker {} received delivery: consumerTag={}, deliveryTag={}, exchange={}, routingKey={}",
                id, consumerTag, delivery.envelope.deliveryTag, delivery.envelope.exchange, delivery.envelope.routingKey
            )
            try {
                deliveryQueue.put(delivery)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw IOException("Interrupted while queueing delivery for worker $id", e)
            }
        }
        val cancelCallback = CancelCallback { consumerTag ->
            log.trace("Worker {} consumer cancelled by broker: consumerTag={}", id, consumerTag)
            failed.set(true)
        }
        config.queues.forEach { queue ->
            consumerTags += channel.basicConsume(queue, config.autoAck, deliverCallback, cancelCallback)
        }
        log.trace("Worker {} subscribed: consumerTags={}", id, consumerTags)
        running.set(true)
        thread.start()
        log.trace("Worker {} started", id)
    }

    fun stop(timeout: Duration) {
        log.trace("Stopping consumer worker {}: timeout={}", id, timeout)
        running.set(false)
        consumerTags.forEach { tag -> runCatching { channel.basicCancel(tag) } }
        if (thread.isAlive) {
            thread.join(timeout.toMillis())
        }
        forceClose()
        log.trace("Consumer worker {} stopped", id)
    }

    fun forceClose() {
        log.trace("Force closing consumer worker {}", id)
        runCatching { if (channel.isOpen) channel.close() }
    }

    private fun runMainLoop() {
        log.trace("Worker {} main loop started", id)
        try {
            while (running.get()) {
                val delivery = deliveryQueue.poll(POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS) ?: continue
                processDelivery(delivery)
            }
        } catch (e: Exception) {
            log.error("Worker {} main loop failed", id, e)
            failed.set(true)
        }
        log.trace("Worker {} main loop exited", id)
    }

    private fun processDelivery(delivery: Delivery) {
        val deliveryTag = delivery.envelope.deliveryTag
        try {
            val incomingMessage = map(delivery)
            log.trace("Worker {} handling message: deliveryTag={}, messageId={}", id, deliveryTag, incomingMessage.metadata.messageId)
            val result = handler.handle(incomingMessage)
            log.trace("Worker {} handled message: deliveryTag={}, result={}", id, deliveryTag, result)
            if (!config.autoAck) settle(incomingMessage.deliveryTag, result)
        } catch (e: Exception) {
            log.error("Worker {} failed to process delivery: deliveryTag={}", id, deliveryTag, e)
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
        log.trace("Worker {} settling delivery: deliveryTag={}, result={}", id, deliveryTag, result)
        when (result) {
            is ConsumeResult.Ack -> channel.basicAck(deliveryTag, false)
            is ConsumeResult.Nack -> channel.basicNack(deliveryTag, false, result.requeue)
            is ConsumeResult.Reject -> channel.basicReject(deliveryTag, result.requeue)
        }
    }
}
