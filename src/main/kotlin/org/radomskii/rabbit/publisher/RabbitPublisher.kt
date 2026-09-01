package org.radomskii.rabbit.publisher

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ShutdownSignalException
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.model.MessageMetadata
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.resources.ConnectionPool
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*

class RabbitPublisher<T> internal constructor(
    private val connectionPool: ConnectionPool,
    private val config: PublisherConfig<T>
) {

    fun publish(exchange: String, routingKey: String, payload: T, metadata: MessageMetadata = MessageMetadata()) {
        log.trace(
            "Publishing message: exchange={}, routingKey={}, messageId={}, correlationId={}",
            exchange, routingKey, metadata.messageId, metadata.correlationId
        )

        val connection = try {
            connectionPool.nextConnection()
        } catch (e: Exception) {
            log.error("Failed to obtain a connection for publish: exchange={}, routingKey={}", exchange, routingKey, e)
            throw RabbitPublishException(exchange, routingKey, "Failed to obtain a connection", e)
        }

        val channel = try {
            connection.createChannel()
        } catch (e: Exception) {
            log.error("Failed to create a channel for publish: exchange={}, routingKey={}, connection={}", exchange, routingKey, connection, e)
            throw RabbitPublishException(exchange, routingKey, "Failed to create a channel", e)
        }

        try {
            val body = config.serializer.serialize(payload)
            val properties = buildProperties(metadata, body).build()
            channel.basicPublish(exchange, routingKey, false, properties, body.bytes)
            log.trace(
                "Message published: exchange={}, routingKey={}, messageId={}, bodyBytes={}",
                exchange, routingKey, metadata.messageId, body.bytes.size
            )
        } catch (e: IOException) {
            log.error("Failed to publish message: exchange={}, routingKey={}, messageId={}", exchange, routingKey, metadata.messageId, e)
            throw RabbitPublishException(exchange, routingKey, "Failed to publish message", e)
        } catch (e: ShutdownSignalException) {
            log.error("Failed to publish message: exchange={}, routingKey={}, messageId={}", exchange, routingKey, metadata.messageId, e)
            throw RabbitPublishException(exchange, routingKey, "Failed to publish message", e)
        } finally {
            log.trace("Closing publish channel: exchange={}, routingKey={}", exchange, routingKey)
            runCatching { if (channel.isOpen) channel.close() }
        }
    }

    private fun buildProperties(
        metadata: MessageMetadata,
        body: MessagePayload
    ): AMQP.BasicProperties.Builder {
        val builder = AMQP.BasicProperties.Builder()
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

    class Builder<T> {
        private var connectionPool: ConnectionPool? = null
        private var config: PublisherConfig<T>? = null

        fun connectionPool(pool: ConnectionPool) = apply { this.connectionPool = pool }

        fun config(config: PublisherConfig<T>) = apply { this.config = config }

        fun build(): RabbitPublisher<T> {
            val resolvedConnectionPool = requireNotNull(connectionPool) { "connectionPool must be set" }
            val resolvedConfig = requireNotNull(config) { "config must be set" }
            return RabbitPublisher(resolvedConnectionPool, resolvedConfig)
        }
    }

    companion object {
        @JvmStatic
        fun <T> builder(): Builder<T> = Builder()

        private val log = LoggerFactory.getLogger(RabbitPublisher::class.java)
    }

}
