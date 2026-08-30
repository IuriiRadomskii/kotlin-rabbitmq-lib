package org.radomskii.rabbit.publisher

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ShutdownSignalException
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.model.MessageMetadata
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.resources.InitializableConnectionPool
import java.io.IOException
import java.util.*

class RabbitPublisher<T> internal constructor(
    private val initializableConnectionPool: InitializableConnectionPool,
    private val config: PublisherConfig<T>
) {

    fun publish(exchange: String, routingKey: String, payload: T, metadata: MessageMetadata = MessageMetadata()) {
        val connection = try {
            initializableConnectionPool.nextConnection()
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
            channel.basicPublish(exchange, routingKey, false, properties, body.bytes)
        } catch (e: MessageReturnedException) {
            throw e
        } catch (e: IOException) {
            throw RabbitPublishException(exchange, routingKey, "Failed to publish message", e)
        } catch (e: ShutdownSignalException) {
            throw RabbitPublishException(exchange, routingKey, "Failed to publish message", e)
        } finally {
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

}
