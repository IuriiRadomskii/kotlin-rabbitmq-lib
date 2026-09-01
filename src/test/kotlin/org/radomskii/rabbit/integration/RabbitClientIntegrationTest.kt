package org.radomskii.rabbit.integration

import com.rabbitmq.client.ConnectionFactory
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.consumer.RabbitConsumer
import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.publisher.RabbitPublisher
import org.radomskii.rabbit.resources.ConnectionPool
import org.radomskii.rabbit.serialization.JsonMessageSerializer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Testcontainers
@Tag("integration")
class RabbitClientIntegrationTest {

    data class SampleEvent(val id: Int, val text: String)

    companion object {
        @Container
        @JvmStatic
        private val rabbitContainer: RabbitMQContainer =
            RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management-alpine"))

        private lateinit var connectionPool: ConnectionPool

        @BeforeAll
        @JvmStatic
        fun setUp() {
            connectionPool = ConnectionPool.builder()
                .connectionFactory(
                    ConnectionFactory().apply {
                        username = rabbitContainer.adminUsername
                        password = rabbitContainer.adminPassword
                        host = rabbitContainer.host
                        port = rabbitContainer.amqpPort
                    }
                )
                .build()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            connectionPool.close()
        }

        private fun declareTopology(exchange: String, queue: String, routingKey: String) {
            val factory = ConnectionFactory().apply {
                host = rabbitContainer.host
                port = rabbitContainer.amqpPort
                username = rabbitContainer.adminUsername
                password = rabbitContainer.adminPassword
            }
            factory.newConnection().use { connection ->
                connection.createChannel().use { channel ->
                    channel.exchangeDeclare(exchange, "direct", true)
                    channel.queueDeclare(queue, true, false, false, null)
                    channel.queueBind(queue, exchange, routingKey)
                }
            }
        }
    }

    @Test
    fun shouldPublishAndConsumeMessageRoundTrip() {
        val exchange = "test.exchange.roundtrip"
        val queue = "test.queue.roundtrip"
        val routingKey = "test.key.roundtrip"
        declareTopology(exchange, queue, routingKey)

        val publisher = RabbitPublisher.builder<SampleEvent>()
            .connectionPool(connectionPool)
            .config(PublisherConfig(serializer = JsonMessageSerializer.create<SampleEvent>()))
            .build()
        val received = ArrayBlockingQueue<SampleEvent>(1)
        val consumer = RabbitConsumer.builder<SampleEvent>()
            .connectionPool(connectionPool)
            .config(ConsumerConfig(queues = listOf(queue), deserializer = JsonMessageSerializer.create<SampleEvent>()))
            .build()
        consumer.start { message ->
            received.put(message.payload)
            ConsumeResult.Ack
        }

        try {
            publisher.publish(exchange, routingKey, SampleEvent(1, "hello"))

            val message = received.poll(10, TimeUnit.SECONDS)

            assertThat(message).isEqualTo(SampleEvent(1, "hello"))
        } finally {
            consumer.stop()
        }
    }

    @Test
    fun shouldRedeliverMessageWhenHandlerNacksWithRequeue() {
        val exchange = "test.exchange.requeue"
        val queue = "test.queue.requeue"
        val routingKey = "test.key.requeue"
        declareTopology(exchange, queue, routingKey)

        val publisher = RabbitPublisher.builder<SampleEvent>()
            .connectionPool(connectionPool)
            .config(PublisherConfig(serializer = JsonMessageSerializer.create<SampleEvent>()))
            .build()
        val attempts = AtomicInteger(0)
        val received = ArrayBlockingQueue<SampleEvent>(1)
        val consumer = RabbitConsumer.builder<SampleEvent>()
            .connectionPool(connectionPool)
            .config(ConsumerConfig(queues = listOf(queue), deserializer = JsonMessageSerializer.create<SampleEvent>()))
            .build()
        consumer.start { message ->
            if (attempts.getAndIncrement() == 0) {
                ConsumeResult.Nack(requeue = true)
            } else {
                received.put(message.payload)
                ConsumeResult.Ack
            }
        }

        try {
            publisher.publish(exchange, routingKey, SampleEvent(3, "retry-me"))

            val message = received.poll(10, TimeUnit.SECONDS)

            assertThat(message).isEqualTo(SampleEvent(3, "retry-me"))
            assertThat(attempts.get()).isGreaterThanOrEqualTo(2)
        } finally {
            consumer.stop()
        }
    }
}
