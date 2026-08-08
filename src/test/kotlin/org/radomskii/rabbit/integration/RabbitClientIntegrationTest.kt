package org.radomskii.rabbit.integration

import com.rabbitmq.client.ConnectionFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.radomskii.rabbit.RabbitMQClient
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.publisher.MessageReturnedException
import org.radomskii.rabbit.serialization.JsonMessageSerializer
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
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

        private lateinit var client: RabbitMQClient

        @BeforeAll
        @JvmStatic
        fun setUp() {
            client = RabbitMQClient.builder()
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
            client.close()
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

        val publisher = client.createPublisher(
            PublisherConfig(serializer = JsonMessageSerializer.create<SampleEvent>())
        )
        val received = ArrayBlockingQueue<SampleEvent>(1)
        val consumer = client.createConsumer(
            ConsumerConfig(queues = listOf(queue), deserializer = JsonMessageSerializer.create<SampleEvent>())
        )
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
            publisher.close()
        }
    }

    @Test
    fun shouldThrowMessageReturnedExceptionWhenMandatoryPublishIsUnroutable() {
        val publisher = client.createPublisher(
            PublisherConfig(
                serializer = JsonMessageSerializer.create<SampleEvent>(),
                mandatory = true,
                returnListenerTimeout = Duration.ofSeconds(5)
            )
        )

        try {
            assertThatThrownBy {
                publisher.publish("", "no.such.queue.${System.nanoTime()}", SampleEvent(2, "lost"))
            }.isInstanceOf(MessageReturnedException::class.java)
        } finally {
            publisher.close()
        }
    }

    @Test
    fun shouldRedeliverMessageWhenHandlerNacksWithRequeue() {
        val exchange = "test.exchange.requeue"
        val queue = "test.queue.requeue"
        val routingKey = "test.key.requeue"
        declareTopology(exchange, queue, routingKey)

        val publisher = client.createPublisher(
            PublisherConfig(serializer = JsonMessageSerializer.create<SampleEvent>())
        )
        val attempts = AtomicInteger(0)
        val received = ArrayBlockingQueue<SampleEvent>(1)
        val consumer = client.createConsumer(
            ConsumerConfig(queues = listOf(queue), deserializer = JsonMessageSerializer.create<SampleEvent>())
        )
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
            publisher.close()
        }
    }
}
