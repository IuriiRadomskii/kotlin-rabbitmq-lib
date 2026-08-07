package org.radomskii.rabbit.consumer

import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DeliverCallback
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.resources.ConnectionPool
import org.radomskii.rabbit.resources.ManagedConnection
import org.radomskii.rabbit.serialization.MessageSerializer
import java.time.Duration

class RabbitConsumerTest {

    private val deserializer = object : MessageSerializer<String> {
        override fun serialize(payload: String) = MessagePayload(payload.toByteArray(), "text/plain")
        override fun deserialize(payload: MessagePayload) = String(payload.bytes)
    }

    private val ackHandler = MessageHandler<String> { ConsumeResult.Ack }

    private fun newConsumer(): RabbitConsumer<String> {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.basicConsume(any<String>(), any<Boolean>(), any<DeliverCallback>(), any<CancelCallback>()))
            .thenReturn("consumer-tag")
        val managedConnection = mock<ManagedConnection>()
        whenever(managedConnection.createChannel()).thenReturn(rawChannel)
        val connectionPool = mock<ConnectionPool>()
        whenever(connectionPool.nextConnection()).thenReturn(managedConnection)

        val config = ConsumerConfig(
            queues = listOf("q1"),
            deserializer = deserializer,
            gracefulShutdownTimeout = Duration.ofSeconds(2),
            supervisorPollInterval = Duration.ofSeconds(1)
        )
        return RabbitConsumer(connectionPool, config)
    }

    @Test
    fun shouldReportRunningAfterStart() {
        val consumer = newConsumer()

        consumer.start(ackHandler)

        assertThat(consumer.isRunning()).isTrue()
        consumer.stop()
    }

    @Test
    fun shouldThrowWhenStartedTwice() {
        val consumer = newConsumer()
        consumer.start(ackHandler)

        assertThatThrownBy { consumer.start(ackHandler) }
            .isInstanceOf(IllegalStateException::class.java)

        consumer.stop()
    }

    @Test
    fun shouldReportNotRunningAfterStop() {
        val consumer = newConsumer()
        consumer.start(ackHandler)

        consumer.stop()

        assertThat(consumer.isRunning()).isFalse()
    }

    @Test
    fun shouldBeIdempotentWhenStoppedTwice() {
        val consumer = newConsumer()
        consumer.start(ackHandler)

        consumer.stop()
        consumer.stop()

        assertThat(consumer.isRunning()).isFalse()
    }

    @Test
    fun shouldReportNotRunningBeforeStart() {
        val consumer = newConsumer()

        assertThat(consumer.isRunning()).isFalse()
    }
}
