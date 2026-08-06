package org.radomskii.rabbit.consumer

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.CancelCallback
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import com.rabbitmq.client.Envelope
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.timeout
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.serialization.MessageSerializer
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class ConsumerWorkerTest {

    private val echoDeserializer = object : MessageSerializer<String> {
        override fun serialize(payload: String) = MessagePayload(payload.toByteArray(), "text/plain")
        override fun deserialize(payload: MessagePayload) = String(payload.bytes)
    }

    private var activeWorker: ConsumerWorker<String>? = null

    @AfterEach
    fun tearDown() {
        activeWorker?.stop(Duration.ofSeconds(1))
        activeWorker = null
    }

    private data class Started(val worker: ConsumerWorker<String>, val channel: Channel, val deliverCallback: DeliverCallback)

    private fun startWorker(
        result: ConsumeResult = ConsumeResult.Ack,
        autoAck: Boolean = false,
        capturedMessage: AtomicReference<String>? = null,
        latch: CountDownLatch,
        deserializer: MessageSerializer<String> = echoDeserializer
    ): Started {
        val channel = mock<Channel>()
        val deliverCaptor = argumentCaptor<DeliverCallback>()
        whenever(channel.basicConsume(eq("q1"), eq(autoAck), deliverCaptor.capture(), any<CancelCallback>()))
            .thenReturn("consumer-tag")

        val config = ConsumerConfig(queues = listOf("q1"), deserializer = deserializer, autoAck = autoAck)
        val handler = MessageHandler<String> { message ->
            capturedMessage?.set(message.payload)
            latch.countDown()
            result
        }
        val worker = ConsumerWorker(0, channel, config, handler)
        activeWorker = worker
        worker.start()

        return Started(worker, channel, deliverCaptor.firstValue)
    }

    private fun delivery(body: String = "hello", deliveryTag: Long = 7L): Delivery {
        val properties = AMQP.BasicProperties.Builder()
            .contentType("text/plain")
            .contentEncoding("UTF-8")
            .build()
        return Delivery(Envelope(deliveryTag, false, "ex", "rk"), properties, body.toByteArray())
    }

    @Test
    fun shouldPassDeserializedPayloadToHandlerAndAckOnSuccess() {
        val latch = CountDownLatch(1)
        val payloadRef = AtomicReference<String>()
        val started = startWorker(result = ConsumeResult.Ack, capturedMessage = payloadRef, latch = latch)

        started.deliverCallback.handle("consumer-tag", delivery("hello"))

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue()
        assertThat(payloadRef.get()).isEqualTo("hello")
        verify(started.channel, timeout(1000)).basicAck(7L, false)
    }

    @Test
    fun shouldNackWithoutRequeueWhenHandlerReturnsNack() {
        val latch = CountDownLatch(1)
        val started = startWorker(result = ConsumeResult.Nack(requeue = false), latch = latch)

        started.deliverCallback.handle("consumer-tag", delivery())

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue()
        verify(started.channel, timeout(1000)).basicNack(7L, false, false)
    }

    @Test
    fun shouldRejectWithRequeueWhenHandlerReturnsReject() {
        val latch = CountDownLatch(1)
        val started = startWorker(result = ConsumeResult.Reject(requeue = true), latch = latch)

        started.deliverCallback.handle("consumer-tag", delivery())

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue()
        verify(started.channel, timeout(1000)).basicReject(7L, true)
    }

    @Test
    fun shouldNotSettleDeliveryWhenAutoAckEnabled() {
        val latch = CountDownLatch(1)
        val started = startWorker(result = ConsumeResult.Ack, autoAck = true, latch = latch)

        started.deliverCallback.handle("consumer-tag", delivery())

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue()
        Thread.sleep(200)
        verify(started.channel, never()).basicAck(any(), any())
    }

    @Test
    fun shouldNackAndRequeueWhenDeserializationFails() {
        val failingDeserializer = object : MessageSerializer<String> {
            override fun serialize(payload: String) = MessagePayload(payload.toByteArray(), "text/plain")
            override fun deserialize(payload: MessagePayload): String = throw IllegalArgumentException("bad payload")
        }
        val latch = CountDownLatch(0)
        val started = startWorker(latch = latch, deserializer = failingDeserializer)

        started.deliverCallback.handle("consumer-tag", delivery())

        verify(started.channel, timeout(1000)).basicNack(7L, false, true)
    }

    @Test
    fun shouldCancelConsumerAndCloseChannelOnStop() {
        val latch = CountDownLatch(0)
        val started = startWorker(latch = latch)
        whenever(started.channel.isOpen).thenReturn(true)

        started.worker.stop(Duration.ofSeconds(1))
        activeWorker = null

        verify(started.channel).basicCancel("consumer-tag")
        verify(started.channel).close()
    }
}
