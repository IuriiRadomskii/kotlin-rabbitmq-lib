package org.radomskii.rabbit.publisher

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Return
import com.rabbitmq.client.ReturnCallback
import com.rabbitmq.client.ReturnListener
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.resources.ChannelPool
import org.radomskii.rabbit.resources.ConnectionPool
import org.radomskii.rabbit.resources.ManagedChannel
import org.radomskii.rabbit.resources.ManagedConnection
import org.radomskii.rabbit.serialization.MessageSerializer
import java.io.IOException
import java.time.Duration

class RabbitPublisherTest {

    private val serializer = object : MessageSerializer<String> {
        override fun serialize(payload: String) = MessagePayload(payload.toByteArray(), "text/plain")
        override fun deserialize(payload: MessagePayload) = String(payload.bytes)
    }

    private data class Fixture(
        val publisher: RabbitPublisher<String>,
        val rawChannel: Channel,
        val channelPool: ChannelPool
    )

    private fun fixture(mandatory: Boolean = false, returnListenerTimeout: Duration = Duration.ofMillis(200)): Fixture {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val channelPool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, channelPool)
        val managedConnection = mock<ManagedConnection>()
        whenever(managedConnection.acquireChannel()).thenReturn(managedChannel)
        val connectionPool = mock<ConnectionPool>()
        whenever(connectionPool.nextConnection()).thenReturn(managedConnection)

        val config = PublisherConfig(
            serializer = serializer,
            mandatory = mandatory,
            returnListenerTimeout = returnListenerTimeout
        )
        return Fixture(RabbitPublisher(connectionPool, config), rawChannel, channelPool)
    }

    @Test
    fun shouldPublishSerializedPayloadAndReleaseChannel() {
        val (publisher, rawChannel, channelPool) = fixture()

        publisher.publish("my-exchange", "my.key", "hello")

        verify(rawChannel).basicPublish(eq("my-exchange"), eq("my.key"), eq(false), any(), any())
        verify(channelPool).release(any())
    }

    @Test
    fun shouldWrapIOExceptionAndInvalidateChannel() {
        val (publisher, rawChannel, channelPool) = fixture()
        whenever(rawChannel.basicPublish(any(), any(), any<Boolean>(), any(), any())).thenThrow(IOException("boom"))

        assertThatThrownBy { publisher.publish("ex", "key", "hi") }
            .isInstanceOf(RabbitPublishException::class.java)
            .hasCauseInstanceOf(IOException::class.java)

        verify(channelPool).discard(any())
        verify(channelPool, never()).release(any())
    }

    @Test
    fun shouldThrowMessageReturnedExceptionWhenMandatoryPublishIsReturned() {
        val (publisher, rawChannel, _) = fixture(mandatory = true)
        val listener = mock<ReturnListener>()
        val callbackCaptor = argumentCaptor<ReturnCallback>()
        whenever(rawChannel.addReturnListener(callbackCaptor.capture())).thenReturn(listener)
        whenever(rawChannel.basicPublish(any(), any(), eq(true), any(), any())).thenAnswer {
            val returned = Return(312, "NO_ROUTE", "ex", "key", mock<AMQP.BasicProperties>(), ByteArray(0))
            callbackCaptor.firstValue.handle(returned)
            null
        }

        assertThatThrownBy { publisher.publish("ex", "key", "hi") }
            .isInstanceOf(MessageReturnedException::class.java)

        verify(rawChannel).removeReturnListener(listener)
    }

    @Test
    fun shouldNotThrowWhenMandatoryPublishRoutesSuccessfully() {
        val (publisher, rawChannel, _) = fixture(mandatory = true)
        whenever(rawChannel.addReturnListener(any<ReturnCallback>())).thenReturn(mock())

        publisher.publish("ex", "key", "hi")

        verify(rawChannel).basicPublish(eq("ex"), eq("key"), eq(true), any(), any())
    }

    @Test
    fun shouldRejectPublishAfterClose() {
        val (publisher, _, _) = fixture()
        publisher.close()

        assertThatThrownBy { publisher.publish("ex", "key", "hi") }
            .isInstanceOf(IllegalStateException::class.java)

        assertThat(publisher.isClosed()).isTrue()
    }

    @Test
    fun shouldWaitForInFlightPublishesBeforeCloseReturns() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val channelPool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, channelPool)
        val managedConnection = mock<ManagedConnection>()
        val releaseGate = java.util.concurrent.CountDownLatch(1)
        whenever(rawChannel.basicPublish(any(), any(), any<Boolean>(), any(), any())).thenAnswer {
            releaseGate.await()
            null
        }
        whenever(managedConnection.acquireChannel()).thenReturn(managedChannel)
        val connectionPool = mock<ConnectionPool>()
        whenever(connectionPool.nextConnection()).thenReturn(managedConnection)
        val config = PublisherConfig(serializer = serializer, closeTimeout = Duration.ofSeconds(5))
        val publisher = RabbitPublisher(connectionPool, config)

        val publishThread = Thread { publisher.publish("ex", "key", "hi") }
        publishThread.start()
        Thread.sleep(50)

        val closeThread = Thread { publisher.close() }
        closeThread.start()
        Thread.sleep(50)
        assertThat(closeThread.isAlive).isTrue()

        releaseGate.countDown()
        publishThread.join(1000)
        closeThread.join(1000)

        assertThat(closeThread.isAlive).isFalse()
    }
}
