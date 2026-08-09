package org.radomskii.rabbit.publisher

import com.rabbitmq.client.Channel
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.resources.ConnectionPool
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
        val rawChannel: Channel
    )

    private fun fixture(returnListenerTimeout: Duration = Duration.ofMillis(200)): Fixture {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val managedConnection = mock<ManagedConnection>()
        whenever(managedConnection.createChannel()).thenReturn(rawChannel)
        val connectionPool = mock<ConnectionPool>()
        whenever(connectionPool.nextConnection()).thenReturn(managedConnection)

        val config = PublisherConfig(
            serializer = serializer,
            returnListenerTimeout = returnListenerTimeout
        )
        return Fixture(RabbitPublisher(connectionPool, config), rawChannel)
    }

    @Test
    fun shouldPublishSerializedPayloadAndCloseChannel() {
        val (publisher, rawChannel) = fixture()

        publisher.publish("my-exchange", "my.key", "hello")

        verify(rawChannel).basicPublish(eq("my-exchange"), eq("my.key"), eq(false), any(), any())
        verify(rawChannel).close()
    }

    @Test
    fun shouldWrapIOExceptionAndStillCloseChannel() {
        val (publisher, rawChannel) = fixture()
        whenever(rawChannel.basicPublish(any(), any(), any<Boolean>(), any(), any())).thenThrow(IOException("boom"))

        assertThatThrownBy { publisher.publish("ex", "key", "hi") }
            .isInstanceOf(RabbitPublishException::class.java)
            .hasCauseInstanceOf(IOException::class.java)

        verify(rawChannel).close()
    }

}
