package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class ManagedConnectionTest {

    @Test
    fun shouldCreateChannelDelegatingCallsToUnderlyingChannel() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        whenever(rawChannel.isOpen).thenReturn(true)
        val connectionDecorator = ConnectionDecorator(rawConnection)

        val channel = connectionDecorator.createChannel()

        assertThat(channel.isOpen).isTrue()
        verify(rawChannel).isOpen
    }

    @Test
    fun shouldIncrementChannelCountWhenChannelCreated() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(mock(), mock())
        val connectionDecorator = ConnectionDecorator(rawConnection)

        connectionDecorator.createChannel()
        connectionDecorator.createChannel()

        assertThat(connectionDecorator.channelCount).isEqualTo(2)
    }

    @Test
    fun shouldDecrementChannelCountWhenChannelClosed() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val connectionDecorator = ConnectionDecorator(rawConnection)
        val channel = connectionDecorator.createChannel()

        channel.close()

        assertThat(connectionDecorator.channelCount).isZero()
        verify(rawChannel).close()
    }

    @Test
    fun shouldDecrementChannelCountOnlyOnceWhenCloseCalledRepeatedly() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val connectionDecorator = ConnectionDecorator(rawConnection)
        val channel = connectionDecorator.createChannel()

        channel.close()
        channel.close()

        assertThat(connectionDecorator.channelCount).isZero()
        verify(rawChannel, times(2)).close()
    }

    @Test
    fun shouldDecrementChannelCountWhenChannelAborted() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val connectionDecorator = ConnectionDecorator(rawConnection)
        val channel = connectionDecorator.createChannel()

        channel.abort()

        assertThat(connectionDecorator.channelCount).isZero()
        verify(rawChannel).abort()
    }

    @Test
    fun shouldReportClosedWhenUnderlyingConnectionClosed() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(false)
        val connectionDecorator = ConnectionDecorator(rawConnection)

        assertThat(connectionDecorator.isOpen).isFalse()
    }

    @Test
    fun shouldCloseUnderlyingConnectionOnceAndBeIdempotent() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val connectionDecorator = ConnectionDecorator(rawConnection)

        connectionDecorator.close()
        connectionDecorator.close()

        verify(rawConnection, times(1)).close()
        assertThat(connectionDecorator.isOpen).isFalse()
    }

    @Test
    fun shouldThrowWhenCreatingChannelAfterClose() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val connectionDecorator = ConnectionDecorator(rawConnection)

        connectionDecorator.close()

        assertThatThrownBy { connectionDecorator.createChannel() }
            .isInstanceOf(IllegalStateException::class.java)
    }
}
