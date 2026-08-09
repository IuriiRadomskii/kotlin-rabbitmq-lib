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
        val managedConnection = ManagedConnection(rawConnection)

        val channel = managedConnection.createChannel()

        assertThat(channel.isOpen).isTrue()
        verify(rawChannel).isOpen
    }

    @Test
    fun shouldIncrementChannelCountWhenChannelCreated() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(mock(), mock())
        val managedConnection = ManagedConnection(rawConnection)

        managedConnection.createChannel()
        managedConnection.createChannel()

        assertThat(managedConnection.channelCount).isEqualTo(2)
    }

    @Test
    fun shouldDecrementChannelCountWhenChannelClosed() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnection = ManagedConnection(rawConnection)
        val channel = managedConnection.createChannel()

        channel.close()

        assertThat(managedConnection.channelCount).isZero()
        verify(rawChannel).close()
    }

    @Test
    fun shouldDecrementChannelCountOnlyOnceWhenCloseCalledRepeatedly() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnection = ManagedConnection(rawConnection)
        val channel = managedConnection.createChannel()

        channel.close()
        channel.close()

        assertThat(managedConnection.channelCount).isZero()
        verify(rawChannel, times(2)).close()
    }

    @Test
    fun shouldDecrementChannelCountWhenChannelAborted() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnection = ManagedConnection(rawConnection)
        val channel = managedConnection.createChannel()

        channel.abort()

        assertThat(managedConnection.channelCount).isZero()
        verify(rawChannel).abort()
    }

    @Test
    fun shouldReportClosedWhenUnderlyingConnectionClosed() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(false)
        val managedConnection = ManagedConnection(rawConnection)

        assertThat(managedConnection.isOpen).isFalse()
    }

    @Test
    fun shouldCloseUnderlyingConnectionOnceAndBeIdempotent() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val managedConnection = ManagedConnection(rawConnection)

        managedConnection.close()
        managedConnection.close()

        verify(rawConnection, times(1)).close()
        assertThat(managedConnection.isOpen).isFalse()
    }

    @Test
    fun shouldThrowWhenCreatingChannelAfterClose() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val managedConnection = ManagedConnection(rawConnection)

        managedConnection.close()

        assertThatThrownBy { managedConnection.createChannel() }
            .isInstanceOf(IllegalStateException::class.java)
    }
}
