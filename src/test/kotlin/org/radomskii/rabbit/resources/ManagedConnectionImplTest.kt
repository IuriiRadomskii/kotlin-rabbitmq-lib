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

class ManagedConnectionImplTest {

    @Test
    fun shouldCreateChannelDelegatingCallsToUnderlyingChannel() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        whenever(rawChannel.isOpen).thenReturn(true)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)

        val channel = managedConnectionImpl.createChannel()

        assertThat(channel.isOpen).isTrue()
        verify(rawChannel).isOpen
    }

    @Test
    fun shouldIncrementChannelCountWhenChannelCreated() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(mock(), mock())
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)

        managedConnectionImpl.createChannel()
        managedConnectionImpl.createChannel()

        assertThat(managedConnectionImpl.channelCount).isEqualTo(2)
    }

    @Test
    fun shouldDecrementChannelCountWhenChannelClosed() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)
        val channel = managedConnectionImpl.createChannel()

        channel.close()

        assertThat(managedConnectionImpl.channelCount).isZero()
        verify(rawChannel).close()
    }

    @Test
    fun shouldDecrementChannelCountOnlyOnceWhenCloseCalledRepeatedly() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)
        val channel = managedConnectionImpl.createChannel()

        channel.close()
        channel.close()

        assertThat(managedConnectionImpl.channelCount).isZero()
        verify(rawChannel, times(2)).close()
    }

    @Test
    fun shouldDecrementChannelCountWhenChannelAborted() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)
        val channel = managedConnectionImpl.createChannel()

        channel.abort()

        assertThat(managedConnectionImpl.channelCount).isZero()
        verify(rawChannel).abort()
    }

    @Test
    fun shouldReportClosedWhenUnderlyingConnectionClosed() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(false)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)

        assertThat(managedConnectionImpl.isOpen).isFalse()
    }

    @Test
    fun shouldCloseUnderlyingConnectionOnceAndBeIdempotent() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)

        managedConnectionImpl.close()
        managedConnectionImpl.close()

        verify(rawConnection, times(1)).close()
        assertThat(managedConnectionImpl.isOpen).isFalse()
    }

    @Test
    fun shouldThrowWhenCreatingChannelAfterClose() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val managedConnectionImpl = ManagedConnectionImpl(rawConnection)

        managedConnectionImpl.close()

        assertThatThrownBy { managedConnectionImpl.createChannel() }
            .isInstanceOf(IllegalStateException::class.java)
    }
}
