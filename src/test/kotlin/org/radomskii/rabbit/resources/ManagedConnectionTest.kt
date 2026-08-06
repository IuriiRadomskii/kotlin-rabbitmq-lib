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
import org.radomskii.rabbit.config.ChannelPoolConfig

class ManagedConnectionTest {

    @Test
    fun shouldAcquirePooledChannelBackedByConnection() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        whenever(rawChannel.isOpen).thenReturn(true)
        val managedConnection = ManagedConnection(rawConnection, ChannelPoolConfig())

        val acquired = managedConnection.acquireChannel()

        assertThat(acquired.rawChannel()).isSameAs(rawChannel)
    }

    @Test
    fun shouldCreateDedicatedChannelNotTrackedByPool() {
        val rawConnection = mock<Connection>()
        val dedicated = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(dedicated)
        val managedConnection = ManagedConnection(rawConnection, ChannelPoolConfig())

        val channel = managedConnection.createDedicatedChannel()

        assertThat(channel).isSameAs(dedicated)
    }

    @Test
    fun shouldReportClosedWhenUnderlyingConnectionClosed() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(false)
        val managedConnection = ManagedConnection(rawConnection, ChannelPoolConfig())

        assertThat(managedConnection.isOpen).isFalse()
    }

    @Test
    fun shouldCloseUnderlyingConnectionOnceAndBeIdempotent() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val managedConnection = ManagedConnection(rawConnection, ChannelPoolConfig())

        managedConnection.close()
        managedConnection.close()

        verify(rawConnection, times(1)).close()
        assertThat(managedConnection.isOpen).isFalse()
    }

    @Test
    fun shouldThrowWhenAcquiringChannelAfterClose() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val managedConnection = ManagedConnection(rawConnection, ChannelPoolConfig())

        managedConnection.close()

        assertThatThrownBy { managedConnection.acquireChannel() }
            .isInstanceOf(IllegalStateException::class.java)
    }
}
