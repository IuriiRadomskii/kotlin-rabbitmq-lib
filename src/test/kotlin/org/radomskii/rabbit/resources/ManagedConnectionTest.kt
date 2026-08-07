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
    fun shouldCreateChannelBackedByConnection() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val managedConnection = ManagedConnection(rawConnection)

        val channel = managedConnection.createChannel()

        assertThat(channel).isSameAs(rawChannel)
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
