package org.radomskii.rabbit.resources

import com.rabbitmq.client.Address
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.radomskii.rabbit.config.ReconnectionConfig
import java.time.Duration

class ConnectionPoolTest {

    private fun fastReconnectionConfig(maxAttempts: Int) =
        ReconnectionConfig(maxAttempts = maxAttempts, retryInterval = Duration.ofMillis(1))

    @Test
    fun shouldOpenConfiguredConnectionCountAndServeThemRoundRobinAfterInit() {
        val connection1 = mock<Connection>()
        val connection2 = mock<Connection>()
        whenever(connection1.isOpen).thenReturn(true)
        whenever(connection2.isOpen).thenReturn(true)
        val rawConnections = ArrayDeque(listOf(connection1, connection2))
        val pool = ConnectionPool(ConnectionFactory(), connectionCount = 2) {
            rawConnections.removeFirst()
        }

        pool.init()
        val first = pool.nextConnection()
        val second = pool.nextConnection()
        val third = pool.nextConnection()

        assertThat(second).isNotSameAs(first)
        assertThat(third).isSameAs(first)
    }

    @Test
    fun shouldThrowWhenNextConnectionCalledBeforeInit() {
        val pool = ConnectionPool(ConnectionFactory()) { mock<Connection>() }

        assertThatThrownBy { pool.nextConnection() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldRetryConnectionFactoryThenSucceed() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        var attempts = 0
        val pool = ConnectionPool(
            ConnectionFactory(),
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 3)
        ) {
            attempts++
            if (attempts < 3) throw RuntimeException("boom") else rawConnection
        }

        pool.init()

        assertThat(attempts).isEqualTo(3)
        assertThat(pool.nextConnection()).isNotNull()
    }

    @Test
    fun shouldThrowRabbitConnectionExceptionAfterExhaustingReconnectionAttempts() {
        var attempts = 0
        val failure = RuntimeException("boom")
        val pool = ConnectionPool(
            ConnectionFactory(),
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 2)
        ) {
            attempts++
            throw failure
        }

        assertThatThrownBy { pool.init() }
            .isInstanceOf(RabbitConnectionException::class.java)
            .hasCause(failure)
        assertThat(attempts).isEqualTo(2)
    }

    @Test
    fun shouldBeIdempotentWhenInitCalledTwice() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        var factoryCalls = 0
        val pool = ConnectionPool(ConnectionFactory()) {
            factoryCalls++
            rawConnection
        }

        pool.init()
        pool.init()

        assertThat(factoryCalls).isEqualTo(1)
    }

    @Test
    fun shouldThrowWhenInitCalledAfterClose() {
        val pool = ConnectionPool(ConnectionFactory()) { mock<Connection>() }
        pool.close()

        assertThatThrownBy { pool.init() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun shouldCloseAlreadyOpenedConnectionsWhenLaterConnectionFailsDuringInit() {
        val firstConnection = mock<Connection>()
        whenever(firstConnection.isOpen).thenReturn(true)
        var callCount = 0
        val pool = ConnectionPool(
            ConnectionFactory(),
            connectionCount = 2,
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 1)
        ) {
            callCount++
            if (callCount == 1) firstConnection else throw RuntimeException("boom")
        }

        assertThatThrownBy { pool.init() }
            .isInstanceOf(RabbitConnectionException::class.java)

        verify(firstConnection).close()
    }

    @Test
    fun shouldCloseAllConnectionsOnClose() {
        val connection1 = mock<Connection>()
        val connection2 = mock<Connection>()
        whenever(connection1.isOpen).thenReturn(true)
        whenever(connection2.isOpen).thenReturn(true)
        val rawConnections = ArrayDeque(listOf(connection1, connection2))
        val pool = ConnectionPool(ConnectionFactory(), connectionCount = 2) {
            rawConnections.removeFirst()
        }
        pool.init()

        pool.close()

        verify(connection1).close()
        verify(connection2).close()
    }

    @Test
    fun shouldThrowWhenNextConnectionCalledAfterClose() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val pool = ConnectionPool(ConnectionFactory()) { rawConnection }
        pool.init()

        pool.close()

        assertThatThrownBy { pool.nextConnection() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldThrowWhenConnectionFactoryHasAutomaticRecoveryDisabled() {
        val factory = ConnectionFactory().apply { isAutomaticRecoveryEnabled = false }

        assertThatThrownBy { ConnectionPool(factory) { mock<Connection>() } }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldCallNoArgNewConnectionWhenAddressesEmpty() {
        val factory = mock<ConnectionFactory>()
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(factory.isAutomaticRecoveryEnabled).thenReturn(true)
        whenever(factory.newConnection()).thenReturn(rawConnection)
        val pool = ConnectionPool(factory)

        pool.init()

        verify(factory).newConnection()
    }

    @Test
    fun shouldCallNewConnectionWithAddressesWhenProvided() {
        val factory = mock<ConnectionFactory>()
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(factory.isAutomaticRecoveryEnabled).thenReturn(true)
        val addresses = listOf(Address("host-a"), Address("host-b"))
        whenever(factory.newConnection(addresses)).thenReturn(rawConnection)
        val pool = ConnectionPool(factory, addresses = addresses)

        pool.init()

        verify(factory).newConnection(addresses)
    }
}
