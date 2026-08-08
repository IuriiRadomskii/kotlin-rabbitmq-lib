package org.radomskii.rabbit.resources

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

    private fun mockFactory(): ConnectionFactory = mock<ConnectionFactory>().apply {
        whenever(isAutomaticRecoveryEnabled).thenReturn(true)
    }

    @Test
    fun shouldOpenConfiguredConnectionCountAndServeThemRoundRobinAfterInit() {
        val connection1 = mock<Connection>()
        val connection2 = mock<Connection>()
        whenever(connection1.isOpen).thenReturn(true)
        whenever(connection2.isOpen).thenReturn(true)
        val rawConnections = ArrayDeque(listOf(connection1, connection2))
        val factory = mockFactory()
        whenever(factory.newConnection()).thenAnswer { rawConnections.removeFirst() }
        val pool = ConnectionPool(factory, connectionCount = 2)

        pool.init()
        val first = pool.nextConnection()
        val second = pool.nextConnection()
        val third = pool.nextConnection()

        assertThat(second).isNotSameAs(first)
        assertThat(third).isSameAs(first)
    }

    @Test
    fun shouldThrowWhenNextConnectionCalledBeforeInit() {
        val pool = ConnectionPool(mockFactory())

        assertThatThrownBy { pool.nextConnection() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldRetryConnectionFactoryThenSucceed() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val factory = mockFactory()
        var attempts = 0
        whenever(factory.newConnection()).thenAnswer {
            attempts++
            if (attempts < 3) throw RuntimeException("boom") else rawConnection
        }
        val pool = ConnectionPool(factory, reconnectionConfig = fastReconnectionConfig(maxAttempts = 3))

        pool.init()

        assertThat(attempts).isEqualTo(3)
        assertThat(pool.nextConnection()).isNotNull()
    }

    @Test
    fun shouldThrowRabbitConnectionExceptionAfterExhaustingReconnectionAttempts() {
        val factory = mockFactory()
        var attempts = 0
        val failure = RuntimeException("boom")
        whenever(factory.newConnection()).thenAnswer {
            attempts++
            throw failure
        }
        val pool = ConnectionPool(factory, reconnectionConfig = fastReconnectionConfig(maxAttempts = 2))

        assertThatThrownBy { pool.init() }
            .isInstanceOf(RabbitConnectionException::class.java)
            .hasCause(failure)
        assertThat(attempts).isEqualTo(2)
    }

    @Test
    fun shouldBeIdempotentWhenInitCalledTwice() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val factory = mockFactory()
        var factoryCalls = 0
        whenever(factory.newConnection()).thenAnswer {
            factoryCalls++
            rawConnection
        }
        val pool = ConnectionPool(factory)

        pool.init()
        pool.init()

        assertThat(factoryCalls).isEqualTo(1)
    }

    @Test
    fun shouldThrowWhenInitCalledAfterClose() {
        val pool = ConnectionPool(mockFactory())
        pool.close()

        assertThatThrownBy { pool.init() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun shouldCloseAlreadyOpenedConnectionsWhenLaterConnectionFailsDuringInit() {
        val firstConnection = mock<Connection>()
        whenever(firstConnection.isOpen).thenReturn(true)
        val factory = mockFactory()
        var callCount = 0
        whenever(factory.newConnection()).thenAnswer {
            callCount++
            if (callCount == 1) firstConnection else throw RuntimeException("boom")
        }
        val pool = ConnectionPool(
            factory,
            connectionCount = 2,
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 1)
        )

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
        val factory = mockFactory()
        whenever(factory.newConnection()).thenAnswer { rawConnections.removeFirst() }
        val pool = ConnectionPool(factory, connectionCount = 2)
        pool.init()

        pool.close()

        verify(connection1).close()
        verify(connection2).close()
    }

    @Test
    fun shouldThrowWhenNextConnectionCalledAfterClose() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val factory = mockFactory()
        whenever(factory.newConnection()).thenReturn(rawConnection)
        val pool = ConnectionPool(factory)
        pool.init()

        pool.close()

        assertThatThrownBy { pool.nextConnection() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldThrowWhenConnectionFactoryHasAutomaticRecoveryDisabled() {
        val factory = ConnectionFactory().apply { isAutomaticRecoveryEnabled = false }

        assertThatThrownBy { ConnectionPool(factory) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
