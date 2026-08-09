package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.after
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
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

    private fun awaitConnection(pool: ConnectionPool, timeoutMillis: Long = 2_000): ManagedConnection {
        val deadline = System.currentTimeMillis() + timeoutMillis
        var lastError: RabbitConnectionException? = null
        while (System.currentTimeMillis() < deadline) {
            try {
                return pool.nextConnection()
            } catch (e: RabbitConnectionException) {
                lastError = e
                Thread.sleep(10)
            }
        }
        throw lastError ?: AssertionError("No connection became available within ${timeoutMillis}ms")
    }

    private fun awaitDistinctConnectionCount(pool: ConnectionPool, expected: Int, timeoutMillis: Long = 2_000) {
        val deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            val ids = (1..expected * 4).map { pool.nextConnection().id }.distinct()
            if (ids.size >= expected) return
            Thread.sleep(10)
        }
        throw AssertionError("Pool never reached $expected distinct connections within ${timeoutMillis}ms")
    }

    @Test
    fun shouldAsynchronouslyOpenFirstConnectionDuringInit() {
        val connection = mock<Connection>()
        whenever(connection.isOpen).thenReturn(true)
        val factory = mockFactory()
        whenever(factory.newConnection()).thenReturn(connection)
        val pool = ConnectionPool(factory)

        pool.init()

        assertThat(awaitConnection(pool)).isNotNull()
        verify(factory, times(1)).newConnection()
    }

    @Test
    fun shouldAsynchronouslyRetryConnectionFactoryThenSucceed() {
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

        assertThat(awaitConnection(pool)).isNotNull()
        assertThat(attempts).isEqualTo(3)
    }

    @Test
    fun shouldGiveUpOpeningFirstConnectionAfterExhaustingReconnectionAttempts() {
        val factory = mockFactory()
        var attempts = 0
        val failure = RuntimeException("boom")
        whenever(factory.newConnection()).thenAnswer {
            attempts++
            throw failure
        }
        val pool = ConnectionPool(factory, reconnectionConfig = fastReconnectionConfig(maxAttempts = 2))

        pool.init()

        verify(factory, timeout(2_000).times(2)).newConnection()
        assertThat(attempts).isEqualTo(2)
        assertThatThrownBy { pool.nextConnection() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldBeIdempotentWhenInitCalledTwice() {
        val rawConnection = mock<Connection>()
        whenever(rawConnection.isOpen).thenReturn(true)
        val factory = mockFactory()
        whenever(factory.newConnection()).thenReturn(rawConnection)
        val pool = ConnectionPool(factory)
        pool.init()
        awaitConnection(pool)

        pool.init()

        verify(factory, after(300).times(1)).newConnection()
    }

    @Test
    fun shouldThrowWhenNextConnectionCalledBeforeInit() {
        val pool = ConnectionPool(mockFactory())

        assertThatThrownBy { pool.nextConnection() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldReuseSingleConnectionWhenConnectionCountIsOne() {
        val connection = mock<Connection>()
        whenever(connection.isOpen).thenReturn(true)
        val factory = mockFactory()
        whenever(factory.newConnection()).thenReturn(connection)
        val pool = ConnectionPool(factory, connectionCount = 1)
        pool.init()

        val first = awaitConnection(pool)
        val second = pool.nextConnection()

        assertThat(second).isSameAs(first)
        verify(factory, times(1)).newConnection()
    }

    @Test
    fun shouldAsynchronouslyOpenAdditionalConnectionWhenExistingConnectionNearsChannelCapacity() {
        val firstRawConnection = mock<Connection>()
        val firstRawChannel = mock<Channel>()
        whenever(firstRawConnection.isOpen).thenReturn(true)
        whenever(firstRawConnection.channelMax).thenReturn(4)
        whenever(firstRawConnection.createChannel()).thenReturn(firstRawChannel)
        val secondRawConnection = mock<Connection>()
        whenever(secondRawConnection.isOpen).thenReturn(true)
        val factory = mockFactory()
        var callCount = 0
        whenever(factory.newConnection()).thenAnswer {
            callCount++
            if (callCount == 1) firstRawConnection else secondRawConnection
        }
        val pool = ConnectionPool(
            factory,
            connectionCount = 2,
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 1)
        )
        pool.init()

        val first = awaitConnection(pool)
        repeat(3) { first.createChannel() } // 3/4 = 75% of channelMax

        verify(factory, timeout(2_000).times(2)).newConnection()
        awaitDistinctConnectionCount(pool, 2)
        val selected = (1..4).map { pool.nextConnection() }
        assertThat(selected).contains(first)
        assertThat(selected.map { it.id }.distinct()).hasSize(2)
    }

    @Test
    fun shouldNotScaleBeyondConfiguredConnectionCount() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.channelMax).thenReturn(4)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val factory = mockFactory()
        whenever(factory.newConnection()).thenReturn(rawConnection)
        val pool = ConnectionPool(
            factory,
            connectionCount = 1,
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 1)
        )
        pool.init()

        val connection = awaitConnection(pool)
        repeat(4) { connection.createChannel() }

        verify(factory, after(300).times(1)).newConnection()
    }

    @Test
    fun shouldKeepUsingExistingConnectionWhenAsyncScaleUpFails() {
        val rawConnection = mock<Connection>()
        val rawChannel = mock<Channel>()
        whenever(rawConnection.isOpen).thenReturn(true)
        whenever(rawConnection.channelMax).thenReturn(4)
        whenever(rawConnection.createChannel()).thenReturn(rawChannel)
        val factory = mockFactory()
        var callCount = 0
        whenever(factory.newConnection()).thenAnswer {
            callCount++
            if (callCount == 1) rawConnection else throw RuntimeException("boom")
        }
        val pool = ConnectionPool(
            factory,
            connectionCount = 2,
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 1)
        )
        pool.init()

        val first = awaitConnection(pool)
        repeat(3) { first.createChannel() }

        verify(factory, timeout(2_000).times(2)).newConnection()
        assertThat(pool.nextConnection()).isSameAs(first)
    }

    @Test
    fun shouldThrowWhenInitCalledAfterClose() {
        val pool = ConnectionPool(mockFactory())
        pool.close()

        assertThatThrownBy { pool.init() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun shouldCloseAllOpenedConnectionsOnClose() {
        val connection1 = mock<Connection>()
        val connection2 = mock<Connection>()
        val channel1 = mock<Channel>()
        whenever(connection1.isOpen).thenReturn(true)
        whenever(connection1.channelMax).thenReturn(4)
        whenever(connection1.createChannel()).thenReturn(channel1)
        whenever(connection2.isOpen).thenReturn(true)
        val factory = mockFactory()
        var callCount = 0
        whenever(factory.newConnection()).thenAnswer {
            callCount++
            if (callCount == 1) connection1 else connection2
        }
        val pool = ConnectionPool(
            factory,
            connectionCount = 2,
            reconnectionConfig = fastReconnectionConfig(maxAttempts = 1)
        )
        pool.init()
        val first = awaitConnection(pool)
        repeat(3) { first.createChannel() }
        awaitDistinctConnectionCount(pool, 2)

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
        awaitConnection(pool)

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
