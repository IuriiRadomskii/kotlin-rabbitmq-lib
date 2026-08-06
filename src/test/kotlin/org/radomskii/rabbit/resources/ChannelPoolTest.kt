package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.radomskii.rabbit.config.ChannelPoolConfig
import java.time.Duration

class ChannelPoolTest {

    private fun poolConfig(max: Int = 2, timeout: Duration = Duration.ofMillis(200)) =
        ChannelPoolConfig(maxChannelsPerConnection = max, acquisitionTimeout = timeout)

    @Test
    fun shouldCreateNewChannelWhenIdleQueueEmpty() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = ChannelPool({ rawChannel }, poolConfig())

        val acquired = pool.acquire()

        assertThat(acquired.rawChannel()).isSameAs(rawChannel)
    }

    @Test
    fun shouldReuseReleasedChannelInsteadOfCreatingNewOne() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = ChannelPool({ rawChannel }, poolConfig())
        val first = pool.acquire()
        pool.release(first)

        val second = pool.acquire()

        assertThat(second).isSameAs(first)
    }

    @Test
    fun shouldSkipStaleIdleChannelsAndCreateNewOne() {
        val rawChannel1 = mock<Channel>()
        val rawChannel2 = mock<Channel>()
        whenever(rawChannel1.isOpen).thenReturn(true)
        whenever(rawChannel2.isOpen).thenReturn(true)
        val channels = ArrayDeque(listOf(rawChannel1, rawChannel2))
        val pool = ChannelPool({ channels.removeFirst() }, poolConfig(max = 2))

        val first = pool.acquire()
        pool.release(first)
        whenever(rawChannel1.isOpen).thenReturn(false)

        val second = pool.acquire()

        assertThat(second.rawChannel()).isSameAs(rawChannel2)
    }

    @Test
    fun shouldThrowWhenAcquisitionTimesOutBecauseAllPermitsHeld() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = ChannelPool({ rawChannel }, poolConfig(max = 1, timeout = Duration.ofMillis(100)))
        pool.acquire()

        assertThatThrownBy { pool.acquire() }
            .isInstanceOf(RabbitConnectionException::class.java)
    }

    @Test
    fun shouldAllowNewAcquireAfterDiscardReleasesPermit() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = ChannelPool({ rawChannel }, poolConfig(max = 1))
        val first = pool.acquire()

        pool.discard(first)
        val second = pool.acquire()

        assertThat(second).isNotNull()
        verify(rawChannel).close()
    }

    @Test
    fun shouldRejectAcquireAfterPoolClosed() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = ChannelPool({ rawChannel }, poolConfig())

        pool.close()

        assertThatThrownBy { pool.acquire() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun shouldCloseChannelInsteadOfPoolingWhenReleasedAfterPoolClosed() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = ChannelPool({ rawChannel }, poolConfig())
        val channel = pool.acquire()

        pool.close()
        pool.release(channel)

        verify(rawChannel).close()
    }
}
