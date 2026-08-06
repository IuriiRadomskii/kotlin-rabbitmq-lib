package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

class ManagedChannelTest {

    @Test
    fun shouldReleaseToPoolWhenClosedWithoutInvalidate() {
        val rawChannel = mock<Channel>()
        val pool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, pool)

        managedChannel.close()

        verify(pool).release(managedChannel)
        verify(pool, never()).discard(managedChannel)
    }

    @Test
    fun shouldDiscardWhenClosedAfterInvalidate() {
        val rawChannel = mock<Channel>()
        val pool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, pool)

        managedChannel.invalidate()
        managedChannel.close()

        verify(pool).discard(managedChannel)
        verify(pool, never()).release(managedChannel)
    }

    @Test
    fun shouldBeIdempotentWhenClosedMultipleTimes() {
        val rawChannel = mock<Channel>()
        val pool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, pool)

        managedChannel.close()
        managedChannel.close()
        managedChannel.close()

        verify(pool, times(1)).release(managedChannel)
    }

    @Test
    fun shouldReflectUnderlyingChannelOpenState() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(true)
        val pool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, pool)

        assertThat(managedChannel.isOpen).isTrue()
    }

    @Test
    fun shouldThrowWhenRawChannelRequestedAfterUnderlyingChannelClosed() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenReturn(false)
        val pool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, pool)

        assertThatThrownBy { managedChannel.rawChannel() }
            .isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun shouldSwallowExceptionWhenClosingQuietlyOnAlreadyBrokenChannel() {
        val rawChannel = mock<Channel>()
        whenever(rawChannel.isOpen).thenThrow(RuntimeException("broker unreachable"))
        val pool = mock<ChannelPool>()
        val managedChannel = ManagedChannel(rawChannel, pool)

        assertThatCodeDoesNotThrow { managedChannel.closeQuietly() }
    }

    private fun assertThatCodeDoesNotThrow(block: () -> Unit) {
        org.assertj.core.api.Assertions.assertThatCode(block).doesNotThrowAnyException()
    }
}
