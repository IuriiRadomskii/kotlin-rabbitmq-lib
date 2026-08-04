package org.radomskii.rabbit.config

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Duration

class ChannelPoolConfigTest {

    @Test
    fun shouldAcceptWhenAllFieldsValid() {
        assertThatCode { ChannelPoolConfig(maxChannelsPerConnection = 5, acquisitionTimeout = Duration.ofSeconds(1)) }
            .doesNotThrowAnyException()
    }

    @Test
    fun shouldThrowWhenMaxChannelsNotPositive() {
        assertThatThrownBy { ChannelPoolConfig(maxChannelsPerConnection = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenAcquisitionTimeoutNegative() {
        assertThatThrownBy { ChannelPoolConfig(acquisitionTimeout = Duration.ofSeconds(-1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
