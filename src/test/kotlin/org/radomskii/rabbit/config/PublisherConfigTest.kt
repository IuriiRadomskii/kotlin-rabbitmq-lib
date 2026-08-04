package org.radomskii.rabbit.config

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.radomskii.rabbit.serialization.JsonMessageSerializer
import java.time.Duration

class PublisherConfigTest {

    private val serializer = JsonMessageSerializer.create<String>()

    @Test
    fun shouldAcceptWhenAllFieldsValid() {
        assertThatCode { PublisherConfig(serializer = serializer) }.doesNotThrowAnyException()
    }

    @Test
    fun shouldThrowWhenReturnListenerTimeoutNegative() {
        assertThatThrownBy {
            PublisherConfig(serializer = serializer, returnListenerTimeout = Duration.ofSeconds(-1))
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenCloseTimeoutNegative() {
        assertThatThrownBy {
            PublisherConfig(serializer = serializer, closeTimeout = Duration.ofSeconds(-1))
        }.isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldDefaultMandatoryToFalse() {
        val config = PublisherConfig(serializer = serializer)

        assertThat(config.mandatory).isFalse()
    }
}
