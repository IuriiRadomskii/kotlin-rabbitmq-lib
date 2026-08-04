package org.radomskii.rabbit.model

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class MessagePayloadTest {

    @Test
    fun shouldBeEqualWhenBytesContentEqualButDifferentArrayInstances() {
        val first = MessagePayload("hello".toByteArray(), "text/plain")
        val second = MessagePayload("hello".toByteArray(), "text/plain")

        assertThat(first).isEqualTo(second)
        assertThat(first.hashCode()).isEqualTo(second.hashCode())
    }

    @Test
    fun shouldNotBeEqualWhenBytesDiffer() {
        val first = MessagePayload("hello".toByteArray(), "text/plain")
        val second = MessagePayload("world".toByteArray(), "text/plain")

        assertThat(first).isNotEqualTo(second)
    }

    @Test
    fun shouldNotBeEqualWhenContentTypeDiffers() {
        val first = MessagePayload("hello".toByteArray(), "text/plain")
        val second = MessagePayload("hello".toByteArray(), "application/json")

        assertThat(first).isNotEqualTo(second)
    }

    @Test
    fun shouldUseUtf8AsDefaultEncoding() {
        val payload = MessagePayload("hello".toByteArray(), "text/plain")

        assertThat(payload.contentEncoding).isEqualTo("UTF-8")
    }
}
