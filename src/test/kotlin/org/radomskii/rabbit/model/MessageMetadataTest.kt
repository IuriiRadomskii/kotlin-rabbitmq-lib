package org.radomskii.rabbit.model

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class MessageMetadataTest {

    @Test
    fun shouldGenerateUniqueMessageIdWhenNotSpecified() {
        val first = MessageMetadata()
        val second = MessageMetadata()

        assertThat(first.messageId).isNotBlank()
        assertThat(first.messageId).isNotEqualTo(second.messageId)
    }

    @Test
    fun shouldUseEmptyHeadersWhenNotSpecified() {
        val metadata = MessageMetadata()

        assertThat(metadata.headers).isEmpty()
    }

    @Test
    fun shouldThrowWhenMessageIdBlank() {
        assertThatThrownBy { MessageMetadata(messageId = "  ") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenPriorityOutOfRange() {
        assertThatThrownBy { MessageMetadata(priority = 256) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldAcceptBoundaryPriorityValues() {
        assertThat(MessageMetadata(priority = 0).priority).isEqualTo(0)
        assertThat(MessageMetadata(priority = 255).priority).isEqualTo(255)
    }
}
