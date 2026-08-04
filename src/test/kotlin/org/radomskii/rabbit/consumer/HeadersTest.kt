package org.radomskii.rabbit.consumer

import com.rabbitmq.client.LongString
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.DataInputStream

private class FakeLongString(private val value: String) : LongString {
    override fun length(): Long = getBytes().size.toLong()
    override fun getStream(): DataInputStream = DataInputStream(value.byteInputStream())
    override fun getBytes(): ByteArray = value.toByteArray(Charsets.UTF_8)
    override fun toString(): String = value
}

class HeadersTest {

    @Test
    fun shouldReturnEmptyMapWhenHeadersNull() {
        assertThat(normalizeHeaders(null)).isEmpty()
    }

    @Test
    fun shouldConvertLongStringHeaderValuesToStringWhenNormalizing() {
        val headers = mapOf<String, Any>("trace-id" to FakeLongString("abc-123"))

        val normalized = normalizeHeaders(headers)

        assertThat(normalized["trace-id"]).isEqualTo("abc-123")
        assertThat(normalized["trace-id"]).isInstanceOf(String::class.java)
    }

    @Test
    fun shouldPassThroughNonLongStringValuesUnchanged() {
        val headers = mapOf<String, Any>("retry-count" to 3, "urgent" to true)

        val normalized = normalizeHeaders(headers)

        assertThat(normalized["retry-count"]).isEqualTo(3)
        assertThat(normalized["urgent"]).isEqualTo(true)
    }
}
