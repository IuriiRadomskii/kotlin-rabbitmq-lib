package org.radomskii.rabbit.serialization

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class JsonMessageSerializerTest {

    data class SamplePayload(val id: Int, val name: String)

    @Test
    fun shouldRoundTripWhenSerializingSimpleDataClass() {
        val serializer = JsonMessageSerializer.create<SamplePayload>()
        val original = SamplePayload(id = 42, name = "widget")

        val wire = serializer.serialize(original)
        val restored = serializer.deserialize(wire)

        assertThat(restored).isEqualTo(original)
    }

    @Test
    fun shouldRoundTripWhenSerializingGenericListType() {
        val serializer = JsonMessageSerializer.create<List<SamplePayload>>()
        val original = listOf(SamplePayload(1, "a"), SamplePayload(2, "b"))

        val wire = serializer.serialize(original)
        val restored = serializer.deserialize(wire)

        assertThat(restored).isEqualTo(original)
    }

    @Test
    fun shouldProduceApplicationJsonContentType() {
        val serializer = JsonMessageSerializer.create<String>()

        val wire = serializer.serialize("hello")

        assertThat(wire.contentType).isEqualTo("application/json")
        assertThat(wire.contentEncoding).isEqualTo("UTF-8")
    }
}
