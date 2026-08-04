package org.radomskii.rabbit.config

import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.radomskii.rabbit.serialization.JsonMessageSerializer

class ConsumerConfigTest {

    private val deserializer = JsonMessageSerializer.create<String>()

    private fun validConfig(
        queues: List<String> = listOf("my-queue"),
        workerPoolSize: Int = 1,
        prefetchCount: Int = 1,
        queueCapacity: Int = 100
    ) = ConsumerConfig(
        queues = queues,
        deserializer = deserializer,
        workerPoolSize = workerPoolSize,
        prefetchCount = prefetchCount,
        queueCapacity = queueCapacity
    )

    @Test
    fun shouldAcceptWhenAllFieldsValid() {
        assertThatCode { validConfig() }.doesNotThrowAnyException()
    }

    @Test
    fun shouldThrowWhenQueuesEmpty() {
        assertThatThrownBy { validConfig(queues = emptyList()) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenQueuesContainBlankEntry() {
        assertThatThrownBy { validConfig(queues = listOf("ok", " ")) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenWorkerPoolSizeNotPositive() {
        assertThatThrownBy { validConfig(workerPoolSize = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenPrefetchCountNotPositive() {
        assertThatThrownBy { validConfig(prefetchCount = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenQueueCapacityNotPositive() {
        assertThatThrownBy { validConfig(queueCapacity = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
