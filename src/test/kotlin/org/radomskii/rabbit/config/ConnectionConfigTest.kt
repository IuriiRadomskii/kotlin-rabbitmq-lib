package org.radomskii.rabbit.config

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.assertThatCode
import org.junit.jupiter.api.Test

class ConnectionConfigTest {

    private fun validConfig(
        hosts: List<String> = listOf("localhost"),
        port: Int = 5672,
        connectionCount: Int = 1
    ) = ConnectionConfig(
        hosts = hosts,
        port = port,
        username = "guest",
        password = "guest",
        connectionCount = connectionCount
    )

    @Test
    fun shouldAcceptWhenAllFieldsValid() {
        assertThatCode { validConfig() }.doesNotThrowAnyException()
    }

    @Test
    fun shouldThrowWhenHostsEmpty() {
        assertThatThrownBy { validConfig(hosts = emptyList()) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenHostsContainBlankEntry() {
        assertThatThrownBy { validConfig(hosts = listOf("localhost", "  ")) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenPortOutOfRange() {
        assertThatThrownBy { validConfig(port = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { validConfig(port = 70000) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun shouldThrowWhenConnectionCountNotPositive() {
        assertThatThrownBy { validConfig(connectionCount = 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
