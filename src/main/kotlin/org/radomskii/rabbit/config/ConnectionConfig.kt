package org.radomskii.rabbit.config

import java.time.Duration

/**
 * Configuration for the RabbitMQ connection pool.
 *
 * @param hosts broker hosts to connect to
 * @param port broker port, shared by all [hosts]
 * @param username credential used to authenticate
 * @param password credential used to authenticate
 * @param virtualHost virtual host to connect to
 * @param connectionTimeout TCP connection establishment timeout
 * @param heartbeatInterval requested AMQP heartbeat interval
 * @param networkRecoveryInterval delay between automatic recovery attempts after a connection is lost
 * @param connectionCount number of physical connections to open and round-robin across
 */
data class ConnectionConfig(
    val hosts: List<String>,
    val port: Int = 5672,
    val username: String,
    val password: String,
    val virtualHost: String = "/",
    val connectionTimeout: Duration = Duration.ofSeconds(30),
    val heartbeatInterval: Duration = Duration.ofSeconds(60),
    val networkRecoveryInterval: Duration = Duration.ofSeconds(5),
    val connectionCount: Int = 1
) {
    init {
        require(hosts.isNotEmpty()) { "hosts must not be empty" }
        require(hosts.none { it.isBlank() }) { "hosts must not contain blank entries" }
        require(port in 1..65535) { "port must be between 1 and 65535" }
        require(connectionCount > 0) { "connectionCount must be positive" }
    }
}
