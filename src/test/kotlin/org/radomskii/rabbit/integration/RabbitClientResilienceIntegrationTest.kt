package org.radomskii.rabbit.integration

import com.rabbitmq.client.ConnectionFactory
import eu.rekawek.toxiproxy.Proxy
import eu.rekawek.toxiproxy.ToxiproxyClient
import eu.rekawek.toxiproxy.model.ToxicDirection
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.radomskii.rabbit.config.ConnectionPoolConfig
import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.config.PublisherConfig
import org.radomskii.rabbit.config.ReconnectionConfig
import org.radomskii.rabbit.consumer.RabbitConsumer
import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.model.MessagePayload
import org.radomskii.rabbit.publisher.RabbitPublisher
import org.radomskii.rabbit.resources.ConnectionPool
import org.radomskii.rabbit.resources.RabbitConnectionException
import org.radomskii.rabbit.serialization.JsonMessageSerializer
import org.testcontainers.containers.Network
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.containers.ToxiproxyContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

/**
 * Verifies that the client survives broken network conditions between the application and the
 * broker: connection establishment while the broker is unreachable, consumer/publisher recovery
 * once connectivity returns, and bounded (non-hanging) graceful shutdown while the outage is
 * still ongoing.
 *
 * Two fault-injection mechanisms are used:
 *  - Toxiproxy, sitting between the client and the broker, simulates a pure network outage
 *    (the broker process itself keeps running).
 *  - `rabbitmqctl stop_app` / `start_app`, executed inside the running broker container,
 *    simulates the broker application itself restarting without restarting the container
 *    (Testcontainers does not support reattaching to a fixed port after a container restart).
 */
@Testcontainers
@Tag("integration")
class RabbitClientResilienceIntegrationTest {

    private data class SampleEvent(val id: Int, val text: String)

    /**
     * Thin wrapper around a Toxiproxy [Proxy], built directly via [ToxiproxyClient] instead of the
     * deprecated `ToxiproxyContainer.getProxy(...)` convenience method.
     */
    private class RabbitProxy(
        private val proxy: Proxy,
        val containerIpAddress: String,
        val proxyPort: Int
    ) {
        private var cut = false

        /** Cuts (or restores) the connection by setting bandwidth in both directions to zero. */
        fun setConnectionCut(shouldCut: Boolean) {
            if (shouldCut) {
                proxy.toxics().bandwidth("cut-downstream", ToxicDirection.DOWNSTREAM, 0)
                proxy.toxics().bandwidth("cut-upstream", ToxicDirection.UPSTREAM, 0)
                cut = true
            } else if (cut) {
                proxy.toxics().get("cut-downstream").remove()
                proxy.toxics().get("cut-upstream").remove()
                cut = false
            }
        }
    }

    companion object {
        private const val RABBIT_PORT = 5672
        private const val PROXY_LISTEN_PORT = 8666
        private val serializer = JsonMessageSerializer.create<SampleEvent>()

        private val network: Network = Network.newNetwork()

        @Container
        @JvmStatic
        private val rabbitContainer: RabbitMQContainer =
            RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management-alpine"))
                .withNetwork(network)
                .withNetworkAliases("rabbitmq")

        @Container
        @JvmStatic
        private val toxiproxyContainer: ToxiproxyContainer =
            ToxiproxyContainer(DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.5.0"))
                .withNetwork(network)

        private lateinit var controlConnectionPool: ConnectionPool
        private lateinit var controlPublisher: RabbitPublisher<SampleEvent>
        private lateinit var rabbitProxy: RabbitProxy

        @BeforeAll
        @JvmStatic
        fun setUpAll() {
            val toxiproxyClient = ToxiproxyClient(toxiproxyContainer.host, toxiproxyContainer.controlPort)
            val proxy = toxiproxyClient.createProxy(
                "rabbitmq", "0.0.0.0:$PROXY_LISTEN_PORT", "rabbitmq:$RABBIT_PORT"
            )
            rabbitProxy = RabbitProxy(
                proxy = proxy,
                containerIpAddress = toxiproxyContainer.host,
                proxyPort = toxiproxyContainer.getMappedPort(PROXY_LISTEN_PORT)
            )

            controlConnectionPool = ConnectionPool.builder()
                .connectionFactory(directConnectionFactory())
                .build()
            awaitPoolReady(controlConnectionPool)
            controlPublisher = RabbitPublisher.builder<SampleEvent>()
                .connectionPool(controlConnectionPool)
                .config(PublisherConfig(serializer = serializer))
                .build()
        }

        @AfterAll
        @JvmStatic
        fun tearDownAll() {
            controlConnectionPool.close()
        }

        private fun directConnectionFactory(): ConnectionFactory = ConnectionFactory().apply {
            host = rabbitContainer.host
            port = rabbitContainer.amqpPort
            username = rabbitContainer.adminUsername
            password = rabbitContainer.adminPassword
            isAutomaticRecoveryEnabled = true
            networkRecoveryInterval = 500
            requestedHeartbeat = 2
            connectionTimeout = 3_000
        }

        /** Waits for a freshly-built pool's asynchronous first connection attempt to complete. */
        private fun awaitPoolReady(pool: ConnectionPool, timeoutSeconds: Long = 20) {
            await.atMost(timeoutSeconds, TimeUnit.SECONDS).until {
                runCatching { pool.nextConnection() }.getOrNull()?.isOpen == true
            }
        }

        /** A connection factory routed through the Toxiproxy proxy in front of the broker. */
        private fun proxiedConnectionFactory(proxy: RabbitProxy): ConnectionFactory =
            ConnectionFactory().apply {
                host = proxy.containerIpAddress
                port = proxy.proxyPort
                username = rabbitContainer.adminUsername
                password = rabbitContainer.adminPassword
                isAutomaticRecoveryEnabled = true
                networkRecoveryInterval = 500
                requestedHeartbeat = 2
                connectionTimeout = 3_000
            }

        private fun fastPoolConfig() = ConnectionPoolConfig(
            reconnectionConfig = ReconnectionConfig(maxAttempts = 30, retryInterval = Duration.ofMillis(300)),
            schedulerShutdownTimeout = Duration.ofSeconds(2)
        )

        private fun fastConsumerConfig(queue: String) = ConsumerConfig(
            queues = listOf(queue),
            deserializer = serializer,
            gracefulShutdownTimeout = Duration.ofSeconds(3),
            supervisorPollInterval = Duration.ofMillis(500),
            reconnectionConfig = ReconnectionConfig(maxAttempts = 30, retryInterval = Duration.ofMillis(300))
        )

        private fun declareTopology(exchange: String, queue: String, routingKey: String) {
            directConnectionFactory().newConnection().use { connection ->
                connection.createChannel().use { channel ->
                    channel.exchangeDeclare(exchange, "direct", true)
                    channel.queueDeclare(queue, true, false, false, null)
                    channel.queueBind(queue, exchange, routingKey)
                }
            }
        }

        private fun publishDirect(exchange: String, routingKey: String, payload: SampleEvent) {
            controlPublisher.publish(exchange, routingKey, payload)
        }

        private fun pollQueueDirect(queue: String, timeout: Duration = Duration.ofSeconds(10)): SampleEvent? {
            val deadline = System.nanoTime() + timeout.toNanos()
            directConnectionFactory().newConnection().use { connection ->
                connection.createChannel().use { channel ->
                    while (System.nanoTime() < deadline) {
                        val response = channel.basicGet(queue, true)
                        if (response != null) {
                            val payload = MessagePayload(
                                bytes = response.body,
                                contentType = response.props.contentType ?: "application/json",
                                contentEncoding = response.props.contentEncoding ?: "UTF-8"
                            )
                            return serializer.deserialize(payload)
                        }
                        Thread.sleep(200)
                    }
                }
            }
            return null
        }

        private fun stopBrokerApp() {
            val result = rabbitContainer.execInContainer("rabbitmqctl", "stop_app")
            check(result.exitCode == 0) { "rabbitmqctl stop_app failed: ${result.stderr}" }
        }

        private fun startBrokerApp() {
            val result = rabbitContainer.execInContainer("rabbitmqctl", "start_app")
            check(result.exitCode == 0) { "rabbitmqctl start_app failed: ${result.stderr}" }
        }
    }

    @Test
    fun shouldEstablishConnectionAfterBrokerBecomesReachableFollowingPoolStart() {
        val proxy = rabbitProxy
        proxy.setConnectionCut(true)

        val pool = ConnectionPool.builder()
            .connectionFactory(proxiedConnectionFactory(proxy))
            .connectionPoolConfig(fastPoolConfig())
            .build()

        try {
            assertThatThrownBy { pool.nextConnection() }.isInstanceOf(RabbitConnectionException::class.java)

            proxy.setConnectionCut(false)

            awaitPoolReady(pool, timeoutSeconds = 30)
        } finally {
            proxy.setConnectionCut(false)
            pool.close()
        }
    }

    @Test
    fun shouldResumeConsumingAfterNetworkOutageIsRestored() {
        val exchange = "resilience.exchange.consumer.network"
        val queue = "resilience.queue.consumer.network"
        val routingKey = "resilience.key.consumer.network"
        declareTopology(exchange, queue, routingKey)

        val proxy = rabbitProxy
        val pool = ConnectionPool.builder()
            .connectionFactory(proxiedConnectionFactory(proxy))
            .connectionPoolConfig(fastPoolConfig())
            .build()
        awaitPoolReady(pool)
        val received = LinkedBlockingQueue<SampleEvent>()
        val consumer = RabbitConsumer.builder<SampleEvent>()
            .connectionPool(pool)
            .config(fastConsumerConfig(queue))
            .build()
        consumer.start { message ->
            received.put(message.payload)
            ConsumeResult.Ack
        }

        try {
            publishDirect(exchange, routingKey, SampleEvent(1, "before-outage"))
            assertThat(received.poll(10, TimeUnit.SECONDS)).isEqualTo(SampleEvent(1, "before-outage"))

            proxy.setConnectionCut(true)
            publishDirect(exchange, routingKey, SampleEvent(2, "during-outage"))
            assertThat(received.poll(2, TimeUnit.SECONDS)).isNull()

            proxy.setConnectionCut(false)

            assertThat(received.poll(30, TimeUnit.SECONDS)).isEqualTo(SampleEvent(2, "during-outage"))
        } finally {
            proxy.setConnectionCut(false)
            consumer.stop()
            pool.close()
        }
    }

    @Test
    fun shouldPublishSuccessfullyAfterNetworkOutageIsRestored() {
        val exchange = "resilience.exchange.publisher.network"
        val queue = "resilience.queue.publisher.network"
        val routingKey = "resilience.key.publisher.network"
        declareTopology(exchange, queue, routingKey)

        val proxy = rabbitProxy
        val pool = ConnectionPool.builder()
            .connectionFactory(proxiedConnectionFactory(proxy))
            .connectionPoolConfig(fastPoolConfig())
            .build()
        awaitPoolReady(pool)
        val publisher = RabbitPublisher.builder<SampleEvent>()
            .connectionPool(pool)
            .config(PublisherConfig(serializer = serializer))
            .build()

        try {
            publisher.publish(exchange, routingKey, SampleEvent(10, "before-outage"))
            assertThat(pollQueueDirect(queue)).isEqualTo(SampleEvent(10, "before-outage"))

            proxy.setConnectionCut(true)
            await.atMost(20, TimeUnit.SECONDS).until {
                runCatching { pool.nextConnection() }.isFailure
            }

            proxy.setConnectionCut(false)
            await.atMost(20, TimeUnit.SECONDS).until {
                runCatching { publisher.publish(exchange, routingKey, SampleEvent(11, "after-outage")) }.isSuccess
            }

            assertThat(pollQueueDirect(queue)).isEqualTo(SampleEvent(11, "after-outage"))
        } finally {
            proxy.setConnectionCut(false)
            pool.close()
        }
    }

    @Test
    fun shouldShutDownGracefullyDuringNetworkOutage() {
        val exchange = "resilience.exchange.shutdown.network"
        val queue = "resilience.queue.shutdown.network"
        val routingKey = "resilience.key.shutdown.network"
        declareTopology(exchange, queue, routingKey)

        val proxy = rabbitProxy
        val pool = ConnectionPool.builder()
            .connectionFactory(proxiedConnectionFactory(proxy))
            .connectionPoolConfig(fastPoolConfig())
            .build()
        awaitPoolReady(pool)
        val consumer = RabbitConsumer.builder<SampleEvent>()
            .connectionPool(pool)
            .config(fastConsumerConfig(queue))
            .build()
        consumer.start { ConsumeResult.Ack }

        try {
            proxy.setConnectionCut(true)
            await.atMost(20, TimeUnit.SECONDS).until {
                runCatching { pool.nextConnection() }.isFailure
            }

            val consumerStopMillis = measureTimeMillis { consumer.stop() }
            val poolCloseMillis = measureTimeMillis { pool.close() }

            assertThat(consumer.isRunning()).isFalse()
            assertThat(consumerStopMillis).isLessThan(Duration.ofSeconds(10).toMillis())
            assertThat(poolCloseMillis).isLessThan(Duration.ofSeconds(10).toMillis())
        } finally {
            proxy.setConnectionCut(false)
        }
    }

    @Test
    fun shouldResumeConsumingAfterBrokerRestart() {
        val exchange = "resilience.exchange.consumer.broker-restart"
        val queue = "resilience.queue.consumer.broker-restart"
        val routingKey = "resilience.key.consumer.broker-restart"
        declareTopology(exchange, queue, routingKey)

        val pool = ConnectionPool.builder()
            .connectionFactory(directConnectionFactory())
            .connectionPoolConfig(fastPoolConfig())
            .build()
        awaitPoolReady(pool)
        val received = LinkedBlockingQueue<SampleEvent>()
        val consumer = RabbitConsumer.builder<SampleEvent>()
            .connectionPool(pool)
            .config(fastConsumerConfig(queue))
            .build()
        consumer.start { message ->
            received.put(message.payload)
            ConsumeResult.Ack
        }

        try {
            publishDirect(exchange, routingKey, SampleEvent(20, "before-restart"))
            assertThat(received.poll(10, TimeUnit.SECONDS)).isEqualTo(SampleEvent(20, "before-restart"))

            stopBrokerApp()
            startBrokerApp()

            await.atMost(30, TimeUnit.SECONDS).until {
                runCatching { publishDirect(exchange, routingKey, SampleEvent(21, "after-restart")) }.isSuccess
            }

            assertThat(received.poll(30, TimeUnit.SECONDS)).isEqualTo(SampleEvent(21, "after-restart"))
        } finally {
            consumer.stop()
            pool.close()
            runCatching { startBrokerApp() }
            runCatching { awaitPoolReady(controlConnectionPool) }
        }
    }

    @Test
    fun shouldPublishSuccessfullyAfterBrokerRestart() {
        val exchange = "resilience.exchange.publisher.broker-restart"
        val queue = "resilience.queue.publisher.broker-restart"
        val routingKey = "resilience.key.publisher.broker-restart"
        declareTopology(exchange, queue, routingKey)

        val pool = ConnectionPool.builder()
            .connectionFactory(directConnectionFactory())
            .connectionPoolConfig(fastPoolConfig())
            .build()
        awaitPoolReady(pool)
        val publisher = RabbitPublisher.builder<SampleEvent>()
            .connectionPool(pool)
            .config(PublisherConfig(serializer = serializer))
            .build()

        try {
            publisher.publish(exchange, routingKey, SampleEvent(30, "before-restart"))
            assertThat(pollQueueDirect(queue)).isEqualTo(SampleEvent(30, "before-restart"))

            stopBrokerApp()
            await.atMost(20, TimeUnit.SECONDS).until {
                runCatching { pool.nextConnection() }.isFailure
            }

            startBrokerApp()
            await.atMost(30, TimeUnit.SECONDS).until {
                runCatching { publisher.publish(exchange, routingKey, SampleEvent(31, "after-restart")) }.isSuccess
            }

            assertThat(pollQueueDirect(queue)).isEqualTo(SampleEvent(31, "after-restart"))
        } finally {
            pool.close()
            runCatching { startBrokerApp() }
            runCatching { awaitPoolReady(controlConnectionPool) }
        }
    }

    @Test
    fun shouldShutDownGracefullyWhileBrokerIsUnavailable() {
        val exchange = "resilience.exchange.shutdown.broker-restart"
        val queue = "resilience.queue.shutdown.broker-restart"
        val routingKey = "resilience.key.shutdown.broker-restart"
        declareTopology(exchange, queue, routingKey)

        val pool = ConnectionPool.builder()
            .connectionFactory(directConnectionFactory())
            .connectionPoolConfig(fastPoolConfig())
            .build()
        awaitPoolReady(pool)
        val consumer = RabbitConsumer.builder<SampleEvent>()
            .connectionPool(pool)
            .config(fastConsumerConfig(queue))
            .build()
        consumer.start { ConsumeResult.Ack }

        try {
            stopBrokerApp()
            await.atMost(20, TimeUnit.SECONDS).until {
                runCatching { pool.nextConnection() }.isFailure
            }

            val consumerStopMillis = measureTimeMillis { consumer.stop() }
            val poolCloseMillis = measureTimeMillis { pool.close() }

            assertThat(consumer.isRunning()).isFalse()
            assertThat(consumerStopMillis).isLessThan(Duration.ofSeconds(10).toMillis())
            assertThat(poolCloseMillis).isLessThan(Duration.ofSeconds(10).toMillis())
        } finally {
            runCatching { startBrokerApp() }
            runCatching { awaitPoolReady(controlConnectionPool) }
        }
    }
}
