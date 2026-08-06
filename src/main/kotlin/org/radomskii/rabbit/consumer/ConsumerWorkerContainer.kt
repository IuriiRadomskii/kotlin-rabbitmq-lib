package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.resources.ConnectionPool
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Owns the pool of [ConsumerWorker]s for a single [RabbitConsumer], starting them, monitoring
 * their health on a virtual-thread supervisor, recreating any that die unexpectedly, and
 * stopping them all (concurrently, bounded by a timeout) on shutdown.
 */
internal class ConsumerWorkerContainer<T>(
    private val connectionPool: ConnectionPool,
    private val config: ConsumerConfig<T>,
    private val handler: MessageHandler<T>
) {
    private val workers = CopyOnWriteArrayList<ConsumerWorker<T>>()
    private val running = AtomicBoolean(false)
    private var supervisor: Thread? = null

    fun start() {
        check(running.compareAndSet(false, true)) { "ConsumerWorkerContainer already started" }

        try {
            repeat(config.workerPoolSize) { workerId ->
                workers.add(createAndStartWorker(workerId))
            }
        } catch (e: Exception) {
            workers.forEach { it.forceClose() }
            workers.clear()
            running.set(false)
            throw RabbitConsumerException("Failed to start consumer workers", e)
        }

        supervisor = Thread.ofVirtual().name("rabbit-consumer-supervisor").start { supervise() }
    }

    private fun createAndStartWorker(workerId: Int): ConsumerWorker<T> {
        val connection = connectionPool.nextConnection()//TODO nextConnection can throw an exception if RabbitMQ instance is unavailable at the start of the client application, so there has to be try-catch on nextConnection and several tries to obtain connection to rabbitmq instance.
        //Reconnection strategy has to be defined as the RabbitConsumer parameters. The default strategy is 3 tries every 10 seconds and then exception if rabbitmq is still unavailable
        val channel = connection.createDedicatedChannel()//TODO same for channels
        val worker = ConsumerWorker(workerId, channel, config, handler)
        worker.start()
        return worker
    }

    private fun supervise() {//TODO supervision should be implemented without Thread.sleep. Use ScheduledExecutorService.
        try {
            while (running.get()) {
                Thread.sleep(config.supervisorPollInterval.toMillis())
                if (!running.get()) return

                for (index in workers.indices) {
                    val worker = workers[index]
                    if (!running.get()) return
                    if (!worker.isAlive || worker.hasFailed) {
                        worker.forceClose()
                        try {
                            workers[index] = createAndStartWorker(index)
                        } catch (e: Exception) {
                            log.warn("Failed to recreate consumer worker {}, will retry on next check", index, e)
                        }
                    }
                }
            }
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /**
     * Stop the supervisor and all workers, waiting up to [timeout] in total for in-flight
     * handler invocations to finish.
     */
    fun stop(timeout: Duration) {
        if (!running.compareAndSet(true, false)) return

        supervisor?.interrupt()
        supervisor?.join(SUPERVISOR_JOIN_MILLIS)

        val snapshot = workers.toList()
        if (snapshot.isEmpty()) return

        val executor = Executors.newFixedThreadPool(snapshot.size)//TODO This can be virtual thread pool executor. prove me wrong if needed.
        try {
            val futures = snapshot.map { worker -> executor.submit { worker.stop(timeout) } }
            futures.forEach { future ->
                try {
                    future.get(timeout.toMillis() + SHUTDOWN_GRACE_MILLIS, TimeUnit.MILLISECONDS)
                } catch (e: Exception) {
                    log.warn("Consumer worker did not stop cleanly within the shutdown timeout", e)
                }
            }
        } finally {
            executor.shutdown()
        }
        workers.clear()
    }

    private companion object {
        val log = LoggerFactory.getLogger(ConsumerWorkerContainer::class.java)
        const val SUPERVISOR_JOIN_MILLIS = 2_000L
        const val SHUTDOWN_GRACE_MILLIS = 2_000L
    }
}
