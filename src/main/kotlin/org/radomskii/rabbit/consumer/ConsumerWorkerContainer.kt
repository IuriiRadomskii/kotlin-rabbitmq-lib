package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.config.ConsumerConfig
import org.radomskii.rabbit.resources.ConnectionPool
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

internal class ConsumerWorkerContainer<T>(
    private val connectionPool: ConnectionPool,
    private val config: ConsumerConfig<T>,
    private val handler: MessageHandler<T>
) {
    private val reconnectionConfig = config.reconnectionConfig
    private val workers = CopyOnWriteArrayList<ConsumerWorker<T>>()
    private val running = AtomicBoolean(false)
    private val scheduler = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory())
    private var supervisorTask: ScheduledFuture<*>? = null

    private companion object {
        val log = LoggerFactory.getLogger(ConsumerWorkerContainer::class.java)
        const val SCHEDULER_SHUTDOWN_MILLIS = 2_000L
        const val SHUTDOWN_GRACE_MILLIS = 2_000L
    }

    fun start() {
        check(running.compareAndSet(false, true)) { "ConsumerWorkerContainer already started" }
        log.trace("Starting consumer worker container: queues={}, workerPoolSize={}", config.queues, config.workerPoolSize)
        try {
            repeat(config.workerPoolSize) { workerId ->
                workers.add(createAndStartWorkerWithRetry(workerId))
            }
        } catch (e: Exception) {
            log.error("Failed to start consumer worker container, force-closing {} workers", workers.size, e)
            workers.forEach { it.forceClose() }
            workers.clear()
            running.set(false)
            throw e as? RabbitConsumerException ?: RabbitConsumerException("Failed to start consumer workers", e)
        }

        val pollIntervalMillis = config.supervisorPollInterval.toMillis()
        supervisorTask = scheduler.scheduleWithFixedDelay(
            ::checkWorkers, pollIntervalMillis, pollIntervalMillis, TimeUnit.MILLISECONDS
        )
        log.trace("Consumer worker container started: workers={}, supervisorPollIntervalMillis={}", workers.size, pollIntervalMillis)
    }

    private fun createAndStartWorkerWithRetry(workerId: Int): ConsumerWorker<T> {
        var lastError: Exception? = null
        repeat(reconnectionConfig.maxAttempts) { attempt ->
            try {
                return createAndStartWorker(workerId)
            } catch (e: Exception) {
                lastError = e
                log.warn(
                    "Failed to start consumer worker {} (attempt {}/{})",
                    workerId, attempt + 1, reconnectionConfig.maxAttempts, e
                )
                if (attempt < reconnectionConfig.maxAttempts - 1) {
                    awaitRetryInterval()
                }
            }
        }
        throw RabbitConsumerException(
            "Failed to start consumer worker $workerId after ${reconnectionConfig.maxAttempts} attempts", lastError
        )
    }

    private fun awaitRetryInterval() {
        try {
            scheduler.schedule({}, reconnectionConfig.retryInterval.toMillis(), TimeUnit.MILLISECONDS).get()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw RabbitConsumerException("Interrupted while waiting to retry consumer worker startup", e)
        }
    }

    private fun createAndStartWorker(workerId: Int): ConsumerWorker<T> {
        log.trace("Creating consumer worker {}", workerId)
        val connection = connectionPool.nextConnection()
        val channel = connection.createChannel()
        val worker = ConsumerWorker(workerId, channel, config, handler)
        worker.start()
        log.trace("Consumer worker {} created and started", workerId)
        return worker
    }

    private fun checkWorkers() {
        if (!running.get()) return
        try {
            for (index in workers.indices) {
                if (!running.get()) return
                val worker = workers[index]
                if (!worker.isAlive || worker.hasFailed) {
                    log.trace("Consumer worker {} unhealthy: isAlive={}, hasFailed={}, restarting", index, worker.isAlive, worker.hasFailed)
                    worker.forceClose()
                    try {
                        workers[index] = createAndStartWorker(index)
                    } catch (e: Exception) {
                        log.warn("Failed to recreate consumer worker {}, will retry on next check", index, e)
                    }
                }
            }
        } catch (e: Exception) {
            log.warn("Unexpected error while supervising consumer workers", e)
        }
    }

    fun stop(timeout: Duration) {
        if (!running.compareAndSet(true, false)) {
            log.trace("Consumer worker container already stopped")
            return
        }
        log.trace("Stopping consumer worker container: workers={}, timeout={}", workers.size, timeout)

        supervisorTask?.cancel(false)
        scheduler.shutdown()
        try {
            scheduler.awaitTermination(SCHEDULER_SHUTDOWN_MILLIS, TimeUnit.MILLISECONDS)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }

        val snapshot = workers.toList()
        if (snapshot.isEmpty()) {
            log.trace("Consumer worker container stopped: no workers to stop")
            return
        }

        val executor = Executors.newVirtualThreadPerTaskExecutor()
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
        log.trace("Consumer worker container stopped")
    }

}
