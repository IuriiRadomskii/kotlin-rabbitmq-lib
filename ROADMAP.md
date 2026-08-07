# Roadmap

## Completed

- Basic library architecture:
    - **resources** - `ConnectionPool`, `ManagedConnection`
    - **publisher** - `RabbitPublisher`
    - **consumer** - `RabbitConsumer`, `ConsumerWorker`, `ConsumerWorkerContainer`
    - **config** - `PublisherConfig`, `ConsumerConfig`, `ReconnectionConfig`
    - **model** - `MessagePayload`, `MessageMetadata`, `IncomingMessage`, `ConsumeResult`
    - **serialization** - `MessageSerializer`, `JsonMessageSerializer` (Gson)
    - `RabbitMQClient` - entry point/builder, owns `ConnectionPool`, creates publishers and consumers
- Unit tests (JUnit 5, Mockito, AssertJ) for config, model, serialization, resources, and consumer - 68 tests, `./gradlew test` doesn't require Docker
- Integration tests via Testcontainers (`RabbitClientIntegrationTest`, tag `integration`, separate task `./gradlew integrationTest`):
    - publish/consume round-trip
    - mandatory publish to nowhere -> `MessageReturnedException`
    - `Nack(requeue = true)` -> message redelivery
- `logback-test.xml` for readable test output
- README updated with testing section
- Restored `RabbitPublisher.close()`/`isClosed()` with in-flight publish tracking (`publish` increments/decrements an `inFlight` counter around the whole call; `close()` sets `closed` and waits on a monitor, bounded by `PublisherConfig.closeTimeout`, for in-flight publishes to finish). `./gradlew compileKotlin` and `./gradlew test` pass again.
- `ConnectionPool` startup lifecycle: added `ReconnectionConfig` (default 3 attempts / 10s apart, reusable later by `ConsumerWorkerContainer`); split construction from a new public `init()`, both guarded by a `lifecycleLock`; connection creation is now injectable (`connectionFactory: () -> Connection`, mirroring `ChannelPool`'s `channelFactory` seam) so `connectWithRetry()` can be unit-tested without Docker; `init()` cleans up any partially-opened connections on failure; `nextConnection()` now checks `initialized` as well as `closed`. Wired into `RabbitMQClient` (`Builder.reconnectionConfig(...)`, calls `connectionPool.init()` in its own `init` block) so `build()` behaves the same as before from the library consumer's point of view. New `ConnectionPoolTest` (9 tests). Deliberately left out: waiting for all channels to close in `close()` - depends on the `ChannelPool`-vs-create-per-publish decision below.
- `ConnectionPool` now takes a client-supplied, pre-configured `com.rabbitmq.client.ConnectionFactory` instead of the library's own `ConnectionConfig` (removed). New constructor params `addresses: List<Address>` (default empty -> `factory.newConnection()`, otherwise `factory.newConnection(addresses)`) and `connectionCount: Int` (default 1) cover what `ConnectionFactory` itself doesn't. The injectable retry-seam lambda was renamed `connectionFactory` -> `connectionSupplier` to free the name. Added `require(connectionFactory.isAutomaticRecoveryEnabled)` in `init {}` - the pool's resilience story depends on it, so a misconfigured client factory fails fast instead of silently never recovering. `RabbitMQClient.Builder` gained `connectionFactory(...)`/`addresses(...)`/`connectionCount(...)`, replacing `connectionConfig(...)`. `ConnectionPoolTest` grew 3 tests (automatic-recovery validation, no-arg vs addressed `newConnection` dispatch); `RabbitClientIntegrationTest` updated to build its own `ConnectionFactory`.
- Removed channel pooling entirely: `ChannelPool`, `ManagedChannel`, and `config.ChannelPoolConfig` are deleted. `ManagedConnection` now takes a single `delegate: Connection` constructor parameter (renamed from `connection`) and exposes one factory method, `createChannel()`, returning a raw `com.rabbitmq.client.Channel` (`Channel extends AutoCloseable`, so no wrapper is needed) - used by both publishers and consumer workers, replacing the old `acquireChannel()`/`createDedicatedChannel()` split. `RabbitPublisher.publish()` now opens a fresh channel per call and closes it unconditionally in a `finally` block after publishing (mandatory or not) - the old invalidate-vs-release distinction is gone since there's no pool to return a broken channel to. `ConsumerWorkerContainer` still keeps its worker channel open for the worker's lifetime, just via the renamed `createChannel()`. `ConnectionPool` no longer takes a `channelPoolConfig` parameter; its `close()` TODO about waiting for channels is resolved implicitly - closing the underlying `Connection` already closes any channels opened on it. `RabbitMQClient.Builder` lost `channelPoolConfig(...)`.

## Open Questions from Author (from TODO comments in code)

### `resources/ConnectionPool.kt`
- Replace round-robin connection selection with load balancing by number of open channels per connection (channel count per connection should be approximately equal at any time); warn log when approaching `Connection#channelMax` threshold
- Don't create all connections eagerly - open new connection when channel count on existing connection approaches ~75% of `channelMax`

### `consumer/ConsumerWorker.kt`
- Question: what to do if `deliveryQueue.poll` throws `InterruptedException` - should `Thread.currentThread().interrupt()` be called?

### `consumer/ConsumerWorkerContainer.kt`
- `nextConnection()`/`createChannel()` can throw exception if RabbitMQ is unavailable at startup - need try-catch and several attempts; reuse the new `config.ReconnectionConfig` (already used by `ConnectionPool`) as a `RabbitConsumer` parameter instead of designing a separate strategy
- `supervise()` - remove `Thread.sleep`, use `ScheduledExecutorService`
- executor in `stop()` - consider replacing with virtual-thread pool executor

### `consumer/RabbitConsumer.kt`
- `start()` should be protected by `lifecycleLock`

## Proposed Next Steps

1. Make decisions on open architectural questions above:
    - lazy connection creation as channel count grows + load balancing instead of round-robin
    - wire `ReconnectionConfig` into `ConsumerWorkerContainer`/`RabbitConsumer`
    - `lifecycleLock` around `RabbitConsumer.start()`
2. Implement accepted decisions
3. Update and expand unit and integration tests for new logic
4. Run `./gradlew test` and `./gradlew integrationTest`