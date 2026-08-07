# Roadmap

## Completed

- Basic library architecture:
    - **resources** - `ConnectionPool`, `ChannelPool`, `ManagedConnection`, `ManagedChannel`
    - **publisher** - `RabbitPublisher`
    - **consumer** - `RabbitConsumer`, `ConsumerWorker`, `ConsumerWorkerContainer`
    - **config** - `ChannelPoolConfig`, `PublisherConfig`, `ConsumerConfig`, `ReconnectionConfig`
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

## Open Questions from Author (from TODO comments in code)

### `resources/ConnectionPool.kt`
- Replace round-robin connection selection with load balancing by number of open channels per connection (channel count per connection should be approximately equal at any time); warn log when approaching `Connection#channelMax` threshold
- Don't create all connections eagerly - open new connection when channel count on existing connection approaches ~75% of `channelMax`
- `close()` - wait for all channels spawned by the connection to close (now under `lifecycleLock`, but doesn't wait yet)

### `resources/ManagedConnection.kt`
- Question: perhaps channels don't need to be pooled at all - create channel for publish and close it immediately after use, instead of `ChannelPool`
- No need in ManagedChannel object

### `consumer/ConsumerWorker.kt`
- Question: what to do if `deliveryQueue.poll` throws `InterruptedException` - should `Thread.currentThread().interrupt()` be called?

### `consumer/ConsumerWorkerContainer.kt`
- `nextConnection()`/`createDedicatedChannel()` can throw exception if RabbitMQ is unavailable at startup - need try-catch and several attempts; reuse the new `config.ReconnectionConfig` (already used by `ConnectionPool`) as a `RabbitConsumer` parameter instead of designing a separate strategy
- `supervise()` - remove `Thread.sleep`, use `ScheduledExecutorService`
- executor in `stop()` - consider replacing with virtual-thread pool executor

### `consumer/RabbitConsumer.kt`
- `start()` should be protected by `lifecycleLock`

## Proposed Next Steps

1. Make decisions on open architectural questions above:
    - channel pool vs create-per-publish
    - lazy connection creation as channel count grows + load balancing instead of round-robin
    - wire `ReconnectionConfig` into `ConsumerWorkerContainer`/`RabbitConsumer`
    - `lifecycleLock` around `RabbitConsumer.start()`
2. Implement accepted decisions
3. Update and expand unit and integration tests for new logic
4. Run `./gradlew test` and `./gradlew integrationTest`