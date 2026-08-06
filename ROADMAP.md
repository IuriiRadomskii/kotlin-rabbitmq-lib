# Roadmap

## Completed

- Basic library architecture:
    - **resources** - `ConnectionPool`, `ChannelPool`, `ManagedConnection`, `ManagedChannel`
    - **publisher** - `RabbitPublisher`
    - **consumer** - `RabbitConsumer`, `ConsumerWorker`, `ConsumerWorkerContainer`
    - **config** - `ConnectionConfig`, `ChannelPoolConfig`, `PublisherConfig`, `ConsumerConfig`
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

## Known Issue (Build Blocker)

`RabbitMQClient.kt:56` calls `RabbitPublisher.close()`, but the method (along with `isClosed()` and in-flight publish tracking) was removed during manual editing of `RabbitPublisher.kt` (commit `e64575c`). Currently `./gradlew compileKotlin` fails with `Unresolved reference 'close'`, and some `RabbitPublisherTest` tests (`shouldRejectPublishAfterClose`, `shouldWaitForInFlightPublishesBeforeCloseReturns`) don't match the current code.

Need to decide: restore graceful shutdown with waiting for in-flight publishes, or redesign `RabbitPublisher` lifecycle differently - and then fix `RabbitMQClient.close()` and tests accordingly.

## Open Questions from Author (from TODO comments in code)

### `resources/ConnectionPool.kt`
- Accept `com.rabbitmq.client.ConnectionFactory` as parameter instead of `ConnectionConfig` - let library client configure the factory
- Replace round-robin connection selection with load balancing by number of open channels per connection (channel count per connection should be approximately equal at any time); warn log when approaching `Connection#channelMax` threshold
- Separate public `init()` method, protected by `lifecycleLock`
- Reconnection when RabbitMQ is unavailable at application startup - strategy as separate config (default: 3 attempts every 10 seconds)
- Don't create all connections eagerly - open new connection when channel count on existing connection approaches ~75% of `channelMax`
- `close()` - protect with `lifecycleLock`; wait for all channels spawned by the connection to close
- `nextConnection()` - check not only `closed`, but also `initialized`

### `resources/ManagedConnection.kt`
- Question: perhaps channels don't need to be pooled at all - create channel for publish and close it immediately after use, instead of `ChannelPool`
- No need in ManagedChannel object

### `consumer/ConsumerWorker.kt`
- Question: what to do if `deliveryQueue.poll` throws `InterruptedException` - should `Thread.currentThread().interrupt()` be called?

### `consumer/ConsumerWorkerContainer.kt`
- `nextConnection()`/`createDedicatedChannel()` can throw exception if RabbitMQ is unavailable at startup - need try-catch and several attempts; reconnection strategy as `RabbitConsumer` parameter (default: 3 attempts every 10 seconds)
- `supervise()` - remove `Thread.sleep`, use `ScheduledExecutorService`
- executor in `stop()` - consider replacing with virtual-thread pool executor

### `consumer/RabbitConsumer.kt`
- `start()` should be protected by `lifecycleLock`

## Proposed Next Steps

1. Fix the build - decide how `RabbitPublisher` should close (graceful shutdown / in-flight publishes), bring `RabbitMQClient.close()` and tests into alignment
2. Make decisions on open architectural questions above:
    - `ConnectionFactory` instead of `ConnectionConfig`
    - channel pool vs create-per-publish
    - lazy connection creation as channel count grows + load balancing instead of round-robin
    - unified reconnection strategy for `ConnectionPool` and `ConsumerWorkerContainer`
    - `lifecycleLock` around init/start/close methods
3. Implement accepted decisions
4. Update and expand unit and integration tests for new logic
5. Run `./gradlew test` and `./gradlew integrationTest`