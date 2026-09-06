# Load Testing Plan

This document captures findings from reviewing the library's architecture and proposes a
load-testing plan in two phases: local machine, then cloud.

## Why these tests

The library wraps `amqp-client` with a connection pool, a publisher, and a consumer worker
pool. Reading the implementation surfaces a few specific risk areas that generic load
testing wouldn't necessarily target. The plan below is built around exercising those areas
directly rather than just measuring generic throughput.

### Risk areas found in the code

1. **Publisher opens a channel per call.**
   `RabbitPublisher.publish()` (`src/main/kotlin/org/radomskii/rabbit/publisher/RabbitPublisher.kt`)
   creates a new `Channel` and closes it on every single `publish()` invocation. Under high
   concurrency this is channel churn on top of whatever connections are in the pool, and is a
   likely throughput ceiling — especially sensitive to client-broker RTT.

2. **Connection pool auto-scales based on channel saturation.**
   `ConnectionPool.triggerScaleUpIfNearCapacity()`
   (`src/main/kotlin/org/radomskii/rabbit/resources/ConnectionPool.kt`) opens a new connection
   once an existing connection's channel count reaches `channelMax * scaleUpThresholdRatio`
   (default 0.75). Scale-up is guarded by a single `AtomicBoolean scalingInProgress` and polled
   on a fixed interval (`reconnectionConfig.retryInterval`), not triggered synchronously on
   demand. Under a sudden burst this could lag behind actual channel pressure.

3. **Consumers are single-threaded per worker with a bounded internal queue.**
   `ConsumerWorker` (`src/main/kotlin/org/radomskii/rabbit/consumer/ConsumerWorker.kt`) runs one
   platform thread per worker, pulling from a `LinkedBlockingQueue(queueCapacity)`. The AMQP
   `deliverCallback` blocks on `put()` when that queue is full, which pushes back on the
   RabbitMQ Java client's own delivery dispatch thread. Effective throughput is a function of
   `workerPoolSize`, `prefetchCount`, `queueCapacity`, and handler processing time — this
   interaction needs to be measured directly, not assumed.

4. **Recovery paths run on their own schedules.**
   Connection retries go through a `DelayQueue`-driven retry worker
   (`ConnectionPool.runRetryLoop`), and dead consumer workers are detected and restarted by a
   polling supervisor (`ConsumerWorkerContainer.checkWorkers`, default interval 1s). Both need
   to be exercised under load, not just at idle, to see real recovery time.

5. **No built-in metrics yet.**
   `micrometer-core` is a declared dependency but is not used anywhere in `src/main` — the
   library currently exposes no internal metrics. Load tests need to instrument externally
   (wrap `publish()`/`handle()` calls, and pull broker-side metrics from the RabbitMQ
   management API / Prometheus plugin) rather than relying on the library to self-report.

## Phase 1 — Local machine

**Infrastructure**: docker-compose running RabbitMQ (with the `rabbitmq_prometheus` plugin
for broker-side metrics) plus Toxiproxy (the project's `integrationTest` suite already uses
Toxiproxy via Testcontainers — reuse that setup) and Prometheus/Grafana for collection.

**Harness**: a separate Gradle source set, e.g. `loadTest` (kept apart from
`integrationTest`), containing a small console app built on this library. Configurable via
env/args: broker host, target rate, duration, payload size, `workerPoolSize`,
`prefetchCount`, `queueCapacity`, `connectionCount`. Use HdrHistogram for latency recording.

**Scenarios**:

- **A. Publisher throughput** — N threads calling `publish()` continuously, payload sizes
  100B / 1KB / 10KB, `connectionCount=1` vs `N`. Measure msgs/sec, p50/p95/p99 `publish()`
  latency, and channel-creation rate.
- **B. Consumer throughput** — pre-fill a queue with several million messages, sweep
  `workerPoolSize` × `prefetchCount` × `queueCapacity`, measure msgs/sec and end-to-end
  latency (publish timestamp → handler invocation timestamp). Repeat with an artificially
  slow handler to observe backpressure and queue depth growth on the broker side.
- **C. Soak** — publish and consume concurrently at a sustained target rate for 1–4+ hours;
  watch heap/GC/thread count (JFR), open socket/channel counts, and confirm the connection
  pool's auto-scale doesn't grow beyond what's actually needed.
- **D. Resilience under load** — repeat scenarios A/B/C while injecting Toxiproxy faults
  (latency, connection reset, bandwidth cap) and broker restarts; measure recovery time and
  verify ack/nack/requeue correctness (no silent message loss, no unbounded duplicate
  redelivery).
- **E. Burst/spike** — a sudden 10x jump in publish rate for a few seconds at
  `connectionCount=1`, checking whether pool scale-up keeps up without triggering a
  connection-creation storm.

## Phase 2 — Cloud

**Goal**: real client-broker network latency, horizontally scaled load generation, and
cluster-level broker behavior that a single local machine can't reproduce.

- **Broker**: a managed RabbitMQ (CloudAMQP or Amazon MQ for RabbitMQ) for a quick baseline,
  or a self-managed cluster via the RabbitMQ Cluster Operator on EKS/GKE if quorum-queue and
  node-failover scenarios matter.
- **Load generator**: the same Phase 1 harness, containerized, run as a Kubernetes Job with
  multiple replicas (multiple independent library clients, not one process fanning out
  internally), with metrics scraped into Prometheus (or pushed via pushgateway).
- **Chaos testing**: Chaos Mesh / Litmus (in k8s) or AWS FIS to simulate network partitions,
  kill a broker node/pod, or simulate an AZ failure — exercising the reconnection logic
  against real instability rather than only Toxiproxy.
- **Latency profiles**: compare same-AZ vs cross-AZ/cross-region client-broker placement.
  The publisher's per-call channel open/close makes it especially sensitive to RTT — this
  should be measured explicitly against the local baseline.
- **Extended soak**: 12–24h+ run with alerting on memory/thread/connection growth.
- **Ceiling search**: incrementally increase `connectionCount` / `workerPoolSize` / number of
  client instances to find the point where the broker itself becomes the bottleneck rather
  than the client library — the two need to be distinguished explicitly in results.

## Suggested repo layout

- A dedicated `loadTest` Gradle source set with a `LoadTestApp` main class, configured via
  env/args, separate from `integrationTest`.
- A `docker-compose.yml` for the local broker + Prometheus/Grafana stack (Phase 1).
- A Helm values file / k8s manifests for the cloud load-generator Job and chaos experiments
  (Phase 2).
