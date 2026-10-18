# rabbit-client

Самописная Kotlin-библиотека для работы с RabbitMQ: типизированная публикация и потребление сообщений поверх официального [Java-клиента](https://www.rabbitmq.com/client-libraries/java-api-guide) (`amqp-client`).

## Возможности

- **`ConnectionPool`** — пул соединений с round-robin выбором, повторными попытками подключения и автоматическим добавлением соединений при приближении к лимиту каналов.
- **`RabbitPublisher<T>`** — публикация типизированных сообщений (persistent, с `messageId`, `correlationId`, `priority`, TTL и заголовками).
- **`RabbitConsumer<T>`** — пул воркеров с `prefetch`, супервизором, который перезапускает упавших воркеров, и graceful shutdown.
- **Явное подтверждение** — обработчик возвращает `ConsumeResult.Ack`, `Nack` (без повторной доставки → DLX) или `Requeue`. При исключении сообщение возвращается в очередь.
- **Сериализация** — интерфейс `MessageSerializer<T>`, из коробки есть `JsonMessageSerializer` (Gson).

Управление жизненным циклом лежит на вызывающем коде: пул, паблишеры и консьюмеры создаются и закрываются явно. Топологию (exchange, очереди, binding'и) библиотека не объявляет.

## Требования

- JDK 21 (используются виртуальные потоки)
- Docker — только для интеграционных тестов

## Пример

```kotlin
data class OrderCreated(val id: Int, val amount: Long)

val pool = ConnectionPool.builder()
    .connectionFactory(ConnectionFactory().apply {
        host = "localhost"
        username = "guest"
        password = "guest"
    })
    .connectionPoolConfig(ConnectionPoolConfig(connectionCount = 2))
    .build() // build() сразу инициализирует пул

val serializer = JsonMessageSerializer(OrderCreated::class.java)

// Публикация
val publisher = RabbitPublisher.builder<OrderCreated>()
    .connectionPool(pool)
    .config(PublisherConfig(serializer))
    .build()

publisher.publish("orders", "order.created", OrderCreated(1, 100))

// Потребление
val consumer = RabbitConsumer.builder<OrderCreated>()
    .connectionPool(pool)
    .config(ConsumerConfig(queues = listOf("orders.created"), deserializer = serializer, workerPoolSize = 4))
    .build()

consumer.start { message ->
    println(message.payload)
    ConsumeResult.Ack
}

// Завершение
consumer.stop()
pool.close()
```

> `ConnectionFactory` должна иметь включённый `automaticRecoveryEnabled` (по умолчанию `true`), иначе пул не создастся.

## Конфигурация

| Класс | Основные параметры |
|---|---|
| `ConnectionPoolConfig` | `connectionCount`, `scaleUpThresholdRatio`, `reconnectionConfig` |
| `PublisherConfig<T>` | `serializer`, `closeTimeout` |
| `ConsumerConfig<T>` | `queues`, `deserializer`, `workerPoolSize`, `prefetchCount`, `queueCapacity`, `gracefulShutdownTimeout`, `supervisorPollInterval` |
| `ReconnectionConfig` | `maxAttempts`, `retryInterval` |

## Сборка и тесты

```bash
./gradlew build             # сборка + юнит-тесты
./gradlew test              # только юнит-тесты
./gradlew integrationTest   # интеграционные тесты на Testcontainers (нужен Docker)
```

Интеграционные тесты поднимают RabbitMQ в контейнере и проверяют в том числе устойчивость к сетевым сбоям через Toxiproxy.
