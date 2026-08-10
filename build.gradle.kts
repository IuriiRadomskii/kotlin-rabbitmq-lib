plugins {
    kotlin("jvm") version "2.2.20"
}

group = "org.radomskii"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.rabbitmq:amqp-client:5.34.0")
    implementation("com.google.code.gson:gson:2.11.0")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.9")

    // Metrics
    implementation("io.micrometer:micrometer-core:1.12.0")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.awaitility:awaitility-kotlin:3.1.2")

    // Mocking
    testImplementation("org.mockito:mockito-core:5.14.2")
    testImplementation(platform("org.junit:junit-bom:5.14.2"))
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")

    // TestContainers
    testImplementation("org.testcontainers:testcontainers:1.19.3")
    testImplementation("org.testcontainers:rabbitmq:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")

    // Logging for tests
    testImplementation("ch.qos.logback:logback-classic:1.4.14")

    // Assertions
    testImplementation("org.assertj:assertj-core:3.24.2")
}

tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
    jvmArgs("-XX:+EnableDynamicAgentLoading")
}

// Testcontainers-based tests, tagged "integration", require a running Docker daemon.
// Run explicitly via `./gradlew integrationTest`; kept out of the default `test` task.
tasks.register<Test>("integrationTest") {
    useJUnitPlatform {
        includeTags("integration")
    }
    jvmArgs("-XX:+EnableDynamicAgentLoading")
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    shouldRunAfter(tasks.test)
}
kotlin {
    jvmToolchain(21)
}