package org.radomskii.rabbit.model

sealed class ConsumeResult {
    data object Ack : ConsumeResult()
    data class Nack(val requeue: Boolean = false) : ConsumeResult()
    data class Reject(val requeue: Boolean = false) : ConsumeResult()
}
