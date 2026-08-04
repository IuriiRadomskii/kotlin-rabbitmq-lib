package org.radomskii.rabbit.model

/**
 * Outcome of handling a single [IncomingMessage], telling the consumer worker how to
 * settle the delivery with the broker.
 */
sealed class ConsumeResult {
    /** Acknowledge the message - it was processed successfully. */
    data object Ack : ConsumeResult()

    /** Negatively acknowledge the message. */
    data class Nack(val requeue: Boolean = false) : ConsumeResult()

    /** Reject the message. */
    data class Reject(val requeue: Boolean = false) : ConsumeResult()
}
