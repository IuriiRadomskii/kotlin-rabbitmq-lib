package org.radomskii.rabbit.model

/**
 * Outcome of handling a single incoming message, telling the consumer worker how to settle
 * the delivery with the broker.
 *
 * The library does not support batching of acknowledgements: every settlement is issued for
 * exactly one delivery tag, so the `multiple` flag on the underlying AMQP methods is always `false`.
 */
sealed class ConsumeResult {

    /**
     * Acknowledge the message. The broker removes it from the queue.
     */
    data object Ack : ConsumeResult()

    /**
     * Negatively acknowledge the message without requeueing. The broker discards the message,
     * routing it to a dead-letter exchange if one is configured for the queue.
     */
    data object Nack : ConsumeResult()

    /**
     * Negatively acknowledge the message and requeue it so the broker redelivers it.
     */
    data object Requeue : ConsumeResult()
}
