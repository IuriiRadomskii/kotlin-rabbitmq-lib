package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.model.IncomingMessage

/**
 * Processes a single message received from RabbitMQ and decides how it should be settled.
 * Invoked synchronously on a consumer worker's dedicated thread.
 *
 * @param T type of the message payload
 */
fun interface MessageHandler<T> {
    fun handle(message: IncomingMessage<T>): ConsumeResult
}
