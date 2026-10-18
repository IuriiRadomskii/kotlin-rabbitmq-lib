package org.radomskii.rabbit.consumer

import org.radomskii.rabbit.model.ConsumeResult
import org.radomskii.rabbit.model.IncomingMessage

fun interface MessageHandler<T> {
    fun handle(message: IncomingMessage<T>): ConsumeResult
}
