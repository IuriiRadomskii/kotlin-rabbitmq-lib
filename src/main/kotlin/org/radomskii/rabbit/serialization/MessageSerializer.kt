package org.radomskii.rabbit.serialization

import org.radomskii.rabbit.model.MessagePayload

interface MessageSerializer<T> {
    fun serialize(payload: T): MessagePayload
    fun deserialize(payload: MessagePayload): T
}
