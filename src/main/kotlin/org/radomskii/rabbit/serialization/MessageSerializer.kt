package org.radomskii.rabbit.serialization

import org.radomskii.rabbit.model.MessagePayload

/**
 * Converts between a typed payload and its wire-level [MessagePayload] representation.
 * Publishers use [serialize] to produce bytes to send; consumers use [deserialize] to
 * reconstruct the typed payload from bytes received from RabbitMQ.
 *
 * @param T type of the message payload
 */
interface MessageSerializer<T> {
    /**
     * Serialize a payload into its wire-level representation, including content type and encoding.
     */
    fun serialize(payload: T): MessagePayload

    /**
     * Deserialize a wire-level payload back into its typed representation.
     */
    fun deserialize(payload: MessagePayload): T
}
