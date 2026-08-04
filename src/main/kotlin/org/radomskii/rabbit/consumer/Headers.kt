package org.radomskii.rabbit.consumer

import com.rabbitmq.client.LongString

/**
 * Normalizes AMQP headers as received from the broker: string values arrive wrapped as
 * [LongString] rather than [String], so they are unwrapped for consistency with what a
 * publisher puts in [org.radomskii.rabbit.model.MessageMetadata.headers].
 */
internal fun normalizeHeaders(headers: Map<String, Any>?): Map<String, Any> {
    if (headers.isNullOrEmpty()) return emptyMap()
    return headers.mapValues { (_, value) -> if (value is LongString) value.toString() else value }
}
