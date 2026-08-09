package org.radomskii.rabbit.consumer

import com.rabbitmq.client.LongString

internal fun normalizeHeaders(headers: Map<String, Any>?): Map<String, Any> {
    if (headers.isNullOrEmpty()) return emptyMap()
    return headers.mapValues { (_, value) -> if (value is LongString) value.toString() else value }
}
