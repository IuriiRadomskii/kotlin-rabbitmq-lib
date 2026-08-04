package org.radomskii.rabbit.model

/**
 * Raw wire-level representation of a message: its serialized bytes together with
 * the content type and encoding needed to interpret them.
 */
data class MessagePayload(
    val bytes: ByteArray,
    val contentType: String,
    val contentEncoding: String = "UTF-8"
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MessagePayload) return false
        return bytes.contentEquals(other.bytes) &&
            contentType == other.contentType &&
            contentEncoding == other.contentEncoding
    }

    override fun hashCode(): Int {
        var result = bytes.contentHashCode()
        result = 31 * result + contentType.hashCode()
        result = 31 * result + contentEncoding.hashCode()
        return result
    }

    override fun toString(): String =
        "MessagePayload(size=${bytes.size}, contentType='$contentType', contentEncoding='$contentEncoding')"
}
