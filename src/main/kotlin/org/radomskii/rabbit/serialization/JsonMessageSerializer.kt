package org.radomskii.rabbit.serialization

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.radomskii.rabbit.model.MessagePayload
import java.lang.reflect.Type

class JsonMessageSerializer<T>(
    private val type: Type,
    private val gson: Gson = Gson()
) : MessageSerializer<T> {

    constructor(clazz: Class<T>, gson: Gson = Gson()) : this(clazz as Type, gson)

    override fun serialize(payload: T): MessagePayload {
        val json = gson.toJson(payload, type)
        return MessagePayload(json.toByteArray(Charsets.UTF_8), CONTENT_TYPE, CONTENT_ENCODING)
    }

    override fun deserialize(payload: MessagePayload): T {
        val json = String(payload.bytes, charset(payload.contentEncoding))
        return gson.fromJson(json, type)
    }

    companion object {
        private const val CONTENT_TYPE = "application/json"
        private const val CONTENT_ENCODING = "UTF-8"

        inline fun <reified T> create(gson: Gson = Gson()): JsonMessageSerializer<T> =
            JsonMessageSerializer(object : TypeToken<T>() {}.type, gson)
    }
}
