package org.radomskii.rabbit.publisher

import org.radomskii.rabbit.RabbitClientException

class MessageReturnedException(
    val replyCode: Int,
    val replyText: String,
    val exchange: String,
    val routingKey: String
) : RabbitClientException(
    "Message returned by broker: $replyText (code=$replyCode, exchange='$exchange', routingKey='$routingKey')"
)
