package org.radomskii.rabbit.resources

import com.rabbitmq.client.Channel

interface ManagedConnection {

    fun id(): String

    fun createChannel(): Channel

    fun close()

}