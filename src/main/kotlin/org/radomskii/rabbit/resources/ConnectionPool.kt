package org.radomskii.rabbit.resources

interface ConnectionPool {

    fun nextConnection(): ManagedConnection

    fun close()

}