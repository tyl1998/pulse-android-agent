package com.pulse.agent.protocol

import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

/** OEM logcat may be unavailable; retain only a bounded private error snapshot. */
class BoundedErrorLog(
    private val writer: ScheduledThreadPoolExecutor = PROCESS_WRITER,
    private val writeIntervalMs: Long = 250,
    private val preparePrivate: (ByteArray, (() -> Unit) -> Boolean) -> Unit,
) : AutoCloseable {
    val sessionId: String = UUID.randomUUID().toString()
    private val lock = Any()
    private val lines = ArrayDeque<String>()
    private var closed = false
    private var pending: ScheduledFuture<*>? = null

    init { synchronized(lock) { schedule(0) } }

    fun append(line: String) {
        val fields = FORMAT.matchEntire(line)?.groupValues ?: return
        if (fields[1] != "unassigned" && !AgentProtocol.validId(fields[1])) return
        if (Phase.entries.none { it.wire == fields[2] } || ErrorCode.entries.none { it.wire == fields[3] }) return
        synchronized(lock) {
            if (closed) return
            if (lines.size == MAX_ENTRIES) lines.removeFirst()
            lines.addLast(line)
            if (pending == null) schedule(writeIntervalMs)
        }
    }

    private fun schedule(delayMs: Long) {
        pending = writer.schedule(::flush, delayMs, TimeUnit.MILLISECONDS)
    }

    private fun flush() {
        val bytes = synchronized(lock) {
            pending = null
            if (closed) return
            ("log_session=$sessionId\n" + lines.joinToString("\n", postfix = if (lines.isEmpty()) "" else "\n"))
                .toByteArray(Charsets.US_ASCII).also { check(it.size <= MAX_BYTES) }
        }
        // Preparation may block on storage, but never holds a caller or retirement lock.
        runCatching {
            preparePrivate(bytes) { atomicPublish ->
                synchronized(lock) {
                    if (closed) false else { atomicPublish(); true }
                }
            }
        }
    }

    override fun close() = synchronized(lock) {
        closed = true
        pending?.cancel(false)
        pending = null
        lines.clear()
    }

    companion object {
        const val MAX_ENTRIES = 64
        const val MAX_BYTES = 16 * 1024
        private val FORMAT = Regex("request_id=([0-9a-f-]{36}|unassigned) phase=([a-z_]+) category=([a-z_]+)")
        // One process-wide writer, one in-flight preparation and one coalesced pending snapshot.
        private val PROCESS_WRITER = ScheduledThreadPoolExecutor(1) { task ->
            Thread(task, "pulse-error-records").apply { isDaemon = true }
        }.apply { removeOnCancelPolicy = true }
    }
}
