package com.pulse.agent.protocol

import org.junit.Assert.*
import org.junit.Test
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicBoolean

class BoundedErrorLogTest {
    @Test fun storesOnlyClosedStructuredErrors() {
        val writer = writer()
        try {
            val snapshot = AtomicReference(ByteArray(0))
            val log = BoundedErrorLog(writer, 0) { bytes, publish -> publish { snapshot.set(bytes) } }
            val id = UUID.randomUUID().toString()
            log.append(SafeLog.error(id, AgentError(ErrorCode.CANCELLED, Phase.COMPLETION)))
            settle(writer)
            assertEquals("log_session=${log.sessionId}\nrequest_id=$id phase=completion category=cancelled\n",
                snapshot.get().toString(Charsets.US_ASCII))
            log.close()
        } finally { writer.shutdownNow() }
    }

    @Test fun rejectsSecretsUnknownFieldsAndInjectedLines() {
        val writer = writer()
        try {
            val snapshot = AtomicReference(ByteArray(0))
            val log = BoundedErrorLog(writer, 0) { bytes, publish -> publish { snapshot.set(bytes) } }
            val id = UUID.randomUUID().toString()
            listOf("secret-input", "a".repeat(64), "request_id=$id phase=completion category=secret",
                "request_id=$id phase=secret category=cancelled", "request_id=$id phase=completion category=cancelled\nsecret",
                "request_id=${"a".repeat(36)} phase=completion category=cancelled").forEach(log::append)
            settle(writer)
            assertEquals("log_session=${log.sessionId}\n", snapshot.get().toString(Charsets.US_ASCII))
            log.close()
        } finally { writer.shutdownNow() }
    }

    @Test fun keepsOnlyLatestBoundedEntries() {
        val writer = writer()
        try {
            val bytes = AtomicReference(ByteArray(0))
            val log = BoundedErrorLog(writer, 0) { value, publish -> publish { bytes.set(value) } }
            val first = UUID.randomUUID().toString()
            log.append(SafeLog.error(first, AgentError(ErrorCode.TIMEOUT, Phase.QUEUE)))
            repeat(128) { log.append(SafeLog.error(UUID.randomUUID().toString(), AgentError(ErrorCode.UNAUTHORIZED, Phase.AUTH))) }
            settle(writer)
            val text = bytes.get().toString(Charsets.US_ASCII)
            assertFalse(text.contains(first))
            assertEquals(BoundedErrorLog.MAX_ENTRIES + 1, text.lines().count { it.isNotEmpty() })
            assertTrue(bytes.get().size <= BoundedErrorLog.MAX_BYTES)
            log.close()
        } finally { writer.shutdownNow() }
    }

    @Test fun retiredLoggerCannotOverwriteNewSession() {
        val writer = writer()
        try {
            val snapshot = AtomicReference("")
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val previousPublished = AtomicBoolean(false)
            val previous = BoundedErrorLog(writer, 0) { bytes, publish ->
                entered.countDown()
                check(release.await(2, TimeUnit.SECONDS))
                publish { previousPublished.set(true); snapshot.set(bytes.toString(Charsets.US_ASCII)) }
            }
            assertTrue(entered.await(1, TimeUnit.SECONDS))
            previous.close()
            val current = BoundedErrorLog(writer, 0) { bytes, publish -> publish { snapshot.set(bytes.toString(Charsets.US_ASCII)) } }
            current.append(SafeLog.error(null, AgentError(ErrorCode.UNAUTHORIZED, Phase.AUTH)))
            release.countDown()
            settle(writer)
            assertTrue(snapshot.get().startsWith("log_session=${current.sessionId}\n"))
            assertFalse(snapshot.get().contains(previous.sessionId))
            assertFalse(previousPublished.get())
            val expected = snapshot.get()
            previous.append(SafeLog.error(null, AgentError(ErrorCode.STOPPED, Phase.LIFECYCLE)))
            settle(writer)
            assertEquals(expected, snapshot.get())
            current.close()
        } finally { writer.shutdownNow() }
    }

    @Test fun failedWriteDoesNotBreakCancellationOrLogItsCause() {
        val writer = writer()
        try {
            val log = BoundedErrorLog(writer, 0) { _, _ -> throw IllegalStateException("secret-payload") }
            log.append(SafeLog.error(null, AgentError(ErrorCode.CANCELLED, Phase.COMPLETION)))
            settle(writer)
            log.close()
        } finally { writer.shutdownNow() }
    }

    @Test fun appendAndCloseNeverWaitForBlockedStoragePreparation() {
        val writer = writer()
        val caller = Executors.newSingleThreadExecutor()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val published = AtomicBoolean(false)
        try {
            val log = BoundedErrorLog(writer, 0) { _, publish ->
                entered.countDown()
                check(release.await(2, TimeUnit.SECONDS))
                publish { published.set(true) }
            }
            assertTrue(entered.await(1, TimeUnit.SECONDS))
            caller.submit {
                repeat(100) { log.append(SafeLog.error(null, AgentError(ErrorCode.TIMEOUT, Phase.COMPLETION))) }
                assertTrue(writer.queue.size <= 1)
                log.close()
            }.get(250, TimeUnit.MILLISECONDS)
            assertTrue(writer.queue.isEmpty())
            release.countDown()
            settle(writer)
            assertFalse(published.get())
        } finally { release.countDown(); caller.shutdownNow(); writer.shutdownNow() }
    }

    private fun writer() = ScheduledThreadPoolExecutor(1).apply { removeOnCancelPolicy = true }
    private fun settle(writer: ScheduledThreadPoolExecutor) { writer.submit {}.get(2, TimeUnit.SECONDS) }
}
