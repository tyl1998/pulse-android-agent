package com.pulse.agent.execution

import com.pulse.agent.protocol.ActionParams
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.AgentProtocol
import com.pulse.agent.protocol.AgentRequest
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.ObserveParams
import com.pulse.agent.protocol.Phase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class RequestScopeTest {
    @Test fun commitActionEmitsOneReceiptBeforeItsBodyAndExactlyOneCompletedTerminal() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        val platformCalls = AtomicInteger()
        scope.phase = Phase.DISPATCH

        scope.commitAction {
            assertEquals(Phase.COMPLETION, scope.phase)
            assertTrue(scope.wasDispatched)
            assertEquals(1, events.size)
            platformCalls.incrementAndGet()
        }
        scope.dispatchedReceipt()
        scope.dispatchedReceipt()
        assertEquals(1, events.size)

        val receipt = event(events)
        assertTrue(AgentProtocol.validId(receipt.sessionId))
        assertTrue(AgentProtocol.validId(receipt.requestId))
        assertEquals(scope.sessionId, receipt.sessionId)
        assertEquals(scope.request.requestId, receipt.requestId)
        assertEquals("action", receipt.operation)
        assertTrue(receipt.success)
        assertFalse(receipt.terminal)
        assertEquals(mapOf("status" to "dispatched"), receipt.result)
        assertNull(receipt.error)

        val result = mapOf("status" to "completed", "completion_source" to "gesture_callback")
        val terminal = scope.complete(result)
        assertNotNull(terminal)
        assertEquals(terminal, event(events))
        assertTrue(terminal!!.terminal)
        assertTrue(terminal.success)
        assertEquals(result, terminal.result)
        assertEquals(1, platformCalls.get())
        assertTrue(scope.wasDispatched)
        assertTrue(scope.isEnded)
        assertNull(scope.complete(result))
        assertFalse(scope.cancel(ErrorCode.CANCELLED))
        assertTrue(events.isEmpty())
    }

    @Test fun receiptCannotPrecedeThePlatformDispatchMarker() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)

        assertThrows(IllegalStateException::class.java) { scope.dispatchedReceipt() }

        assertFalse(scope.wasDispatched)
        assertTrue(events.isEmpty())
    }

    @Test fun cancellationBeforeDeferredDispatchRejectsWithoutPlatformMutation() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        val platformCalls = AtomicInteger()
        scope.phase = Phase.DISPATCH
        val deferred = Runnable {
            scope.commitAction {
                platformCalls.incrementAndGet()
            }
        }

        assertTrue(scope.cancel(ErrorCode.CANCELLED))
        val failure = assertThrows(AgentException::class.java) { deferred.run() }
        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.DISPATCH), failure.error)
        assertEquals(0, platformCalls.get())
        assertFalse(scope.wasDispatched)
        assertActionFailure(scope.complete(), ErrorCode.CANCELLED, "rejected")
        assertEquals(1, events.size)
    }

    @Test fun connectionIsRecheckedAtActualDeferredDispatch() {
        val alive = AtomicBoolean(true)
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, connectionAlive = alive::get)
        val platformCalls = AtomicInteger()
        scope.phase = Phase.DISPATCH
        scope.checkActive()
        val deferred = Runnable { scope.commitAction { platformCalls.incrementAndGet() } }

        alive.set(false)
        val failure = assertThrows(AgentException::class.java) { deferred.run() }

        assertEquals(AgentError(ErrorCode.DISCONNECTED, Phase.DISPATCH), failure.error)
        assertEquals(0, platformCalls.get())
        assertActionFailure(scope.complete(), ErrorCode.DISCONNECTED, "rejected")
    }

    @Test fun throwingConnectionProbeFailsClosedWithoutExposingItsMessage() {
        val secret = "private-connection-address"
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, connectionAlive = { throw IllegalStateException(secret) })
        val platformCalls = AtomicInteger()

        val failure = assertThrows(AgentException::class.java) {
            scope.dispatch { platformCalls.incrementAndGet() }
        }
        val terminal = scope.complete()!!

        assertEquals(ErrorCode.DISCONNECTED, failure.error.code)
        assertEquals(0, platformCalls.get())
        assertFalse(failure.message.orEmpty().contains(secret))
        assertFalse(terminal.encode().contains(secret))
        assertActionFailure(terminal, ErrorCode.DISCONNECTED, "rejected")
    }

    @Test fun fakeClockSuppressesDeferredDispatchAtTheExactDeadline() {
        val clock = AtomicLong(123)
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, timeoutMs = 100, clock = clock::get)
        val platformCalls = AtomicInteger()
        scope.phase = Phase.DISPATCH
        val deferred = Runnable {
            scope.commitAction {
                platformCalls.incrementAndGet()
            }
        }
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(100) - 1)
        scope.checkActive()

        clock.incrementAndGet()
        val failure = assertThrows(AgentException::class.java) { deferred.run() }

        assertEquals(AgentError(ErrorCode.TIMEOUT, Phase.DISPATCH), failure.error)
        assertEquals(0L, scope.remainingMs)
        assertEquals(0, platformCalls.get())
        assertFalse(scope.wasDispatched)
        assertActionFailure(scope.complete(), ErrorCode.TIMEOUT, "rejected")
    }

    @Test fun committedBodyCrossingDeadlineIsFailedNotRejectedOrCompleted() {
        val clock = AtomicLong()
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, timeoutMs = 100, clock = clock::get)
        scope.phase = Phase.DISPATCH

        scope.commitAction {
            clock.set(TimeUnit.MILLISECONDS.toNanos(100))
        }
        val failure = assertThrows(AgentException::class.java) { scope.dispatchedReceipt() }

        assertEquals(AgentError(ErrorCode.TIMEOUT, Phase.COMPLETION), failure.error)
        assertTrue(scope.wasDispatched)
        assertFalse(event(events).terminal)
        assertTrue(events.isEmpty())
        assertActionFailure(scope.complete(mapOf("status" to "completed")), ErrorCode.TIMEOUT, "failed")
    }

    @Test(timeout = 10_000) fun cancellationWakesAnAwaitingUncompletedCallback() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        val callback = CompletableFuture<Boolean>()
        val started = CountDownLatch(1)
        val executor = Executors.newSingleThreadExecutor()
        scope.phase = Phase.COMPLETION
        try {
            val waiter = executor.submit<AgentError> {
                started.countDown()
                assertThrows(AgentException::class.java) { scope.await(callback) }.error
            }
            assertTrue(started.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertThrows(TimeoutException::class.java) { waiter.get(25, TimeUnit.MILLISECONDS) }

            assertTrue(scope.cancel(ErrorCode.CANCELLED))

            assertEquals(AgentError(ErrorCode.CANCELLED, Phase.COMPLETION), waiter.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertFalse(callback.isDone)
            assertTrue(events.isEmpty())
        } finally {
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 10_000) fun timeoutWakesAnAwaitingCallbackWithoutCompletingIt() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, timeoutMs = 100)
        val callback = CompletableFuture<Boolean>()
        scope.phase = Phase.COMPLETION

        val failure = assertThrows(AgentException::class.java) { scope.await(callback) }

        assertEquals(AgentError(ErrorCode.TIMEOUT, Phase.COMPLETION), failure.error)
        assertFalse(callback.isDone)
        assertActionFailure(scope.complete(), ErrorCode.TIMEOUT, "rejected")
    }

    @Test fun successfulCallbackReturnsItsValueWithoutEmittingCompletionItself() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        scope.phase = Phase.COMPLETION

        assertTrue(scope.await(CompletableFuture.completedFuture(true)))

        assertFalse(scope.isEnded)
        assertTrue(events.isEmpty())
    }

    @Test fun exceptionalCallbackHasSafePlatformFailure() {
        val secret = "typed-password-and-window-title"
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        val callback = CompletableFuture<Boolean>()
        scope.phase = Phase.COMPLETION
        callback.completeExceptionally(IllegalStateException(secret))

        val failure = assertThrows(AgentException::class.java) { scope.await(callback) }

        assertEquals(AgentError(ErrorCode.PLATFORM_FAILED, Phase.COMPLETION), failure.error)
        assertEquals("platform_failed", failure.message)
        assertNull(failure.cause)
        assertFalse(scope.complete(error = failure.error)!!.encode().contains(secret))
    }

    @Test fun typedCallbackFailureRetainsItsSafeCodeAndPhase() {
        val scope = scope(LinkedBlockingQueue())
        val error = AgentError(ErrorCode.TARGET_CHANGED, Phase.DISPATCH, retryable = true)
        val callback = CompletableFuture<Boolean>()
        callback.completeExceptionally(AgentException(error))

        val failure = assertThrows(AgentException::class.java) { scope.await(callback) }

        assertEquals(error, failure.error)
    }

    @Test fun firstCancellationReasonSurvivesLaterDisconnectAndDeadline() {
        val alive = AtomicBoolean(true)
        val clock = AtomicLong()
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, timeoutMs = 100, connectionAlive = alive::get, clock = clock::get)
        scope.phase = Phase.QUEUE

        assertTrue(scope.cancel(ErrorCode.CANCELLED))
        alive.set(false)
        clock.set(TimeUnit.MILLISECONDS.toNanos(101))
        assertFalse(scope.cancel(ErrorCode.DISCONNECTED))
        assertFalse(scope.cancel(ErrorCode.TIMEOUT))

        val failure = assertThrows(AgentException::class.java) { scope.checkActive() }
        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.QUEUE), failure.error)
        assertActionFailure(scope.complete(error = AgentError(ErrorCode.PLATFORM_FAILED, Phase.COMPLETION)), ErrorCode.CANCELLED, "rejected")
        assertEquals(failure.error, assertThrows(AgentException::class.java) { scope.checkActive() }.error)
        assertEquals(1, events.size)
    }

    @Test fun cancellationAfterDispatchCannotBeOverwrittenByLateSuccess() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        scope.phase = Phase.DISPATCH
        scope.commitAction { Unit }
        scope.dispatchedReceipt()
        assertFalse(event(events).terminal)

        assertTrue(scope.cancel(ErrorCode.CANCELLED))
        assertActionFailure(scope.complete(mapOf("status" to "completed")), ErrorCode.CANCELLED, "failed")
        val terminal = event(events)
        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.COMPLETION), terminal.error)
        assertNull(scope.complete(mapOf("status" to "completed")))
        assertEquals(terminal.error, assertThrows(AgentException::class.java) { scope.dispatchedReceipt() }.error)
        assertTrue(events.isEmpty())
    }

    @Test fun rejectedCommitReceiptFailsTheCommitmentWithoutInvokingItsBody() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, acceptEvents = false)
        val platformCalls = AtomicInteger()
        scope.phase = Phase.DISPATCH

        val failure = assertThrows(AgentException::class.java) { scope.commitAction { platformCalls.incrementAndGet() } }

        assertEquals(AgentError(ErrorCode.DISCONNECTED, Phase.COMPLETION), failure.error)
        assertEquals(0, platformCalls.get())
        assertTrue(scope.wasDispatched)
        assertFalse(event(events).terminal)
        assertActionFailure(scope.complete(), ErrorCode.DISCONNECTED, "failed")
        assertTrue(event(events).terminal)
        assertNull(scope.complete())
        assertTrue(events.isEmpty())
    }

    @Test fun rejectedTerminalDeliveryDoesNotRetryOrAllowAnotherTerminal() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, acceptEvents = false)

        val terminal = scope.complete(mapOf("status" to "completed"))

        assertEquals(terminal, event(events))
        assertTrue(scope.isEnded)
        assertNull(scope.complete())
        assertFalse(scope.cancel(ErrorCode.CANCELLED))
        assertTrue(events.isEmpty())
    }

    @Test fun completionRechecksConnectionRatherThanReportingLateSuccess() {
        val alive = AtomicBoolean(true)
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, connectionAlive = alive::get)
        scope.commitAction { Unit }
        alive.set(false)

        val terminal = scope.complete(mapOf("status" to "completed"))

        assertActionFailure(terminal, ErrorCode.DISCONNECTED, "failed")
        assertEquals(Phase.COMPLETION, terminal!!.error!!.phase)
    }

    @Test fun completionRechecksDeadlineRatherThanReportingLateSuccess() {
        val clock = AtomicLong()
        val scope = scope(LinkedBlockingQueue(), timeoutMs = 100, clock = clock::get)
        scope.commitAction { Unit }
        clock.set(TimeUnit.MILLISECONDS.toNanos(100))

        assertActionFailure(scope.complete(mapOf("status" to "completed")), ErrorCode.TIMEOUT, "failed")
    }

    @Test fun stoppedObservationCannotReturnStaleEvidence() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events, operation = "observe")
        scope.phase = Phase.OBSERVE
        scope.cancel(ErrorCode.DISCONNECTED)

        val terminal = scope.complete(mapOf("sources" to mapOf("tree" to "stale-sensitive-tree")))!!

        assertFalse(terminal.success)
        assertTrue(terminal.terminal)
        assertNull(terminal.result)
        assertEquals(AgentError(ErrorCode.DISCONNECTED, Phase.OBSERVE), terminal.error)
        assertFalse(terminal.encode().contains("stale-sensitive-tree"))
    }

    @Test fun stopCleanupRunsOnceAndLateRegistrationSeesExistingCancellation() {
        for (code in listOf(ErrorCode.CANCELLED, ErrorCode.TIMEOUT, ErrorCode.DISCONNECTED, ErrorCode.STOPPED)) {
            val scope = scope(LinkedBlockingQueue())
            val cleanupCalls = AtomicInteger()
            scope.onStopped { cleanupCalls.incrementAndGet() }

            assertTrue(scope.cancel(code))
            assertFalse(scope.cancel(code))
            scope.complete()

            assertEquals(1, cleanupCalls.get())
            scope.onStopped { cleanupCalls.incrementAndGet() }
            assertEquals(2, cleanupCalls.get())
            assertNull(scope.complete())
            assertEquals(2, cleanupCalls.get())
        }
    }

    @Test fun dispatchDeadlineDetectionRunsCleanupWithoutSubmittingThePlatformAction() {
        val clock = AtomicLong()
        val scope = scope(LinkedBlockingQueue(), timeoutMs = 100, clock = clock::get)
        val cleanupCalls = AtomicInteger()
        val platformCalls = AtomicInteger()
        scope.onStopped { cleanupCalls.incrementAndGet() }
        clock.set(TimeUnit.MILLISECONDS.toNanos(100))

        assertThrows(AgentException::class.java) { scope.dispatch { platformCalls.incrementAndGet() } }

        assertEquals(0, platformCalls.get())
        assertEquals(1, cleanupCalls.get())
        scope.complete()
        assertEquals(1, cleanupCalls.get())
    }

    @Test fun throwingCleanupDoesNotMaskCancellationOrExposeItsPayload() {
        val secret = "private-cleanup-payload"
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        scope.onStopped { throw IllegalStateException(secret) }

        assertTrue(scope.cancel(ErrorCode.CANCELLED))
        val terminal = scope.complete()!!

        assertActionFailure(terminal, ErrorCode.CANCELLED, "rejected")
        assertFalse(terminal.encode().contains(secret))
        assertEquals(terminal, event(events))
        assertTrue(events.isEmpty())
    }

    @Test fun stoppedScopeCannotCommitAndCheckActiveRetainsFirstReasonAfterTerminal() {
        for (code in listOf(ErrorCode.CANCELLED, ErrorCode.TIMEOUT, ErrorCode.DISCONNECTED, ErrorCode.STOPPED)) {
            val events = LinkedBlockingQueue<Envelope>()
            val scope = scope(events)
            val platformCalls = AtomicInteger()
            scope.phase = Phase.DISPATCH
            assertTrue(scope.cancel(code))
            assertActionFailure(scope.complete(), code, "rejected")

            val failure = assertThrows(AgentException::class.java) { scope.commitAction { platformCalls.incrementAndGet() } }

            assertEquals(AgentError(code, Phase.DISPATCH), failure.error)
            assertEquals(failure.error, assertThrows(AgentException::class.java) { scope.checkActive() }.error)
            assertEquals(0, platformCalls.get())
            assertFalse(scope.wasDispatched)
            assertEquals(1, events.size)
        }
    }

    @Test(timeout = 10_000) fun cancellationAndTerminalPublicationDoNotWaitForCommittedBlockedBody() {
        assertNonblockingCommittedStop(ErrorCode.CANCELLED)
    }

    @Test(timeout = 10_000) fun deadlineChecksAndTerminalPublicationDoNotWaitForCommittedBlockedBody() {
        assertNonblockingCommittedStop(ErrorCode.TIMEOUT)
    }

    @Test(timeout = 10_000) fun blockedPreflightDoesNotHoldStopLockAndItsLateCommitIsSuppressed() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        val body = BlockedBody()
        val platformCalls = AtomicInteger()
        val executor = Executors.newFixedThreadPool(2)
        scope.phase = Phase.DISPATCH
        try {
            val accessor = executor.submit<Int> {
                scope.dispatch {
                    body.block()
                    scope.commitAction { platformCalls.incrementAndGet() }
                }
            }
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            val stopping = executor.submit<Envelope> {
                assertTrue(scope.cancel(ErrorCode.CANCELLED))
                assertEquals(AgentError(ErrorCode.CANCELLED, Phase.DISPATCH), assertThrows(AgentException::class.java) { scope.checkActive() }.error)
                checkNotNull(scope.complete())
            }

            assertActionFailure(stopping.get(WAIT_MS, TimeUnit.MILLISECONDS), ErrorCode.CANCELLED, "rejected")
            assertEquals(1L, body.release.count)
            assertEquals(1L, body.exited.count)
            assertFalse(accessor.isDone)
            assertEquals(0, platformCalls.get())
            assertFalse(scope.wasDispatched)
            assertTrue(event(events).terminal)

            body.release.countDown()
            val failure = assertThrows(ExecutionException::class.java) { accessor.get(WAIT_MS, TimeUnit.MILLISECONDS) }
            assertEquals(AgentError(ErrorCode.CANCELLED, Phase.DISPATCH), (failure.cause as AgentException).error)
            assertEquals(0, platformCalls.get())
            assertTrue(events.isEmpty())
        } finally {
            body.release.countDown()
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test fun disconnectAfterCommitReceiptButBeforeBodyIsFailedNotSafeRejection() {
        val alive = AtomicBoolean(true)
        val events = LinkedBlockingQueue<Envelope>()
        val platformCalls = AtomicInteger()
        val scope = scope(events, connectionAlive = alive::get, onEvent = {
            if (!it.terminal) alive.set(false)
            true
        })
        scope.phase = Phase.DISPATCH

        val failure = assertThrows(AgentException::class.java) { scope.commitAction { platformCalls.incrementAndGet() } }

        assertEquals(AgentError(ErrorCode.DISCONNECTED, Phase.COMPLETION), failure.error)
        assertTrue(scope.wasDispatched)
        assertEquals(0, platformCalls.get())
        assertFalse(event(events).terminal)
        assertActionFailure(scope.complete(), ErrorCode.DISCONNECTED, "failed")
        assertTrue(event(events).terminal)
        assertTrue(events.isEmpty())
    }

    @Test fun successfulTerminalRunsCleanupOnceAndCannotCommitAnotherPlatformCall() {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = scope(events)
        val cleanupCalls = AtomicInteger()
        val platformCalls = AtomicInteger()
        scope.onStopped { cleanupCalls.incrementAndGet() }
        scope.commitAction { platformCalls.incrementAndGet() }
        assertFalse(event(events).terminal)
        assertTrue(scope.complete(mapOf("status" to "completed"))!!.success)
        assertTrue(event(events).terminal)

        val failure = assertThrows(AgentException::class.java) { scope.commitAction { platformCalls.incrementAndGet() } }

        assertEquals(AgentError(ErrorCode.STOPPED, Phase.COMPLETION), failure.error)
        assertEquals(1, platformCalls.get())
        assertEquals(1, cleanupCalls.get())
        assertNull(scope.complete())
        assertTrue(events.isEmpty())
    }

    private fun assertNonblockingCommittedStop(code: ErrorCode) {
        val events = LinkedBlockingQueue<Envelope>()
        val clock = AtomicLong()
        val scope = scope(events, clock = clock::get)
        val body = BlockedBody()
        val platformCalls = AtomicInteger()
        val executor = Executors.newFixedThreadPool(2)
        scope.phase = Phase.DISPATCH
        try {
            val invocation = executor.submit<Boolean> {
                scope.commitAction {
                    platformCalls.incrementAndGet()
                    body.block()
                }
            }
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertFalse(event(events).terminal)
            val activeCheck = executor.submit<Boolean> {
                scope.checkActive()
                scope.dispatchedReceipt()
                scope.dispatchedReceipt()
                scope.wasDispatched
            }
            assertTrue(activeCheck.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertTrue(events.isEmpty())
            assertEquals(Phase.COMPLETION, scope.phase)
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            if (code == ErrorCode.TIMEOUT) clock.set(TimeUnit.MILLISECONDS.toNanos(scope.request.timeoutMs))
            val stopping = executor.submit<Envelope> {
                if (code == ErrorCode.CANCELLED) assertTrue(scope.cancel(code))
                assertEquals(AgentError(code, Phase.COMPLETION), assertThrows(AgentException::class.java) { scope.checkActive() }.error)
                checkNotNull(scope.complete(mapOf("status" to "completed")))
            }

            val terminal = stopping.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertActionFailure(terminal, code, "failed")
            assertEquals(AgentError(code, Phase.COMPLETION), terminal.error)
            assertEquals(1L, body.release.count)
            assertEquals(1L, body.exited.count)
            assertFalse(invocation.isDone)
            assertEquals(terminal, event(events))
            assertEquals(terminal.error, assertThrows(AgentException::class.java) { scope.checkActive() }.error)

            body.release.countDown()
            assertTrue(invocation.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertEquals(1, platformCalls.get())
            assertNull(scope.complete(mapOf("status" to "completed")))
            assertTrue(events.isEmpty())
        } finally {
            body.release.countDown()
            executor.shutdownNow()
            assertTrue(executor.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    private class BlockedBody {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val exited = CountDownLatch(1)
        val interrupted = CountDownLatch(1)
        private val thread = AtomicReference<Thread?>()

        fun interrupt() { checkNotNull(thread.get()).interrupt() }

        fun block(): Boolean {
            thread.set(Thread.currentThread())
            entered.countDown()
            val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS * 3)
            var wasInterrupted = false
            try {
                while (true) {
                    try {
                        check(release.await((deadline - System.nanoTime()).coerceAtLeast(1), TimeUnit.NANOSECONDS))
                        return true
                    } catch (_: InterruptedException) {
                        wasInterrupted = true
                        interrupted.countDown()
                        check(System.nanoTime() < deadline)
                    }
                }
            } finally {
                if (wasInterrupted) Thread.currentThread().interrupt()
                exited.countDown()
            }
        }
    }

    private fun scope(
        events: BlockingQueue<Envelope>,
        timeoutMs: Long = 10_000,
        operation: String = "action",
        connectionAlive: () -> Boolean = { true },
        clock: () -> Long = System::nanoTime,
        acceptEvents: Boolean = true,
        onEvent: (Envelope) -> Boolean = { true },
    ): RequestScope = RequestScope(
        sessionId = UUID.randomUUID().toString(),
        request = AgentRequest(
            UUID.randomUUID().toString(), operation, timeoutMs,
            action = if (operation == "action") ActionParams("tap", "com.pulse.fixture", x = 10, y = 20) else null,
            observe = if (operation == "observe") ObserveParams() else null,
        ),
        connectionAlive = connectionAlive,
        events = { events.offer(it) && acceptEvents && onEvent(it) },
        clock = clock,
    )

    private fun event(events: BlockingQueue<Envelope>): Envelope =
        checkNotNull(events.poll(WAIT_MS, TimeUnit.MILLISECONDS)) { "No lifecycle envelope within bounded wait" }

    private fun assertActionFailure(envelope: Envelope?, code: ErrorCode, status: String) {
        assertNotNull(envelope)
        assertTrue(envelope!!.terminal)
        assertFalse(envelope.success)
        assertEquals(code, envelope.error!!.code)
        assertEquals(mapOf("status" to status), envelope.result)
    }

    companion object {
        private const val WAIT_MS = 2_000L
    }
}
