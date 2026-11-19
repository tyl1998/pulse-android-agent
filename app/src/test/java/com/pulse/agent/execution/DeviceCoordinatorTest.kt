package com.pulse.agent.execution

import com.pulse.agent.protocol.ActionParams
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.AgentRequest
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.ObserveParams
import com.pulse.agent.protocol.Phase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertSame
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

class DeviceCoordinatorTest {
    @Test(timeout = 10_000) fun actionIsDispatchedNonterminalUntilTruePlatformCallback() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val request = recording()
            coordinator.submit(request.scope)
            val action = take(backend.actions)
            val receipt = request.receipt()

            assertEquals(request.scope.sessionId, action.scope.sessionId)
            assertEquals(request.scope.request.action, action.params)
            assertTrue(action.scope.wasDispatched)
            assertEquals(request.scope.request.requestId, receipt.requestId)
            assertEquals(request.scope.sessionId, receipt.sessionId)
            assertEquals("action", receipt.operation)
            assertTrue(request.events.isEmpty())
            assertFalse(request.scope.isEnded)

            assertTrue(action.callback(true))
            val terminal = request.terminal()

            assertTrue(terminal.success)
            assertEquals(mapOf("status" to "completed", "completion_source" to "gesture_callback"), terminal.result)
            assertNull(terminal.error)
            assertTrue(request.scope.isEnded)
            assertFalse(coordinator.cancel(request.scope.sessionId, request.scope.request.requestId))
            assertTrue(request.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun falseGestureCallbackIsFailedNotSuccessfulAcknowledgement() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val request = recording()
            coordinator.submit(request.scope)
            val action = take(backend.actions)
            request.receipt()

            action.callback(false)

            assertFailure(request.terminal(), ErrorCode.GESTURE_CANCELLED, Phase.COMPLETION, "failed")
            assertTrue(request.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun exceptionalPlatformCallbackIsSafeFailureAndNeverLogsPayload() {
        val secret = "password-text-window-title-package-private"
        val logs = LinkedBlockingQueue<String>()
        val backend = FakeBackend()
        DeviceCoordinator(backend, logError = { logs.offer(it) }).use { coordinator ->
            val request = recording(action = ActionParams("set_text", "com.private.account", text = secret))
            coordinator.submit(request.scope)
            val action = take(backend.actions)
            request.receipt()

            action.completion.completeExceptionally(IllegalStateException(secret))
            val terminal = request.terminal()
            val log = take(logs)

            assertFailure(terminal, ErrorCode.PLATFORM_FAILED, Phase.COMPLETION, "failed")
            assertEquals("request_id=${request.scope.request.requestId} phase=completion category=platform_failed", log)
            assertFalse(log.contains(secret))
            assertFalse(log.contains("com.private.account"))
            assertFalse(terminal.encode().contains(secret))
            assertFalse(terminal.encode().contains("com.private.account"))
            assertTrue(logs.isEmpty())
            assertTrue(request.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun dispatchExceptionProducesSafeRejectedFailureAndBodyFreeLog() {
        val secret = "private-node-text-and-credential"
        val logs = LinkedBlockingQueue<String>()
        val backend = FakeBackend().apply { onDispatch = { _, _ -> throw IllegalStateException(secret) } }
        DeviceCoordinator(backend, logError = { logs.offer(it) }).use { coordinator ->
            val request = recording(action = ActionParams("set_text", "com.private.account", text = secret))
            coordinator.submit(request.scope)

            val terminal = request.terminal()
            val log = take(logs)

            assertFailure(terminal, ErrorCode.INTERNAL_ERROR, Phase.DISPATCH, "rejected")
            assertEquals("request_id=${request.scope.request.requestId} phase=dispatch category=internal_error", log)
            assertFalse(log.contains(secret))
            assertFalse(log.contains("com.private.account"))
            assertFalse(terminal.encode().contains(secret))
            assertTrue(backend.actions.isEmpty())
            assertTrue(request.events.isEmpty())
            assertTrue(logs.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun backendDispatchRejectionHasNoDispatchedReceipt() {
        val backend = FakeBackend().apply { onDispatch = { _, _ -> null } }
        DeviceCoordinator(backend).use { coordinator ->
            val request = recording()
            coordinator.submit(request.scope)

            assertFailure(request.terminal(), ErrorCode.DISPATCH_REJECTED, Phase.DISPATCH, "rejected")
            assertFalse(request.scope.wasDispatched)
            assertTrue(backend.actions.isEmpty())
            assertTrue(request.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun oneWorkerSerializesActionsAndObservationsThroughCompletion() {
        val observationCompletion = CompletableFuture<Observation>()
        val backend = FakeBackend().apply { onObserve = { scope, _ -> scope.await(observationCompletion) } }
        DeviceCoordinator(backend).use { coordinator ->
            val first = recording()
            val observation = recording(operation = "observe")
            val last = recording()
            coordinator.submit(first.scope)
            val firstAction = take(backend.actions)
            first.receipt()
            coordinator.submit(observation.scope)
            coordinator.submit(last.scope)

            assertNull(backend.observations.poll(25, TimeUnit.MILLISECONDS))
            assertNull(backend.actions.poll(25, TimeUnit.MILLISECONDS))
            assertTrue(observation.events.isEmpty())
            assertTrue(last.events.isEmpty())

            firstAction.callback(true)
            assertTrue(first.terminal().success)
            val observationCall = take(backend.observations)
            assertEquals(observation.scope.sessionId, observationCall.scope.sessionId)
            assertEquals(observation.scope.request.observe, observationCall.params)
            assertNull(backend.actions.poll(25, TimeUnit.MILLISECONDS))

            observationCompletion.complete(evidence())
            assertTrue(observation.terminal().success)
            val lastAction = take(backend.actions)
            assertEquals(last.scope.sessionId, lastAction.scope.sessionId)
            last.receipt()
            lastAction.callback(true)
            assertTrue(last.terminal().success)
            assertTrue(backend.actions.isEmpty())
            assertTrue(backend.observations.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun queuedCancellationPreventsBackendExecution() {
        assertQueuedStop(ErrorCode.CANCELLED)
    }

    @Test(timeout = 10_000) fun queuedTimeoutPreventsBackendExecution() {
        assertQueuedStop(ErrorCode.TIMEOUT)
    }

    @Test(timeout = 10_000) fun queuedDisconnectPreventsBackendExecution() {
        assertQueuedStop(ErrorCode.DISCONNECTED)
    }

    @Test(timeout = 10_000) fun cancellationBeforeDeferredMainDispatchPreventsLateAction() {
        assertDeferredStop(ErrorCode.CANCELLED)
    }

    @Test(timeout = 10_000) fun expiredDeferredMainRunnableIsSuppressedByActualDispatchGuard() {
        assertDeferredStop(ErrorCode.TIMEOUT)
    }

    @Test(timeout = 10_000) fun disconnectBeforeDeferredMainDispatchPreventsLateAction() {
        assertDeferredStop(ErrorCode.DISCONNECTED)
    }

    @Test(timeout = 10_000) fun cancellationWakesActiveCompletionAndLetsNextJobRun() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val active = recording()
            val queued = recording()
            coordinator.submit(active.scope)
            val activeAction = take(backend.actions)
            active.receipt()
            coordinator.submit(queued.scope)

            assertTrue(coordinator.cancel(active.scope.sessionId, active.scope.request.requestId))
            assertFailure(active.terminal(), ErrorCode.CANCELLED, Phase.COMPLETION, "failed")
            val queuedAction = take(backend.actions)

            assertFalse(activeAction.completion.isDone)
            assertEquals(queued.scope.sessionId, queuedAction.scope.sessionId)
            queued.receipt()
            activeAction.callback(true)
            assertTrue(active.events.isEmpty())
            assertTrue(queued.events.isEmpty())
            assertFalse(queued.scope.isEnded)
            queuedAction.callback(true)
            assertTrue(queued.terminal().success)
        }
    }

    @Test(timeout = 10_000) fun activeCompletionTimeoutIsFailedAndUnblocksTheWorker() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val active = recording(timeoutMs = 1_000)
            val queued = recording()
            coordinator.submit(active.scope)
            val activeAction = take(backend.actions)
            active.receipt()
            coordinator.submit(queued.scope)

            assertFailure(active.terminal(), ErrorCode.TIMEOUT, Phase.COMPLETION, "failed")
            val queuedAction = take(backend.actions)

            assertFalse(activeAction.completion.isDone)
            assertEquals(queued.scope.sessionId, queuedAction.scope.sessionId)
            queued.receipt()
            activeAction.callback(true)
            assertTrue(active.events.isEmpty())
            queuedAction.callback(true)
            assertTrue(queued.terminal().success)
        }
    }

    @Test(timeout = 10_000) fun reconnectWithReusedRequestIdIsIsolatedFromOldCancellationAndCompletion() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val reusedRequestId = UUID.randomUUID().toString()
            val old = recording(requestId = reusedRequestId)
            coordinator.submit(old.scope)
            val oldAction = take(backend.actions)
            old.receipt()
            coordinator.disconnect(old.scope.sessionId)
            assertFailure(old.terminal(), ErrorCode.DISCONNECTED, Phase.COMPLETION, "failed")

            val reconnected = recording(requestId = reusedRequestId)
            assertFalse(old.scope.sessionId == reconnected.scope.sessionId)
            coordinator.submit(reconnected.scope)
            val newAction = take(backend.actions)
            reconnected.receipt()

            assertFalse(coordinator.cancel(old.scope.sessionId, reusedRequestId))
            coordinator.disconnect(old.scope.sessionId)
            assertTrue(oldAction.callback(true))

            assertFalse(newAction.completion.isDone)
            assertFalse(reconnected.scope.isEnded)
            assertTrue(old.events.isEmpty())
            assertTrue(reconnected.events.isEmpty())
            assertTrue(newAction.callback(true))
            val terminal = reconnected.terminal()
            assertTrue(terminal.success)
            assertEquals(reconnected.scope.sessionId, terminal.sessionId)
            assertEquals(reusedRequestId, terminal.requestId)
            assertTrue(old.events.isEmpty())
            assertTrue(reconnected.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun mismatchedRequestIdCannotCancelTheActiveSession() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val request = recording()
            coordinator.submit(request.scope)
            val action = take(backend.actions)
            request.receipt()

            assertFalse(coordinator.cancel(request.scope.sessionId, UUID.randomUUID().toString()))
            assertFalse(coordinator.cancel(UUID.randomUUID().toString(), request.scope.request.requestId))
            assertFalse(request.scope.isEnded)
            assertTrue(request.events.isEmpty())

            action.callback(true)
            assertTrue(request.terminal().success)
        }
    }

    @Test(timeout = 10_000) fun queueBoundRejectsOverflowButPreservesAlreadyAcceptedJobs() {
        val backend = FakeBackend()
        DeviceCoordinator(backend, queueSize = 1).use { coordinator ->
            val active = recording()
            val queued = recording()
            val overflow = recording()
            coordinator.submit(active.scope)
            val activeAction = take(backend.actions)
            active.receipt()
            coordinator.submit(queued.scope)
            coordinator.submit(overflow.scope)

            assertFailure(overflow.terminal(), ErrorCode.BUSY, Phase.QUEUE, "rejected")
            assertFalse(overflow.scope.wasDispatched)
            assertTrue(backend.actions.isEmpty())
            assertTrue(queued.events.isEmpty())
            assertFalse(coordinator.cancel(overflow.scope.sessionId, overflow.scope.request.requestId))

            activeAction.callback(true)
            assertTrue(active.terminal().success)
            val queuedAction = take(backend.actions)
            assertEquals(queued.scope.sessionId, queuedAction.scope.sessionId)
            queued.receipt()
            queuedAction.callback(true)
            assertTrue(queued.terminal().success)
            assertTrue(backend.actions.isEmpty())
            assertTrue(overflow.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun cancellingAQueuedJobImmediatelyReclaimsItsBoundedQueueSlot() {
        val backend = FakeBackend()
        DeviceCoordinator(backend, queueSize = 1).use { coordinator ->
            val active = recording()
            val cancelled = recording()
            val replacement = recording()
            coordinator.submit(active.scope)
            val activeAction = take(backend.actions)
            active.receipt()
            coordinator.submit(cancelled.scope)

            assertTrue(coordinator.cancel(cancelled.scope.sessionId, cancelled.scope.request.requestId))
            assertFailure(cancelled.terminal(), ErrorCode.CANCELLED, Phase.QUEUE, "rejected")
            coordinator.submit(replacement.scope)

            assertTrue(replacement.events.isEmpty())
            activeAction.callback(true)
            assertTrue(active.terminal().success)
            val replacementAction = take(backend.actions)
            assertEquals(replacement.scope.sessionId, replacementAction.scope.sessionId)
            replacement.receipt()
            replacementAction.callback(true)
            assertTrue(replacement.terminal().success)
            assertTrue(backend.actions.isEmpty())
            assertTrue(cancelled.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun aSessionCannotHaveTwoOutstandingDeviceJobs() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val active = recording()
            coordinator.submit(active.scope)
            val action = take(backend.actions)
            active.receipt()
            val duplicate = recording(sessionId = active.scope.sessionId)
            coordinator.submit(duplicate.scope)

            assertFailure(duplicate.terminal(), ErrorCode.BUSY, Phase.QUEUE, "rejected")
            assertFalse(coordinator.cancel(active.scope.sessionId, duplicate.scope.request.requestId))
            assertTrue(active.events.isEmpty())
            assertTrue(backend.actions.isEmpty())

            action.callback(true)
            assertTrue(active.terminal().success)
        }
    }

    @Test(timeout = 10_000) fun cancellingAllOldGenerationJobsDoesNotCancelNewlySubmittedWork() {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val active = recording()
            val queued = recording()
            coordinator.submit(active.scope)
            val oldAction = take(backend.actions)
            active.receipt()
            coordinator.submit(queued.scope)

            coordinator.cancelAll(ErrorCode.ACCESSIBILITY_INTERRUPTED)

            assertFailure(active.terminal(), ErrorCode.ACCESSIBILITY_INTERRUPTED, Phase.COMPLETION, "failed", allowLifecyclePhase = true)
            assertFailure(queued.terminal(), ErrorCode.ACCESSIBILITY_INTERRUPTED, Phase.QUEUE, "rejected", allowLifecyclePhase = true)
            assertTrue(backend.actions.isEmpty())

            val fresh = recording()
            coordinator.submit(fresh.scope)
            val newAction = take(backend.actions)
            assertEquals(fresh.scope.sessionId, newAction.scope.sessionId)
            fresh.receipt()
            oldAction.callback(true)
            assertTrue(active.events.isEmpty())
            assertTrue(queued.events.isEmpty())
            assertTrue(fresh.events.isEmpty())
            assertFalse(newAction.completion.isDone)
            newAction.callback(true)
            assertTrue(fresh.terminal().success)
        }
    }

    @Test(timeout = 10_000) fun closedCoordinatorRejectsNewJobsAndClosesBackendOnlyOnce() {
        val backend = FakeBackend()
        val coordinator = DeviceCoordinator(backend)
        try {
            coordinator.close()
            coordinator.close()
            val action = recording()
            val observation = recording(operation = "observe")

            coordinator.submit(action.scope)
            coordinator.submit(observation.scope)

            assertFailure(action.terminal(), ErrorCode.STOPPED, Phase.LIFECYCLE, "rejected")
            val observationTerminal = observation.terminal()
            assertFailure(observationTerminal, ErrorCode.STOPPED, Phase.LIFECYCLE, null)
            assertEquals(1, backend.closeCalls.get())
            assertTrue(backend.actions.isEmpty())
            assertTrue(backend.observations.isEmpty())
            assertFalse(coordinator.cancel(action.scope.sessionId, action.scope.request.requestId))
        } finally {
            coordinator.close()
        }
    }

    @Test(timeout = 10_000) fun closeCancelsActiveAndQueuedJobsAndSuppressesLateCallback() {
        val backend = FakeBackend()
        val coordinator = DeviceCoordinator(backend)
        try {
            val active = recording()
            val queuedAction = recording()
            val queuedObservation = recording(operation = "observe")
            coordinator.submit(active.scope)
            val action = take(backend.actions)
            active.receipt()
            coordinator.submit(queuedAction.scope)
            coordinator.submit(queuedObservation.scope)

            coordinator.close()

            assertFailure(active.terminal(), ErrorCode.STOPPED, Phase.COMPLETION, "failed", allowLifecyclePhase = true)
            assertFailure(queuedAction.terminal(), ErrorCode.STOPPED, Phase.QUEUE, "rejected", allowLifecyclePhase = true)
            val observationTerminal = queuedObservation.terminal()
            assertFailure(observationTerminal, ErrorCode.STOPPED, Phase.QUEUE, null, allowLifecyclePhase = true)
            assertTrue(backend.actions.isEmpty())
            assertTrue(backend.observations.isEmpty())
            assertEquals(1, backend.closeCalls.get())
            action.callback(true)
            assertTrue(active.events.isEmpty())
            assertTrue(queuedAction.events.isEmpty())
            assertTrue(queuedObservation.events.isEmpty())
        } finally {
            coordinator.close()
        }
    }

    @Test(timeout = 10_000) fun closeCancelsAnActiveObservationWithoutReturningLateEvidence() {
        val capture = CompletableFuture<Observation>()
        val backend = FakeBackend().apply { onObserve = { scope, _ -> scope.await(capture) } }
        val coordinator = DeviceCoordinator(backend)
        try {
            val active = recording(operation = "observe")
            val queued = recording()
            coordinator.submit(active.scope)
            val captureCall = take(backend.observations)
            assertEquals(active.scope.sessionId, captureCall.scope.sessionId)
            coordinator.submit(queued.scope)

            coordinator.close()

            val terminal = active.terminal()
            assertFailure(terminal, ErrorCode.STOPPED, Phase.OBSERVE, null, allowLifecyclePhase = true)
            assertFailure(queued.terminal(), ErrorCode.STOPPED, Phase.QUEUE, "rejected", allowLifecyclePhase = true)
            assertFalse(capture.isDone)
            capture.complete(evidence())
            assertTrue(active.events.isEmpty())
            assertTrue(queued.events.isEmpty())
            assertTrue(backend.actions.isEmpty())
            assertEquals(1, backend.closeCalls.get())
        } finally {
            coordinator.close()
        }
    }

    @Test(timeout = 10_000) fun slowQueuedTerminalDeliveryDuringCloseCannotAllowLateDispatchOrSuccess() {
        val releaseCapture = CountDownLatch(1)
        val queuedTerminalEntered = CountDownLatch(1)
        val releaseQueuedTerminal = CountDownLatch(1)
        val firstQueuedTerminal = AtomicBoolean(true)
        val slowTerminalSink: (Envelope) -> Boolean = { envelope ->
            if (envelope.terminal && firstQueuedTerminal.compareAndSet(true, false)) {
                queuedTerminalEntered.countDown()
                check(releaseQueuedTerminal.await(WAIT_MS, TimeUnit.MILLISECONDS))
            }
            true
        }
        val backend = FakeBackend().apply {
            onObserve = { _, _ ->
                // Keep capture blocked even after scope cancellation until the close barrier is reached.
                check(releaseCapture.await(WAIT_MS, TimeUnit.MILLISECONDS))
                evidence()
            }
        }
        val coordinator = DeviceCoordinator(backend, queueSize = 2)
        val closer = Executors.newSingleThreadExecutor()
        try {
            val active = recording(operation = "observe")
            val queuedFirst = recording(onEvent = slowTerminalSink)
            val queuedSecond = recording(onEvent = slowTerminalSink)
            coordinator.submit(active.scope)
            take(backend.observations)
            coordinator.submit(queuedFirst.scope)
            coordinator.submit(queuedSecond.scope)
            val closing = closer.submit<Unit> { coordinator.close() }

            assertTrue(queuedTerminalEntered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            releaseCapture.countDown()
            val activeTerminal = active.terminal()

            assertFailure(activeTerminal, ErrorCode.STOPPED, Phase.OBSERVE, null, allowLifecyclePhase = true)
            assertNull(backend.actions.poll(25, TimeUnit.MILLISECONDS))

            releaseQueuedTerminal.countDown()
            closing.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertFailure(queuedFirst.terminal(), ErrorCode.STOPPED, Phase.QUEUE, "rejected", allowLifecyclePhase = true)
            assertFailure(queuedSecond.terminal(), ErrorCode.STOPPED, Phase.QUEUE, "rejected", allowLifecyclePhase = true)
            assertTrue(backend.actions.isEmpty())
            assertTrue(queuedFirst.events.isEmpty())
            assertTrue(queuedSecond.events.isEmpty())
            assertEquals(1, backend.closeCalls.get())
        } finally {
            releaseCapture.countDown()
            releaseQueuedTerminal.countDown()
            closer.shutdown()
            if (!closer.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS)) {
                closer.shutdownNow()
                assertTrue(closer.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
            }
            coordinator.close()
        }
    }

    @Test(timeout = 20_000) fun completionRacingCancellationAlwaysProducesExactlyOneTerminal() {
        val backend = FakeBackend()
        val racers = Executors.newFixedThreadPool(2)
        try {
            DeviceCoordinator(backend).use { coordinator ->
                repeat(24) {
                    val request = recording()
                    coordinator.submit(request.scope)
                    val action = take(backend.actions)
                    request.receipt()
                    val ready = CountDownLatch(2)
                    val start = CountDownLatch(1)
                    val cancellation = racers.submit<Boolean> {
                        ready.countDown()
                        check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                        coordinator.cancel(request.scope.sessionId, request.scope.request.requestId)
                    }
                    val completion = racers.submit<Boolean> {
                        ready.countDown()
                        check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                        action.callback(true)
                    }
                    assertTrue(ready.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    start.countDown()
                    val cancellationWon = cancellation.get(WAIT_MS, TimeUnit.MILLISECONDS)
                    assertTrue(completion.get(WAIT_MS, TimeUnit.MILLISECONDS))
                    val terminal = request.terminal()

                    if (terminal.success) {
                        assertFalse(cancellationWon)
                        assertEquals("completed", terminal.result!!["status"])
                        assertNull(terminal.error)
                    } else {
                        assertTrue(cancellationWon)
                        assertFailure(terminal, ErrorCode.CANCELLED, Phase.COMPLETION, "failed")
                    }
                    assertTrue(request.scope.isEnded)
                    assertFalse(coordinator.cancel(request.scope.sessionId, request.scope.request.requestId))
                    assertFalse(action.callback(false))
                    assertNull(request.scope.complete(mapOf("status" to "completed")))
                    assertTrue(request.events.isEmpty())
                }
            }
        } finally {
            racers.shutdownNow()
            assertTrue(racers.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 10_000) fun cancellingCommittedBlockedBodyPublishesTerminalAndReclaimsSessionImmediately() {
        val secret = "private-blocked-node-payload"
        val body = BlockedBody()
        val platformCalls = AtomicInteger()
        val logs = LinkedBlockingQueue<String>()
        val active = recording(action = ActionParams("set_text", "com.private.account", text = secret))
        val queued = recording()
        val replacement = recording(sessionId = active.scope.sessionId)
        val backend = FakeBackend()
        val ordinaryDispatch = backend.onDispatch
        backend.onDispatch = { scope, params ->
            if (scope.request.requestId == active.scope.request.requestId) scope.commitAction {
                platformCalls.incrementAndGet()
                body.block()
                CompletableFuture.completedFuture(ActionCompletion("accessibility_action_return"))
            } else ordinaryDispatch(scope, params)
        }
        val coordinator = DeviceCoordinator(backend, logError = { logs.offer(it) })
        val controller = Executors.newSingleThreadExecutor()
        try {
            coordinator.submit(active.scope)
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            active.receipt()
            coordinator.submit(queued.scope)
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            val cancelling = controller.submit<Boolean> { coordinator.cancel(active.scope.sessionId, active.scope.request.requestId) }

            assertTrue(cancelling.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertFailure(active.terminal(), ErrorCode.CANCELLED, Phase.COMPLETION, "failed")
            val activeCheck = controller.submit<AgentError> { assertThrows(AgentException::class.java) { active.scope.checkActive() }.error }
            assertEquals(AgentError(ErrorCode.CANCELLED, Phase.COMPLETION), activeCheck.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertEquals(1L, body.release.count)
            assertEquals(1L, body.exited.count)
            assertEquals(1, platformCalls.get())
            assertTrue(coordinator.cancel(queued.scope.sessionId, queued.scope.request.requestId))
            assertFailure(queued.terminal(), ErrorCode.CANCELLED, Phase.QUEUE, "rejected")

            coordinator.submit(replacement.scope)
            assertTrue(replacement.events.isEmpty())
            assertFalse(coordinator.cancel(active.scope.sessionId, active.scope.request.requestId))
            assertTrue(backend.actions.isEmpty())
            body.release.countDown()
            val next = take(backend.actions)
            assertEquals(replacement.scope.request.requestId, next.scope.request.requestId)
            replacement.receipt()
            assertFalse(coordinator.cancel(active.scope.sessionId, active.scope.request.requestId))
            next.callback(true)
            assertTrue(replacement.terminal().success)
            assertTrue(active.events.isEmpty())
            assertTrue(queued.events.isEmpty())
            assertTrue(backend.actions.isEmpty())
            val emittedLogs = setOf(take(logs), take(logs))
            assertEquals(
                setOf(
                    "request_id=${active.scope.request.requestId} phase=completion category=cancelled",
                    "request_id=${queued.scope.request.requestId} phase=queue category=cancelled",
                ),
                emittedLogs,
            )
            assertTrue(emittedLogs.none { it.contains(secret) || it.contains("com.private.account") })
            assertTrue(logs.isEmpty())
        } finally {
            body.release.countDown()
            coordinator.close()
            controller.shutdownNow()
            assertTrue(controller.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 10_000) fun deadlinePublishesCommittedFailureWhilePlatformBodyIgnoresInterruption() {
        val body = BlockedBody()
        val platformCalls = AtomicInteger()
        val logs = LinkedBlockingQueue<String>()
        val backend = FakeBackend().apply {
            onDispatch = { scope, _ -> scope.commitAction {
                platformCalls.incrementAndGet()
                body.block()
                CompletableFuture.completedFuture(ActionCompletion("accessibility_action_return"))
            } }
        }
        val coordinator = DeviceCoordinator(backend, logError = { logs.offer(it) })
        val controller = Executors.newSingleThreadExecutor()
        try {
            val active = recording(timeoutMs = 1_000)
            coordinator.submit(active.scope)
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            active.receipt()
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))

            assertFailure(active.terminal(), ErrorCode.TIMEOUT, Phase.COMPLETION, "failed")
            assertEquals(1L, body.release.count)
            assertEquals(1L, body.exited.count)
            assertEquals(1, platformCalls.get())
            assertFalse(coordinator.cancel(active.scope.sessionId, active.scope.request.requestId))
            val activeCheck = controller.submit<AgentError> { assertThrows(AgentException::class.java) { active.scope.checkActive() }.error }
            assertEquals(AgentError(ErrorCode.TIMEOUT, Phase.COMPLETION), activeCheck.get(WAIT_MS, TimeUnit.MILLISECONDS))
            val closing = controller.submit<Unit> { coordinator.close() }
            closing.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertEquals(1L, body.exited.count)

            body.release.countDown()
            assertTrue(body.exited.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertNull(active.scope.complete(mapOf("status" to "completed")))
            assertTrue(active.events.isEmpty())
            assertEquals("request_id=${active.scope.request.requestId} phase=completion category=timeout", take(logs))
            assertTrue(logs.isEmpty())
            assertEquals(1, backend.closeCalls.get())
        } finally {
            body.release.countDown()
            coordinator.close()
            controller.shutdownNow()
            assertTrue(controller.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 10_000) fun blockedAccessorCannotDelayObservationOrAnotherQueuedRequestDeadline() {
        val secret = "private-accessibility-window-text"
        val body = BlockedBody()
        val logs = LinkedBlockingQueue<String>()
        val backend = FakeBackend().apply {
            onObserve = { scope, _ -> scope.dispatch {
                body.block()
                Observation(
                    mapOf("tree" to SourceResult("tree", CapabilityStatus.AVAILABLE, data = mapOf("nodes" to listOf(mapOf("text" to secret))))),
                    emptyMap(),
                )
            } }
        }
        val coordinator = DeviceCoordinator(backend, logError = { logs.offer(it) })
        try {
            val active = recording(operation = "observe", timeoutMs = 1_000)
            val queued = recording(timeoutMs = 1_200)
            val sentinel = recording()
            coordinator.submit(active.scope)
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertEquals(active.scope.sessionId, take(backend.observations).scope.sessionId)
            coordinator.submit(queued.scope)
            coordinator.submit(sentinel.scope)
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))

            val observationTerminal = active.terminal()
            assertFailure(observationTerminal, ErrorCode.TIMEOUT, Phase.OBSERVE, null)
            assertFailure(queued.terminal(), ErrorCode.TIMEOUT, Phase.QUEUE, "rejected")
            assertFalse(observationTerminal.encode().contains(secret))
            assertEquals(1L, body.release.count)
            assertEquals(1L, body.exited.count)
            assertTrue(backend.actions.isEmpty())
            assertTrue(sentinel.events.isEmpty())

            body.release.countDown()
            val next = take(backend.actions)
            assertEquals(sentinel.scope.sessionId, next.scope.sessionId)
            sentinel.receipt()
            next.callback(true)
            assertTrue(sentinel.terminal().success)
            assertTrue(active.events.isEmpty())
            assertTrue(queued.events.isEmpty())
            assertTrue(backend.actions.isEmpty())
            val emittedLogs = setOf(take(logs), take(logs))
            assertEquals(
                setOf(
                    "request_id=${active.scope.request.requestId} phase=observe category=timeout",
                    "request_id=${queued.scope.request.requestId} phase=queue category=timeout",
                ),
                emittedLogs,
            )
            assertTrue(emittedLogs.none { it.contains(secret) })
            assertTrue(logs.isEmpty())
        } finally {
            body.release.countDown()
            coordinator.close()
        }
    }

    @Test(timeout = 10_000) fun cancelledBlockedPreflightPublishesRejectedAndSuppressesItsLateActionCommit() {
        val secret = "private-preflight-action-payload"
        val body = BlockedBody()
        val platformCalls = AtomicInteger()
        val logs = LinkedBlockingQueue<String>()
        val active = recording(action = ActionParams("set_text", "com.private.account", text = secret))
        val sentinel = recording()
        val backend = FakeBackend()
        val ordinaryDispatch = backend.onDispatch
        backend.onDispatch = { scope, params ->
            if (scope.request.requestId == active.scope.request.requestId) scope.dispatch {
                body.block()
                scope.commitAction {
                    platformCalls.incrementAndGet()
                    CompletableFuture.completedFuture(ActionCompletion("accessibility_action_return"))
                }
            } else ordinaryDispatch(scope, params)
        }
        val coordinator = DeviceCoordinator(backend, logError = { logs.offer(it) })
        val controller = Executors.newSingleThreadExecutor()
        try {
            coordinator.submit(active.scope)
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertTrue(active.events.isEmpty())
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            val cancelling = controller.submit<Boolean> { coordinator.cancel(active.scope.sessionId, active.scope.request.requestId) }

            assertTrue(cancelling.get(WAIT_MS, TimeUnit.MILLISECONDS))
            val terminal = active.terminal()
            assertFailure(terminal, ErrorCode.CANCELLED, Phase.DISPATCH, "rejected")
            assertFalse(terminal.encode().contains(secret))
            assertFalse(active.scope.wasDispatched)
            assertEquals(1L, body.exited.count)
            coordinator.submit(sentinel.scope)
            assertTrue(sentinel.events.isEmpty())

            body.release.countDown()
            val next = take(backend.actions)
            assertEquals(sentinel.scope.sessionId, next.scope.sessionId)
            sentinel.receipt()
            next.callback(true)
            assertTrue(sentinel.terminal().success)
            assertEquals(0, platformCalls.get())
            assertTrue(active.events.isEmpty())
            assertEquals("request_id=${active.scope.request.requestId} phase=dispatch category=cancelled", take(logs))
            assertTrue(logs.isEmpty())
        } finally {
            body.release.countDown()
            coordinator.close()
            controller.shutdownNow()
            assertTrue(controller.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 10_000) fun falseNodeApiReturnIsFailedUncertainAndQuarantinedWithoutPayloadLogs() {
        assertNodeUncertainty(throwing = false)
    }

    @Test(timeout = 10_000) fun exceptionAfterNodeApiInvocationIsFailedUncertainAndQuarantinedWithoutPayloadLogs() {
        assertNodeUncertainty(throwing = true)
    }

    @Test(timeout = 10_000) fun committedAsyncMainFutureIsReturnedBeforeBlockedApiAndCancellationKeepsLease() {
        val body = BlockedBody()
        val jobs = LinkedBlockingQueue<Runnable>()
        val lease = AtomicReference<Any?>(null)
        val recovery = AtomicReference<Any?>(null)
        val owner = AtomicReference<PlatformOperation<*>?>(null)
        val operation = PlatformOperation<ActionCompletion>(lease, recovery, owner, { jobs.offer(it) }, { jobs.remove(it) })
        val dispatchReturned = CountDownLatch(1)
        val active = recording()
        val blockedAdmission = recording()
        val backend = FakeBackend().apply {
            onDispatch = { scope, _ ->
                if (scope.request.requestId != active.scope.request.requestId) {
                    val admission = PlatformOperation<ActionCompletion>(lease, recovery, owner, { jobs.offer(it) }, { jobs.remove(it) })
                    if (!admission.acquire()) throw AgentException(AgentError(ErrorCode.BUSY, Phase.DISPATCH, retryable = true))
                    admission.complete(ActionCompletion("unexpected_admission"))
                    throw AgentException(AgentError(ErrorCode.INTERNAL_ERROR, Phase.DISPATCH))
                }
                check(operation.acquire())
                scope.onStopped { operation.abortBeforeStart() }
                val committed = CompletableFuture<Unit>()
                operation.enqueue(scope) { call -> scope.commitAction {
                    call.actionInvoked = true
                    committed.complete(Unit)
                    body.block()
                    call.complete(ActionCompletion("definite_callback"))
                } }
                operation.result.whenComplete { _, error -> if (error != null) committed.completeExceptionally(error) }
                scope.await(committed)
                dispatchReturned.countDown()
                operation.result
            }
        }
        val coordinator = DeviceCoordinator(backend)
        val actors = Executors.newFixedThreadPool(2)
        try {
            coordinator.submit(active.scope)
            val runningMain = actors.submit<Unit> { take(jobs).run() }
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            active.receipt()
            assertTrue(dispatchReturned.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertEquals(1L, body.exited.count)
            assertFalse(operation.result.isDone)
            assertSame(operation, lease.get())
            coordinator.submit(blockedAdmission.scope)
            val cancelling = actors.submit<Boolean> { coordinator.cancel(active.scope.sessionId, active.scope.request.requestId) }

            assertTrue(cancelling.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertFailure(active.terminal(), ErrorCode.CANCELLED, Phase.COMPLETION, "failed")
            val rejected = blockedAdmission.terminal()
            assertFalse(rejected.success)
            assertEquals(mapOf("status" to "rejected"), rejected.result)
            assertEquals(AgentError(ErrorCode.BUSY, Phase.DISPATCH, retryable = true), rejected.error)
            assertFalse(blockedAdmission.scope.wasDispatched)
            assertSame(operation, lease.get())
            assertSame(operation, owner.get())
            assertNull(recovery.get())
            assertFalse(operation.result.isDone)
            assertTrue(jobs.isEmpty())
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertEquals(1L, body.exited.count)

            body.release.countDown()
            runningMain.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertEquals(ActionCompletion("definite_callback"), operation.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertNull(lease.get())
            assertNull(owner.get())
            assertNull(recovery.get())
            assertTrue(active.events.isEmpty())
            assertTrue(blockedAdmission.events.isEmpty())
            assertNull(active.scope.complete(mapOf("status" to "completed")))
        } finally {
            body.release.countDown()
            coordinator.close()
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    private fun assertNodeUncertainty(throwing: Boolean) {
        val secret = "private-node-api-payload-and-exception"
        val jobs = LinkedBlockingQueue<Runnable>()
        val lease = AtomicReference<Any?>(null)
        val recovery = AtomicReference<Any?>(null)
        val owner = AtomicReference<PlatformOperation<*>?>(null)
        val operation = PlatformOperation<ActionCompletion>(lease, recovery, owner, { jobs.offer(it) }, { jobs.remove(it) })
        val platformCalls = AtomicInteger()
        val logs = LinkedBlockingQueue<String>()
        val backend = FakeBackend().apply {
            onDispatch = { scope, _ ->
                check(operation.acquire())
                scope.onStopped { operation.abortBeforeStart() }
                val committed = CompletableFuture<Unit>()
                operation.enqueue(scope) { call ->
                    scope.commitAction {
                        call.actionInvoked = true
                        committed.complete(Unit)
                        platformCalls.incrementAndGet()
                        if (throwing) throw IllegalStateException(secret)
                        call.quarantine(ActionCompletion("accessibility_action_return", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION)))
                    }
                }
                operation.result.whenComplete { _, error -> if (error != null) committed.completeExceptionally(error) }
                scope.await(committed)
                operation.result
            }
        }
        DeviceCoordinator(backend, logError = { logs.offer(it) }).use { coordinator ->
            val request = recording(action = ActionParams("set_text", "com.private.account", text = secret))
            coordinator.submit(request.scope)
            val mainJob = take(jobs)
            assertEquals(0, platformCalls.get())
            assertTrue(request.events.isEmpty())

            mainJob.run()
            request.receipt()
            val terminal = request.terminal()

            assertFailure(terminal, ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
            assertEquals(1, platformCalls.get())
            assertSame(operation, lease.get())
            assertSame(operation, recovery.get())
            assertSame(operation, owner.get())
            val next = PlatformOperation<ActionCompletion>(lease, recovery, owner, { jobs.offer(it) }, { jobs.remove(it) })
            assertFalse(next.acquire())
            assertTrue(jobs.isEmpty())
            val log = take(logs)
            assertEquals("request_id=${request.scope.request.requestId} phase=completion category=action_uncertain", log)
            assertFalse(log.contains(secret))
            assertFalse(log.contains("com.private.account"))
            assertFalse(terminal.encode().contains(secret))
            assertTrue(request.events.isEmpty())
            assertTrue(logs.isEmpty())
        }
    }

    private class BlockedBody {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val exited = CountDownLatch(1)
        val interrupted = CountDownLatch(1)
        private val thread = AtomicReference<Thread?>()

        fun interrupt() { checkNotNull(thread.get()).interrupt() }

        fun block() {
            thread.set(Thread.currentThread())
            entered.countDown()
            val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MS * 3)
            var wasInterrupted = false
            try {
                while (true) {
                    try {
                        check(release.await((deadline - System.nanoTime()).coerceAtLeast(1), TimeUnit.NANOSECONDS))
                        return
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

    private fun assertQueuedStop(code: ErrorCode) {
        val backend = FakeBackend()
        DeviceCoordinator(backend).use { coordinator ->
            val active = recording()
            coordinator.submit(active.scope)
            val activeAction = take(backend.actions)
            active.receipt()
            val queued = recording(timeoutMs = if (code == ErrorCode.TIMEOUT) 100 else 10_000)
            coordinator.submit(queued.scope)

            when (code) {
                ErrorCode.CANCELLED -> assertTrue(coordinator.cancel(queued.scope.sessionId, queued.scope.request.requestId))
                ErrorCode.DISCONNECTED -> coordinator.disconnect(queued.scope.sessionId)
                ErrorCode.TIMEOUT -> Unit
                else -> error("Unsupported test cancellation")
            }
            assertFailure(queued.terminal(), code, Phase.QUEUE, "rejected")
            assertFalse(queued.scope.wasDispatched)
            assertFalse(coordinator.cancel(queued.scope.sessionId, queued.scope.request.requestId))

            val sentinel = recording()
            coordinator.submit(sentinel.scope)
            activeAction.callback(true)
            assertTrue(active.terminal().success)
            val nextAction = take(backend.actions)
            assertEquals(sentinel.scope.sessionId, nextAction.scope.sessionId)
            sentinel.receipt()
            nextAction.callback(true)
            assertTrue(sentinel.terminal().success)
            assertTrue(backend.actions.isEmpty())
            assertTrue(queued.events.isEmpty())
        }
    }

    private fun assertDeferredStop(code: ErrorCode) {
        val clock = AtomicLong()
        val deferred = LinkedBlockingQueue<Runnable>()
        val platformCalls = AtomicInteger()
        val callback = CompletableFuture<ActionCompletion>()
        val accepted = CompletableFuture<CompletableFuture<ActionCompletion>?>()
        val backend = FakeBackend().apply {
            onDispatch = { scope, _ ->
                deferred.offer(Runnable {
                    try {
                        accepted.complete(scope.commitAction {
                            platformCalls.incrementAndGet()
                            callback
                        })
                    } catch (e: Exception) {
                        accepted.completeExceptionally(e)
                    }
                })
                scope.await(accepted)
            }
        }
        DeviceCoordinator(backend).use { coordinator ->
            val request = recording(clock = clock::get)
            coordinator.submit(request.scope)
            val mainRunnable = take(deferred)
            assertTrue(request.events.isEmpty())
            assertFalse(request.scope.wasDispatched)

            when (code) {
                ErrorCode.CANCELLED -> assertTrue(coordinator.cancel(request.scope.sessionId, request.scope.request.requestId))
                ErrorCode.DISCONNECTED -> coordinator.disconnect(request.scope.sessionId)
                ErrorCode.TIMEOUT -> {
                    clock.set(TimeUnit.MILLISECONDS.toNanos(request.scope.request.timeoutMs))
                    mainRunnable.run()
                }
                else -> error("Unsupported test cancellation")
            }
            assertFailure(request.terminal(), code, Phase.DISPATCH, "rejected")
            if (code != ErrorCode.TIMEOUT) mainRunnable.run()
            assertEquals(0, platformCalls.get())
            assertFalse(request.scope.wasDispatched)
            assertTrue(accepted.isCompletedExceptionally)

            callback.complete(ActionCompletion("gesture_callback"))
            assertTrue(request.events.isEmpty())
            assertTrue(backend.actions.isEmpty())
        }
    }

    private class Recording(
        val scope: RequestScope,
        val events: BlockingQueue<Envelope>,
    ) {
        fun receipt(): Envelope = take(events).also {
            assertFalse(it.terminal)
            assertTrue(it.success)
            assertNull(it.error)
            assertEquals(mapOf("status" to "dispatched"), it.result)
            assertEquals(Phase.COMPLETION, scope.phase)
        }

        fun terminal(): Envelope = take(events).also {
            assertTrue(it.terminal)
            assertEquals(scope.sessionId, it.sessionId)
            assertEquals(scope.request.requestId, it.requestId)
            assertEquals(scope.request.operation, it.operation)
        }
    }

    private fun recording(
        operation: String = "action",
        timeoutMs: Long = 10_000,
        sessionId: String = UUID.randomUUID().toString(),
        requestId: String = UUID.randomUUID().toString(),
        action: ActionParams = ActionParams("tap", "com.pulse.fixture", x = 10, y = 20),
        clock: () -> Long = System::nanoTime,
        onEvent: (Envelope) -> Boolean = { true },
    ): Recording {
        val events = LinkedBlockingQueue<Envelope>()
        val request = AgentRequest(
            requestId, operation, timeoutMs,
            action = action.takeIf { operation == "action" },
            observe = ObserveParams().takeIf { operation == "observe" },
        )
        val scope = RequestScope(sessionId, request, events = { events.offer(it) && onEvent(it) }, clock = clock)
        return Recording(scope, events)
    }

    private data class ActionCall(
        val scope: RequestScope,
        val params: ActionParams,
        val completion: CompletableFuture<ActionCompletion> = CompletableFuture(),
    ) {
        fun callback(success: Boolean): Boolean = completion.complete(
            ActionCompletion("gesture_callback", if (success) null else AgentError(ErrorCode.GESTURE_CANCELLED, Phase.COMPLETION)),
        )
    }

    private data class ObserveCall(val scope: RequestScope, val params: ObserveParams)

    private class FakeBackend : DeviceBackend {
        val actions = LinkedBlockingQueue<ActionCall>()
        val observations = LinkedBlockingQueue<ObserveCall>()
        val closeCalls = AtomicInteger()
        var onObserve: (RequestScope, ObserveParams) -> Observation = { _, _ -> evidence() }
        var onDispatch: (RequestScope, ActionParams) -> CompletableFuture<ActionCompletion>? = { scope, params ->
            scope.commitAction {
                ActionCall(scope, params).also { actions.offer(it) }.completion
            }
        }

        override fun observe(scope: RequestScope, params: ObserveParams): Observation {
            observations.offer(ObserveCall(scope, params))
            return onObserve(scope, params)
        }

        override fun dispatch(scope: RequestScope, params: ActionParams): CompletableFuture<ActionCompletion>? = onDispatch(scope, params)

        override fun close() { closeCalls.incrementAndGet() }
    }

    companion object {
        private const val WAIT_MS = 2_000L

        private fun <T : Any> take(queue: BlockingQueue<T>): T =
            checkNotNull(queue.poll(WAIT_MS, TimeUnit.MILLISECONDS)) { "No fake backend event within bounded wait" }

        private fun evidence() = Observation(
            sources = mapOf("tree" to SourceResult("tree", CapabilityStatus.AVAILABLE, data = mapOf("nodes" to emptyList<Any>()))),
            capabilities = mapOf("tree" to true),
        )

        private fun assertFailure(
            envelope: Envelope,
            code: ErrorCode,
            phase: Phase,
            status: String?,
            allowLifecyclePhase: Boolean = false,
        ) {
            assertTrue(envelope.terminal)
            assertFalse(envelope.success)
            val error = checkNotNull(envelope.error)
            assertEquals(code, error.code)
            assertFalse(error.retryable)
            if (allowLifecyclePhase) assertTrue(error.phase == phase || error.phase == Phase.LIFECYCLE)
            else assertEquals(phase, error.phase)
            assertEquals(status?.let { mapOf("status" to it) }, envelope.result)
        }
    }
}
