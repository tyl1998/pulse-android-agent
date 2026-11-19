package com.pulse.agent.execution

import com.pulse.agent.protocol.ActionParams
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.AgentProtocol
import com.pulse.agent.protocol.AgentRequest
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
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
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class PlatformOperationTest {
    @Test fun constructorIsInertAndAcquireClaimsTheLeaseExactlyOnce() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        assertTrue(AgentProtocol.validId(request.scope.sessionId))
        assertTrue(AgentProtocol.validId(request.scope.request.requestId))
        assertReleased(h)
        assertFalse(call.result.isDone)
        assertTrue(h.main.jobs.isEmpty())

        assertTrue(call.acquire())

        assertSame(call, h.lease.get())
        assertSame(call, h.owner.get())
        assertNull(h.recovery.get())
        assertFalse(call.acquire())
        assertSame(call, h.lease.get())
        call.complete("definite")
        assertEquals("definite", call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
        assertReleased(h)
        assertFalse(call.acquire())
    }

    @Test fun competingAcquireFailsClosedAndCannotRetryTheSameConsumedOperation() {
        val h = Harness()
        val first = h.operation<String>()
        val rejected = h.operation<String>()
        assertTrue(first.acquire())
        assertFalse(rejected.acquire())
        assertSame(first, h.lease.get())
        assertSame(first, h.owner.get())
        first.complete("done")

        assertFalse(rejected.acquire())
        rejected.enqueue(recording().scope) { it.complete("must-not-run") }

        assertTrue(h.main.jobs.isEmpty())
        assertReleased(h)
        assertTrue(h.operation<String>().acquire())
    }

    @Test fun retiredBeforeFirstAcquireCannotClaimResourcesOrReplay() {
        val retirements: List<(PlatformOperation<String>) -> Unit> = listOf(
            { it.abortBeforeStart() },
            { it.complete("already-complete") },
            { it.quarantine("already-resolved") },
            { it.fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH))) },
        )
        for (retire in retirements) {
            val h = Harness()
            val call = h.operation<String>()
            retire(call)
            assertTrue(call.result.isDone)

            assertFalse(call.acquire())
            assertFalse(call.acquire())
            call.enqueue(recording().scope) { it.complete("replay") }

            assertReleased(h)
            assertEquals(0, h.main.posts.get())
            assertTrue(h.main.jobs.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun simultaneousAcquireOfOneOperationHasExactlyOneWinner() {
        val h = Harness()
        val call = h.operation<String>()
        val ready = CountDownLatch(2)
        val start = CountDownLatch(1)
        val racers = Executors.newFixedThreadPool(2)
        try {
            val attempts = List(2) {
                racers.submit<Boolean> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    call.acquire()
                }
            }
            assertTrue(ready.await(WAIT_MS, TimeUnit.MILLISECONDS))
            start.countDown()

            assertEquals(1, attempts.count { it.get(WAIT_MS, TimeUnit.MILLISECONDS) })
            assertSame(call, h.lease.get())
            assertSame(call, h.owner.get())
            call.complete("done")
            assertReleased(h)
        } finally {
            start.countDown()
            racers.shutdownNow()
            assertTrue(racers.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 20_000) fun firstAcquireRacingAbortCannotOrphanAResolvedOperationLease() {
        val racers = Executors.newFixedThreadPool(2)
        try {
            repeat(24) {
                val h = Harness()
                val call = h.operation<String>()
                val ready = CountDownLatch(2)
                val start = CountDownLatch(1)
                val acquiring = racers.submit<Boolean> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    call.acquire()
                }
                val aborting = racers.submit<Unit> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    call.abortBeforeStart()
                }
                assertTrue(ready.await(WAIT_MS, TimeUnit.MILLISECONDS))
                start.countDown()
                acquiring.get(WAIT_MS, TimeUnit.MILLISECONDS)
                aborting.get(WAIT_MS, TimeUnit.MILLISECONDS)

                assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), failure(call.result).error)
                assertReleased(h)
                assertFalse(call.acquire())
                assertTrue(h.main.jobs.isEmpty())
            }
        } finally {
            racers.shutdownNow()
            assertTrue(racers.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test fun enqueueWithoutAcquireCannotPostOrInvoke() {
        val h = Harness()
        val call = h.operation<String>()
        val starts = AtomicInteger()

        call.enqueue(recording().scope) { starts.incrementAndGet() }

        assertEquals(0, starts.get())
        assertEquals(0, h.main.posts.get())
        assertTrue(h.main.jobs.isEmpty())
        assertReleased(h)
    }

    @Test fun enqueuePostsAsynchronouslyAndDefiniteCompletionReleasesItsOwnLease() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        acquireAndBind(call, request)

        call.enqueue(request.scope) { starts.incrementAndGet(); it.complete("definite") }

        assertEquals(0, starts.get())
        assertFalse(call.result.isDone)
        assertEquals(1, h.main.jobs.size)
        h.main.dequeue().run()
        assertEquals(1, starts.get())
        assertEquals("definite", call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
        assertReleased(h)
        assertTrue(h.main.jobs.isEmpty())
        assertNull(retainedRunnable(call))
    }

    @Test fun enqueueIsSingleUseBeforeStartDuringCallbackWaitAndAfterCompletion() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { starts.incrementAndGet() }
        call.enqueue(request.scope) { starts.addAndGet(100) }
        assertEquals(1, h.main.posts.get())
        assertEquals(1, h.main.jobs.size)
        val job = h.main.dequeue()
        job.run()

        call.enqueue(request.scope) { starts.addAndGet(100) }
        assertEquals(1, starts.get())
        assertEquals(1, h.main.posts.get())
        assertFalse(call.result.isDone)
        call.complete("done")
        call.enqueue(recording().scope) { starts.addAndGet(100) }
        job.run()

        assertEquals(1, starts.get())
        assertEquals(1, h.main.posts.get())
        assertFalse(call.acquire())
        assertReleased(h)
    }

    @Test fun replayedDequeuedRunnableCannotInvokeStartTwiceWhileCallbackIsPending() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { starts.incrementAndGet() }
        val job = h.main.dequeue()

        job.run()
        job.run()

        assertEquals(1, starts.get())
        assertFalse(call.result.isDone)
        assertSame(call, h.lease.get())
        call.complete("actual-callback")
        assertReleased(h)
    }

    @Test fun cancellationBetweenHookRegistrationAndEnqueuePostsNothing() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording(text = "private-not-posted-payload")
        val starts = AtomicInteger()
        acquireAndBind(call, request)
        assertTrue(request.scope.cancel(ErrorCode.CANCELLED))

        call.enqueue(request.scope) { starts.incrementAndGet() }

        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), failure(call.result).error)
        assertEquals(0, h.main.posts.get())
        assertEquals(0, h.main.removes.get())
        assertEquals(0, starts.get())
        assertNull(retainedRunnable(call))
        assertReleased(h)
    }

    @Test fun alreadyCancelledScopeAbortsImmediatelyWhenHookIsRegisteredBeforeEnqueue() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        assertTrue(request.scope.cancel(ErrorCode.DISCONNECTED))
        assertTrue(call.acquire())
        request.scope.onStopped { call.abortBeforeStart() }

        call.enqueue(request.scope) { it.complete("late") }

        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), failure(call.result).error)
        assertEquals(AgentError(ErrorCode.DISCONNECTED, Phase.DISPATCH), assertThrows(AgentException::class.java) { request.scope.checkActive() }.error)
        assertEquals(0, h.main.posts.get())
        assertTrue(h.main.jobs.isEmpty())
        assertReleased(h)
    }

    @Test fun queuedAbortRemovesOnlyItsExactRunnableAndSuppressesAStaleHandlerInvocation() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        val unrelated = Runnable { starts.addAndGet(100) }
        assertTrue(h.main.post(unrelated))
        acquireAndBind(call, request)
        call.enqueue(request.scope) { starts.incrementAndGet() }
        val exactJob = h.main.jobs.toList().last()

        assertTrue(request.scope.cancel(ErrorCode.CANCELLED))

        assertSame(exactJob, h.main.lastRemoved.get())
        assertEquals(1, h.main.removes.get())
        assertEquals(listOf(unrelated), h.main.jobs.toList())
        assertNull(retainedRunnable(call))
        assertReleased(h)
        exactJob.run()
        assertEquals(0, starts.get())
        assertSame(unrelated, h.main.dequeue())
        assertTrue(h.main.jobs.isEmpty())
    }

    @Test fun dequeuedBeforeBeginCancellationStillSuppressesTheExactJob() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { starts.incrementAndGet() }
        val dequeued = h.main.dequeue()
        assertTrue(h.main.jobs.isEmpty())

        assertTrue(request.scope.cancel(ErrorCode.CANCELLED))
        dequeued.run()
        call.enqueue(recording().scope) { starts.addAndGet(100) }

        assertSame(dequeued, h.main.lastRemoved.get())
        assertEquals(0, starts.get())
        assertEquals(1, h.main.posts.get())
        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), failure(call.result).error)
        assertNull(retainedRunnable(call))
        assertReleased(h)
    }

    @Test fun repeatedQueuedCancellationRetainsNoRunnableOrPayloadInTheNativeQueueOrOperation() {
        val h = Harness(recordRemovals = false)
        val starts = AtomicInteger()
        repeat(32) { index ->
            val secret = "private-retained-payload-$index"
            val request = recording(text = secret)
            val call = h.operation<String>()
            acquireAndBind(call, request)
            call.enqueue(request.scope) {
                starts.incrementAndGet()
                it.complete(checkNotNull(request.scope.request.action).text.orEmpty())
            }
            assertEquals(1, h.main.jobs.size)

            assertTrue(request.scope.cancel(ErrorCode.CANCELLED))

            assertTrue(h.main.jobs.isEmpty())
            assertNull(h.main.lastRemoved.get())
            // Inspect actual retained state, not a GC/WeakReference timing assumption.
            assertNull(retainedRunnable(call))
            assertReleased(h)
            assertFalse(call.acquire())
            call.enqueue(request.scope) { starts.addAndGet(100) }
            val error = failure(call.result)
            assertFalse(error.message.orEmpty().contains(secret))
            assertFailure(checkNotNull(request.scope.complete()), ErrorCode.CANCELLED, Phase.DISPATCH, "rejected")
            assertTrue(request.terminal().terminal)
            assertTrue(request.events.isEmpty())
        }
        assertEquals(32, h.main.posts.get())
        assertEquals(32, h.main.removes.get())
        assertEquals(0, starts.get())
        assertTrue(h.main.jobs.isEmpty())
    }

    @Test(timeout = 10_000) fun cancellationRacingPostCannotLeaveAnAlreadyAbortedJobQueued() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val posting = CountDownLatch(1)
        val releasePost = CountDownLatch(1)
        val cancelling = CountDownLatch(1)
        val starts = AtomicInteger()
        h.main.beforePost = {
            posting.countDown()
            check(releasePost.await(WAIT_MS * 3, TimeUnit.MILLISECONDS))
        }
        acquireAndBind(call, request)
        val actors = Executors.newFixedThreadPool(2)
        try {
            val enqueuing = actors.submit<Unit> { call.enqueue(request.scope) { starts.incrementAndGet() } }
            assertTrue(posting.await(WAIT_MS, TimeUnit.MILLISECONDS))
            val aborting = actors.submit<Boolean> {
                cancelling.countDown()
                request.scope.cancel(ErrorCode.CANCELLED)
            }
            assertTrue(cancelling.await(WAIT_MS, TimeUnit.MILLISECONDS))
            releasePost.countDown()
            enqueuing.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertTrue(aborting.get(WAIT_MS, TimeUnit.MILLISECONDS))

            assertTrue(h.main.jobs.isEmpty())
            assertEquals(0, starts.get())
            assertEquals(1, h.main.posts.get())
            assertEquals(1, h.main.removes.get())
            assertNull(retainedRunnable(call))
            assertReleased(h)
            assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), failure(call.result).error)
        } finally {
            releasePost.countDown()
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test fun rejectedSchedulerReleasesOwnerLeaseAndCannotReplayTheOperation() {
        val h = Harness()
        h.main.rejectPosts = true
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        acquireAndBind(call, request)

        call.enqueue(request.scope) { starts.incrementAndGet() }

        assertEquals(AgentError(ErrorCode.STOPPED, Phase.LIFECYCLE), failure(call.result).error)
        assertReleased(h)
        assertNull(retainedRunnable(call))
        assertTrue(h.main.jobs.isEmpty())
        assertFalse(call.acquire())
        call.enqueue(request.scope) { starts.incrementAndGet() }
        assertEquals(1, h.main.posts.get())
        assertEquals(0, starts.get())
    }

    @Test fun schedulerExceptionBeforePostingSafelyReleasesAndScrubsThrowableText() {
        val secret = "private-post-scheduler-address"
        val h = Harness()
        h.main.postException = IllegalStateException(secret)
        val call = h.operation<String>()
        val request = recording()
        acquireAndBind(call, request)

        call.enqueue(request.scope) { it.complete("must-not-start") }

        val error = failure(call.result)
        assertEquals(AgentError(ErrorCode.PLATFORM_FAILED, Phase.LIFECYCLE), error.error)
        assertFalse(error.message.orEmpty().contains(secret))
        assertReleased(h)
        assertNull(retainedRunnable(call))
        assertTrue(h.main.jobs.isEmpty())
        assertFalse(call.acquire())
        assertEquals(1, h.main.posts.get())
    }

    @Test fun failedRemovalPublishesCancellationButBoundsAdmissionUntilAbortedJobIsDequeued() {
        val secret = "private-remove-callback-error"
        val h = Harness()
        h.main.removeException = IllegalStateException(secret)
        val call = h.operation<String>()
        val request = recording(text = "private-one-retained-job")
        val starts = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { starts.incrementAndGet() }

        assertTrue(request.scope.cancel(ErrorCode.CANCELLED))

        val error = failure(call.result)
        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), error.error)
        assertFalse(error.message.orEmpty().contains(secret))
        assertSame(call, h.lease.get())
        assertSame(call, h.owner.get())
        assertNull(h.recovery.get())
        assertEquals(1, h.main.jobs.size)
        assertNull(retainedRunnable(call))
        val terminal = checkNotNull(request.scope.complete())
        assertFailure(terminal, ErrorCode.CANCELLED, Phase.DISPATCH, "rejected")
        assertEquals(terminal, request.terminal())
        assertSame(call, h.lease.get())
        repeat(16) {
            assertFalse(request.scope.cancel(ErrorCode.CANCELLED))
            call.abortBeforeStart()
            call.enqueue(recording().scope) { starts.incrementAndGet() }
            val retry = h.operation<String>()
            assertFalse(retry.acquire())
            retry.enqueue(recording().scope) { starts.incrementAndGet() }
            assertEquals(1, h.main.jobs.size)
        }
        assertEquals(1, h.main.posts.get())
        assertEquals(1, h.main.removes.get())

        h.main.dequeue().run()

        assertEquals(0, starts.get())
        assertReleased(h)
        assertTrue(h.main.jobs.isEmpty())
        assertEquals(error.error, failure(call.result).error)
        assertTrue(h.operation<String>().acquire())
    }

    @Test fun failedRemovalAfterDequeueStillSuppressesBeforeBeginAndReleasesOnlyItsLease() {
        val h = Harness()
        h.main.removeException = IllegalStateException("private-dequeued-remove-error")
        val call = h.operation<String>()
        val request = recording()
        val starts = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { starts.incrementAndGet() }
        val dequeued = h.main.dequeue()

        assertTrue(request.scope.cancel(ErrorCode.CANCELLED))
        assertSame(call, h.lease.get())
        assertTrue(h.main.jobs.isEmpty())
        dequeued.run()

        assertSame(dequeued, h.main.lastRemoved.get())
        assertEquals(0, starts.get())
        assertReleased(h)
        assertEquals(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE), failure(call.result).error)
    }

    @Test fun exceptionBeforeInvocationSafelyReleasesRatherThanQuarantining() {
        val secret = "private-preflight-root-and-node-text"
        val h = Harness()
        val call = h.operation<String>()
        val request = recording(text = secret)
        acquireAndBind(call, request)
        call.enqueue(request.scope) { throw IllegalStateException(secret) }

        h.main.dequeue().run()

        val error = failure(call.result)
        assertEquals(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH), error.error)
        assertFalse(error.message.orEmpty().contains(secret))
        assertFalse(call.actionInvoked)
        assertFalse(request.scope.wasDispatched)
        assertReleased(h)
        val terminal = checkNotNull(request.scope.complete(error = error.error))
        assertFailure(terminal, ErrorCode.PLATFORM_FAILED, Phase.DISPATCH, "rejected")
        assertFalse(terminal.encode().contains(secret))
        assertTrue(h.operation<String>().acquire())
    }

    @Test fun typedPreflightFailurePreservesSafeCodePhaseAndRetryability() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val expected = AgentError(ErrorCode.TARGET_CHANGED, Phase.DISPATCH, retryable = true)
        acquireAndBind(call, request)
        call.enqueue(request.scope) { throw AgentException(expected) }

        h.main.dequeue().run()

        assertEquals(expected, failure(call.result).error)
        assertReleased(h)
        assertFalse(call.actionInvoked)
    }

    @Test fun exceptionAfterCommittedInvocationIsActionUncertainAndRetainsRecoveryLease() {
        val secret = "private-native-action-exception-text"
        val h = Harness()
        val call = h.operation<ActionCompletion>()
        val request = recording(text = secret)
        acquireAndBind(call, request)
        call.enqueue(request.scope) { operation -> request.scope.commitAction {
            operation.actionInvoked = true
            throw IllegalStateException(secret)
        } }

        h.main.dequeue().run()

        request.receipt()
        val error = failure(call.result)
        assertEquals(AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION), error.error)
        assertFalse(error.message.orEmpty().contains(secret))
        assertSame(call, h.lease.get())
        assertSame(call, h.owner.get())
        assertSame(call, h.recovery.get())
        assertFalse(h.operation<ActionCompletion>().acquire())
        val terminal = checkNotNull(request.scope.complete(error = error.error))
        assertFailure(terminal, ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
        assertFalse(terminal.encode().contains(secret))
        assertEquals(terminal, request.terminal())
        assertTrue(request.events.isEmpty())
    }

    @Test fun falseNodeOutcomeIsQuarantinedFailedEvidenceNotSafeNeverInvokedRejection() {
        val h = Harness()
        val call = h.operation<ActionCompletion>()
        val request = recording()
        val uncertain = ActionCompletion("accessibility_action_return", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION))
        acquireAndBind(call, request)
        call.enqueue(request.scope) { operation -> request.scope.commitAction {
            operation.actionInvoked = true
            operation.quarantine(uncertain)
        } }

        h.main.dequeue().run()

        request.receipt()
        assertEquals(uncertain, call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
        assertFailure(checkNotNull(request.scope.complete(mapOf("status" to "completed"), uncertain.error)), ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
        assertSame(call, h.lease.get())
        assertSame(call, h.recovery.get())
        assertSame(call, h.owner.get())
        assertFalse(h.operation<ActionCompletion>().acquire())
        call.abortBeforeStart()
        assertSame(call, h.lease.get())
    }

    @Test fun cancellationOfStartedOperationsWaitsForDefiniteCallbackBeforeReleasingLease() {
        for (code in listOf(ErrorCode.CANCELLED, ErrorCode.TIMEOUT, ErrorCode.DISCONNECTED)) {
            val h = Harness()
            val call = h.operation<ActionCompletion>()
            val request = recording()
            acquireAndBind(call, request)
            call.enqueue(request.scope) { operation -> request.scope.commitAction { operation.actionInvoked = true } }
            h.main.dequeue().run()
            request.receipt()

            assertTrue(request.scope.cancel(code))
            assertFailure(checkNotNull(request.scope.complete()), code, Phase.COMPLETION, "failed")
            request.terminal()

            assertFalse(call.result.isDone)
            assertSame(call, h.lease.get())
            assertSame(call, h.owner.get())
            assertNull(h.recovery.get())
            assertFalse(h.operation<ActionCompletion>().acquire())
            call.complete(ActionCompletion("definite_callback"))
            assertEquals(ActionCompletion("definite_callback"), call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertReleased(h)
            assertNull(request.scope.complete(mapOf("status" to "completed")))
            assertEquals(AgentError(code, Phase.COMPLETION), assertThrows(AgentException::class.java) { request.scope.checkActive() }.error)
            assertTrue(request.events.isEmpty())
        }
    }

    @Test(timeout = 10_000) fun committedBlockedNativeBodyDoesNotDelayStopButLeaseWaitsForCallback() {
        assertBlockedNativeStop(throwing = false)
    }

    @Test(timeout = 10_000) fun stoppedBlockedBodyExceptionQuarantinesWithoutPublishingAnotherTerminal() {
        assertBlockedNativeStop(throwing = true)
    }

    @Test(timeout = 10_000) fun cancelledBlockedAccessorRetainsLeaseUntilReturnAndSuppressesNextReadAndCommit() {
        val h = Harness()
        val call = h.operation<String>()
        val request = recording()
        val body = BlockedBody()
        val laterReads = AtomicInteger()
        val platformCalls = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { operation ->
            request.scope.dispatch { body.block() }
            request.scope.dispatch { laterReads.incrementAndGet() }
            request.scope.commitAction { operation.actionInvoked = true; platformCalls.incrementAndGet() }
            operation.complete("must-not-complete")
        }
        val actors = Executors.newFixedThreadPool(2)
        try {
            val running = actors.submit<Unit> { h.main.dequeue().run() }
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            val stopping = actors.submit<Envelope> {
                assertTrue(request.scope.cancel(ErrorCode.CANCELLED))
                checkNotNull(request.scope.complete())
            }

            assertFailure(stopping.get(WAIT_MS, TimeUnit.MILLISECONDS), ErrorCode.CANCELLED, Phase.DISPATCH, "rejected")
            assertFalse(call.result.isDone)
            assertSame(call, h.lease.get())
            assertSame(call, h.owner.get())
            assertNull(h.recovery.get())
            assertEquals(1L, body.exited.count)
            assertFalse(h.operation<String>().acquire())

            body.release.countDown()
            running.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertEquals(AgentError(ErrorCode.CANCELLED, Phase.DISPATCH), failure(call.result).error)
            assertEquals(0, laterReads.get())
            assertEquals(0, platformCalls.get())
            assertFalse(request.scope.wasDispatched)
            assertReleased(h)
            assertTrue(request.terminal().terminal)
            assertTrue(request.events.isEmpty())
        } finally {
            body.release.countDown()
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test fun definiteCallbackReleasesQuarantinedLeaseWithoutRewritingUncertainOutcome() {
        val h = Harness()
        val call = h.operation<ActionCompletion>()
        val request = recording()
        val uncertain = ActionCompletion("uncertain", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION))
        acquireAndBind(call, request)
        call.enqueue(request.scope) { operation -> request.scope.commitAction {
            operation.actionInvoked = true
            operation.quarantine(uncertain)
        } }
        h.main.dequeue().run()
        request.receipt()
        assertFailure(checkNotNull(request.scope.complete(error = uncertain.error)), ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
        request.terminal()
        assertSame(call, h.recovery.get())

        call.complete(ActionCompletion("actual_definite_callback"))

        assertReleased(h)
        assertEquals(uncertain, call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
        assertNull(request.scope.complete(mapOf("status" to "completed")))
        assertTrue(request.events.isEmpty())
        val fresh = h.operation<ActionCompletion>()
        assertTrue(fresh.acquire())
        call.complete(ActionCompletion("duplicate_old_callback"))
        assertSame(fresh, h.lease.get())
        assertSame(fresh, h.owner.get())
    }

    @Test fun lateUncertaintyCannotOverwriteDefiniteOutcomeOrResurrectRecovery() {
        val h = Harness()
        val call = h.operation<ActionCompletion>()
        val request = recording()
        val definite = ActionCompletion("definite_callback")
        acquireAndBind(call, request)
        call.enqueue(request.scope) { operation -> request.scope.commitAction { operation.actionInvoked = true } }
        h.main.dequeue().run()
        call.complete(definite)

        call.quarantine(ActionCompletion("late_uncertain", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION)))
        call.fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH)))

        assertEquals(definite, call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
        assertReleased(h)
    }

    @Test fun duplicateOldCallbackCannotClearNewOwnerLeaseOrRecovery() {
        val h = Harness()
        val old = h.operation<String>()
        val oldRequest = recording()
        acquireAndBind(old, oldRequest)
        old.enqueue(oldRequest.scope) { }
        h.main.dequeue().run()
        old.actionInvoked = true
        old.complete("old-definite")
        val fresh = h.operation<String>()
        val freshRequest = recording()
        acquireAndBind(fresh, freshRequest)
        fresh.enqueue(freshRequest.scope) { it.quarantine("new-uncertain") }
        h.main.dequeue().run()

        old.complete("duplicate-old")
        old.fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH)))
        old.quarantine("late-old-uncertain")

        assertSame(fresh, h.lease.get())
        assertSame(fresh, h.owner.get())
        assertSame(fresh, h.recovery.get())
        assertEquals("old-definite", old.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
        assertEquals("new-uncertain", fresh.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
    }

    @Test fun globalLeaseSurvivesBackendOwnerReplacementUntilOldDefiniteCallback() {
        val h = Harness()
        val oldOwner = h.owner
        val newOwner = AtomicReference<PlatformOperation<*>?>(null)
        val old = h.operation<ActionCompletion>()
        val oldRequest = recording()
        acquireAndBind(old, oldRequest)
        old.enqueue(oldRequest.scope) { operation -> oldRequest.scope.commitAction { operation.actionInvoked = true } }
        h.main.dequeue().run()
        oldRequest.receipt()
        oldOwner.set(null)
        assertTrue(oldRequest.scope.cancel(ErrorCode.DISCONNECTED))
        assertFailure(checkNotNull(oldRequest.scope.complete()), ErrorCode.DISCONNECTED, Phase.COMPLETION, "failed")
        oldRequest.terminal()
        val replacementAttempt = h.operation<ActionCompletion>(ownerOverride = newOwner)

        assertFalse(replacementAttempt.acquire())
        replacementAttempt.enqueue(recording().scope) { it.complete(ActionCompletion("must-not-start")) }

        assertSame(old, h.lease.get())
        assertNull(newOwner.get())
        assertTrue(h.main.jobs.isEmpty())
        assertFalse(old.result.isDone)
        old.complete(ActionCompletion("old_definite_callback"))
        assertNull(h.lease.get())
        assertNull(h.recovery.get())
        val replacement = h.operation<ActionCompletion>(ownerOverride = newOwner)
        assertTrue(replacement.acquire())
        assertSame(replacement, newOwner.get())
        old.complete(ActionCompletion("duplicate_old_callback"))
        assertSame(replacement, h.lease.get())
        assertSame(replacement, newOwner.get())
        assertTrue(oldRequest.events.isEmpty())
    }

    @Test(timeout = 15_000) fun blockedOldFutureCallbackCannotMutateOrEmitSuccessForReconnectedScope() {
        val h = Harness()
        val old = h.operation<ActionCompletion>()
        val oldRequest = recording()
        acquireAndBind(old, oldRequest)
        old.enqueue(oldRequest.scope) { operation -> oldRequest.scope.commitAction { operation.actionInvoked = true } }
        h.main.dequeue().run()
        oldRequest.receipt()
        assertTrue(oldRequest.scope.cancel(ErrorCode.DISCONNECTED))
        assertFailure(checkNotNull(oldRequest.scope.complete()), ErrorCode.DISCONNECTED, Phase.COMPLETION, "failed")
        oldRequest.terminal()
        val callbackEntered = CountDownLatch(1)
        val releaseCallback = CountDownLatch(1)
        val latePublication = CompletableFuture<Envelope?>()
        old.result.thenAccept {
            callbackEntered.countDown()
            check(releaseCallback.await(WAIT_MS * 3, TimeUnit.MILLISECONDS))
            latePublication.complete(oldRequest.scope.complete(mapOf("status" to "completed")))
        }
        val actors = Executors.newFixedThreadPool(2)
        try {
            val completingOld = actors.submit<Unit> { old.complete(ActionCompletion("old_definite_callback")) }
            assertTrue(callbackEntered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            assertReleased(h)
            val fresh = h.operation<ActionCompletion>()
            val reconnected = recording(requestId = oldRequest.scope.request.requestId)
            assertFalse(oldRequest.scope.sessionId == reconnected.scope.sessionId)
            val uncertain = ActionCompletion("new_uncertain", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION))
            acquireAndBind(fresh, reconnected)
            fresh.enqueue(reconnected.scope) { operation -> reconnected.scope.commitAction {
                operation.actionInvoked = true
                operation.quarantine(uncertain)
            } }
            h.main.dequeue().run()
            reconnected.receipt()
            val oldCallbacks = actors.submit<Unit> {
                old.complete(ActionCompletion("duplicate_old"))
                old.fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH)))
                old.quarantine(ActionCompletion("late_old_uncertain"))
            }

            oldCallbacks.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertSame(fresh, h.lease.get())
            assertSame(fresh, h.owner.get())
            assertSame(fresh, h.recovery.get())
            assertEquals(uncertain, fresh.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertFalse(reconnected.scope.isEnded)
            assertTrue(reconnected.events.isEmpty())
            assertTrue(oldRequest.events.isEmpty())
            releaseCallback.countDown()
            completingOld.get(WAIT_MS, TimeUnit.MILLISECONDS)
            assertNull(latePublication.get(WAIT_MS, TimeUnit.MILLISECONDS))
            assertTrue(oldRequest.events.isEmpty())
            assertTrue(reconnected.events.isEmpty())

            val terminal = checkNotNull(reconnected.scope.complete(error = uncertain.error))
            assertFailure(terminal, ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
            assertEquals(reconnected.scope.sessionId, terminal.sessionId)
            assertEquals(oldRequest.scope.request.requestId, terminal.requestId)
            assertEquals(terminal, reconnected.terminal())
            assertTrue(reconnected.events.isEmpty())
        } finally {
            releaseCallback.countDown()
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 20_000) fun startVersusAbortRaceHasAtMostOneInvocationOneTerminalAndNoLeakedJob() {
        val actors = Executors.newFixedThreadPool(2)
        try {
            repeat(24) {
                val h = Harness(recordRemovals = false)
                val call = h.operation<ActionCompletion>()
                val request = recording()
                val invocations = AtomicInteger()
                acquireAndBind(call, request)
                call.enqueue(request.scope) { operation -> request.scope.commitAction {
                    operation.actionInvoked = true
                    invocations.incrementAndGet()
                } }
                val dequeued = h.main.dequeue()
                val ready = CountDownLatch(2)
                val start = CountDownLatch(1)
                val beginning = actors.submit<Unit> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    dequeued.run()
                }
                val aborting = actors.submit<Boolean> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    request.scope.cancel(ErrorCode.CANCELLED)
                }
                assertTrue(ready.await(WAIT_MS, TimeUnit.MILLISECONDS))
                start.countDown()
                beginning.get(WAIT_MS, TimeUnit.MILLISECONDS)
                assertTrue(aborting.get(WAIT_MS, TimeUnit.MILLISECONDS))
                val committed = request.scope.wasDispatched
                val phase = if (committed) Phase.COMPLETION else Phase.DISPATCH
                val status = if (committed) "failed" else "rejected"
                assertFailure(checkNotNull(request.scope.complete(mapOf("status" to "completed"))), ErrorCode.CANCELLED, phase, status)
                val envelopes = mutableListOf<Envelope>()
                request.events.drainTo(envelopes)

                assertTrue(invocations.get() in 0..1)
                assertEquals(1, envelopes.count { it.terminal })
                assertEquals(if (committed) 1 else 0, envelopes.count { !it.terminal })
                assertTrue(envelopes.all { it.sessionId == request.scope.sessionId && it.requestId == request.scope.request.requestId })
                if (call.result.isDone) {
                    assertEquals(ErrorCode.CANCELLED, failure(call.result).error.code)
                    assertReleased(h)
                } else {
                    assertEquals(1, invocations.get())
                    assertSame(call, h.lease.get())
                    call.complete(ActionCompletion("actual_callback"))
                }
                assertReleased(h)
                assertNull(retainedRunnable(call))
                assertTrue(h.main.jobs.isEmpty())
                assertFalse(call.acquire())
                call.enqueue(recording().scope) { invocations.addAndGet(100) }
                val beforeReplay = invocations.get()
                dequeued.run()
                assertEquals(beforeReplay, invocations.get())
                assertNull(request.scope.complete(mapOf("status" to "completed")))
                assertTrue(request.events.isEmpty())
            }
        } finally {
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    @Test(timeout = 20_000) fun definiteCallbackRacingQuarantineOrFailureCannotResurrectRecoveryOrRewriteOutcome() {
        val actors = Executors.newFixedThreadPool(2)
        try {
            repeat(32) { index ->
                val h = Harness()
                val call = h.operation<ActionCompletion>()
                val request = recording()
                val publications = AtomicInteger()
                call.result.whenComplete { _, _ -> publications.incrementAndGet() }
                acquireAndBind(call, request)
                call.enqueue(request.scope) { operation -> request.scope.commitAction { operation.actionInvoked = true } }
                h.main.dequeue().run()
                request.receipt()
                val ready = CountDownLatch(2)
                val start = CountDownLatch(1)
                val definite = actors.submit<Unit> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    call.complete(ActionCompletion("definite_callback"))
                }
                val uncertain = actors.submit<Unit> {
                    ready.countDown()
                    check(start.await(WAIT_MS, TimeUnit.MILLISECONDS))
                    if (index % 2 == 0) call.quarantine(ActionCompletion("uncertain", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION)))
                    else call.fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH)))
                }
                assertTrue(ready.await(WAIT_MS, TimeUnit.MILLISECONDS))
                start.countDown()
                definite.get(WAIT_MS, TimeUnit.MILLISECONDS)
                uncertain.get(WAIT_MS, TimeUnit.MILLISECONDS)

                assertReleased(h)
                assertEquals(1, publications.get())
                if (call.result.isCompletedExceptionally) {
                    val error = failure(call.result).error
                    assertEquals(AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION), error)
                    assertFailure(checkNotNull(request.scope.complete(error = error)), ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
                } else {
                    val outcome = call.result.get(WAIT_MS, TimeUnit.MILLISECONDS)
                    assertTrue(outcome.source == "definite_callback" || outcome.source == "uncertain")
                    val terminal = checkNotNull(request.scope.complete(mapOf("status" to "completed"), outcome.error))
                    if (outcome.error == null) assertTrue(terminal.success)
                    else assertFailure(terminal, ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION, "failed")
                }
                assertTrue(request.terminal().terminal)
                assertNull(request.scope.complete(mapOf("status" to "completed")))
                assertTrue(request.events.isEmpty())
                assertFalse(call.acquire())
                assertTrue(h.operation<ActionCompletion>().acquire())
            }
        } finally {
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    private fun assertBlockedNativeStop(throwing: Boolean) {
        val secret = "private-blocked-native-action-exception"
        val h = Harness()
        val call = h.operation<ActionCompletion>()
        val request = recording(text = secret)
        val body = BlockedBody()
        val invocations = AtomicInteger()
        acquireAndBind(call, request)
        call.enqueue(request.scope) { operation -> request.scope.commitAction {
            operation.actionInvoked = true
            invocations.incrementAndGet()
            body.block()
            if (throwing) throw IllegalStateException(secret)
            operation.complete(ActionCompletion("definite_callback"))
        } }
        val actors = Executors.newFixedThreadPool(2)
        try {
            val running = actors.submit<Unit> { h.main.dequeue().run() }
            assertTrue(body.entered.await(WAIT_MS, TimeUnit.MILLISECONDS))
            request.receipt()
            body.interrupt()
            assertTrue(body.interrupted.await(WAIT_MS, TimeUnit.MILLISECONDS))
            val stopping = actors.submit<Envelope> {
                assertTrue(request.scope.cancel(ErrorCode.CANCELLED))
                assertEquals(AgentError(ErrorCode.CANCELLED, Phase.COMPLETION), assertThrows(AgentException::class.java) { request.scope.checkActive() }.error)
                checkNotNull(request.scope.complete())
            }

            assertFailure(stopping.get(WAIT_MS, TimeUnit.MILLISECONDS), ErrorCode.CANCELLED, Phase.COMPLETION, "failed")
            request.terminal()
            assertEquals(1L, body.release.count)
            assertEquals(1L, body.exited.count)
            assertFalse(call.result.isDone)
            assertSame(call, h.lease.get())
            assertSame(call, h.owner.get())
            assertNull(h.recovery.get())
            assertFalse(h.operation<ActionCompletion>().acquire())
            body.release.countDown()
            running.get(WAIT_MS, TimeUnit.MILLISECONDS)

            assertEquals(1, invocations.get())
            if (throwing) {
                val error = failure(call.result)
                assertEquals(AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION), error.error)
                assertFalse(error.message.orEmpty().contains(secret))
                assertSame(call, h.lease.get())
                assertSame(call, h.recovery.get())
                assertFalse(h.operation<ActionCompletion>().acquire())
                assertNull(request.scope.complete(error = error.error))
            } else {
                assertEquals(ActionCompletion("definite_callback"), call.result.get(WAIT_MS, TimeUnit.MILLISECONDS))
                assertReleased(h)
                assertNull(request.scope.complete(mapOf("status" to "completed")))
            }
            assertTrue(request.events.isEmpty())
            assertEquals(AgentError(ErrorCode.CANCELLED, Phase.COMPLETION), assertThrows(AgentException::class.java) { request.scope.checkActive() }.error)
        } finally {
            body.release.countDown()
            actors.shutdownNow()
            assertTrue(actors.awaitTermination(WAIT_MS, TimeUnit.MILLISECONDS))
        }
    }

    private class Harness(recordRemovals: Boolean = true) {
        val lease = AtomicReference<Any?>(null)
        val recovery = AtomicReference<Any?>(null)
        val owner = AtomicReference<PlatformOperation<*>?>(null)
        val main = FakeMainQueue(recordRemovals)

        fun <T> operation(ownerOverride: AtomicReference<PlatformOperation<*>?> = owner): PlatformOperation<T> =
            PlatformOperation(lease, recovery, ownerOverride, main::post, main::remove)
    }

    private class FakeMainQueue(private val recordRemovals: Boolean) {
        val jobs = LinkedBlockingQueue<Runnable>()
        val posts = AtomicInteger()
        val removes = AtomicInteger()
        val lastRemoved = AtomicReference<Runnable?>()
        var rejectPosts = false
        var postException: RuntimeException? = null
        var removeException: RuntimeException? = null
        var beforePost: () -> Unit = {}

        fun post(job: Runnable): Boolean {
            posts.incrementAndGet()
            beforePost()
            postException?.let { throw it }
            return !rejectPosts && jobs.offer(job)
        }

        fun remove(job: Runnable) {
            removes.incrementAndGet()
            if (recordRemovals) lastRemoved.set(job)
            removeException?.let { throw it }
            jobs.remove(job)
        }

        fun dequeue(): Runnable = take(jobs)
    }

    private class Recording(val scope: RequestScope, val events: BlockingQueue<Envelope>) {
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
        }
    }

    private fun recording(text: String = "fixture", requestId: String = UUID.randomUUID().toString()): Recording {
        val events = LinkedBlockingQueue<Envelope>()
        val scope = RequestScope(
            UUID.randomUUID().toString(),
            AgentRequest(requestId, "action", 10_000, action = ActionParams("set_text", "com.pulse.fixture", text = text)),
            events = { events.offer(it) },
        )
        scope.phase = Phase.DISPATCH
        return Recording(scope, events)
    }

    private fun <T> acquireAndBind(call: PlatformOperation<T>, request: Recording) {
        assertTrue(call.acquire())
        request.scope.onStopped { call.abortBeforeStart() }
    }

    private fun failure(future: CompletableFuture<*>): AgentException {
        val thrown = assertThrows(ExecutionException::class.java) { future.get(WAIT_MS, TimeUnit.MILLISECONDS) }
        assertTrue(thrown.cause is AgentException)
        val error = thrown.cause as AgentException
        assertNull(error.cause)
        assertEquals(error.error.code.wire, error.message)
        return error
    }

    private fun retainedRunnable(call: PlatformOperation<*>): Any? {
        val field = PlatformOperation::class.java.getDeclaredField("runnable")
        field.isAccessible = true
        return field.get(call)
    }

    private fun assertReleased(h: Harness) {
        assertNull(h.lease.get())
        assertNull(h.owner.get())
        assertNull(h.recovery.get())
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

    companion object {
        private const val WAIT_MS = 2_000L

        private fun <T : Any> take(queue: BlockingQueue<T>): T =
            checkNotNull(queue.poll(WAIT_MS, TimeUnit.MILLISECONDS)) { "No native-helper event within bounded wait" }

        private fun assertFailure(envelope: Envelope, code: ErrorCode, phase: Phase, status: String) {
            assertTrue(envelope.terminal)
            assertFalse(envelope.success)
            assertEquals(AgentError(code, phase), envelope.error)
            assertEquals(mapOf("status" to status), envelope.result)
        }
    }
}
