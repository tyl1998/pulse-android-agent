package com.pulse.agent.execution

import com.pulse.agent.protocol.ActionParams
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.ObserveParams
import com.pulse.agent.protocol.Phase
import com.pulse.agent.protocol.SafeLog
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

data class ActionCompletion(val source: String, val error: AgentError? = null)

interface DeviceBackend : AutoCloseable {
    fun observe(scope: RequestScope, params: ObserveParams): Observation
    fun dispatch(scope: RequestScope, params: ActionParams): CompletableFuture<ActionCompletion>?
}

/** Exactly one device worker, including the wait for platform completion. */
class DeviceCoordinator(
    private val backend: DeviceBackend,
    queueSize: Int = 8,
    private val logError: (String) -> Unit = {},
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val lifecycle = AtomicReference(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE))
    private val requests = ConcurrentHashMap<String, Work>()
    private val worker = ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, ArrayBlockingQueue<Runnable>(queueSize)) { r ->
        Thread(r, "pulse-device").apply { isDaemon = true }
    }
    private val deadlines = ScheduledThreadPoolExecutor(1) { r -> Thread(r, "pulse-deadline").apply { isDaemon = true } }
        .apply { removeOnCancelPolicy = true }

    @Synchronized
    fun submit(scope: RequestScope) {
        if (closed.get()) { finish(scope, error = AgentError(ErrorCode.STOPPED, Phase.LIFECYCLE)); return }
        val generation = lifecycle.get()
        scope.lifecycleError = {
            if (closed.get()) AgentError(ErrorCode.STOPPED, Phase.LIFECYCLE)
            else lifecycle.get().takeIf { it !== generation }
        }
        val work = Work(scope)
        if (requests.putIfAbsent(scope.sessionId, work) != null) {
            finish(scope, error = AgentError(ErrorCode.BUSY, Phase.QUEUE)); return
        }
        try {
            work.deadline = deadlines.schedule({ stop(work, ErrorCode.TIMEOUT) }, scope.remainingMs, TimeUnit.MILLISECONDS)
            worker.execute(work)
        } catch (_: RejectedExecutionException) {
            cleanup(work)
            finish(scope, error = AgentError(ErrorCode.BUSY, Phase.QUEUE))
        }
    }

    fun cancel(sessionId: String, requestId: String): Boolean {
        val work = requests[sessionId] ?: return false
        if (work.scope.request.requestId != requestId) return false
        return stop(work, ErrorCode.CANCELLED)
    }

    fun disconnect(sessionId: String) { requests[sessionId]?.let { stop(it, ErrorCode.DISCONNECTED) } }

    @Synchronized
    fun cancelAll(code: ErrorCode) {
        // Invalidate every old scope before waking any waiter or emitting any receipt.
        lifecycle.set(AgentError(code, Phase.LIFECYCLE))
        requests.values.forEach { stop(it, code) }
    }

    @Synchronized
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        cancelAll(ErrorCode.STOPPED)
        worker.shutdownNow()
        deadlines.shutdownNow()
        backend.close()
    }

    private fun stop(work: Work, code: ErrorCode): Boolean {
        if (!work.scope.cancel(code)) return false
        worker.remove(work)
        // A blocked Android accessor must not delay the terminal receipt or client deadline.
        finish(work.scope)
        cleanup(work)
        return true
    }

    private fun cleanup(work: Work) {
        work.deadline?.cancel(false)
        requests.remove(work.scope.sessionId, work)
    }

    private fun finish(scope: RequestScope, result: Map<String, Any?>? = null, error: AgentError? = null) {
        scope.complete(result, error)?.error?.let { logError(SafeLog.error(scope.request.requestId, it)) }
    }

    private inner class Work(val scope: RequestScope) : Runnable {
        @Volatile var deadline: ScheduledFuture<*>? = null

        override fun run() {
            try {
                scope.checkActive()
                when (scope.request.operation) {
                    "observe" -> {
                        scope.phase = Phase.OBSERVE
                        val observation = backend.observe(scope, checkNotNull(scope.request.observe))
                        if (observation.hasEvidence) finish(scope, observation.toWire())
                        else finish(scope, observation.toWire(), AgentError(ErrorCode.WINDOW_UNAVAILABLE, Phase.OBSERVE))
                    }
                    "action" -> {
                        scope.phase = Phase.DISPATCH
                        val completion = backend.dispatch(scope, checkNotNull(scope.request.action))
                        if (completion == null) finish(scope, error = AgentError(ErrorCode.DISPATCH_REJECTED, Phase.DISPATCH))
                        else {
                            scope.dispatchedReceipt()
                            scope.phase = Phase.COMPLETION
                            val outcome = scope.await(completion)
                            finish(scope, mapOf("status" to "completed", "completion_source" to outcome.source), outcome.error)
                        }
                    }
                    else -> finish(scope, error = AgentError(ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL))
                }
            } catch (e: AgentException) {
                finish(scope, error = e.error)
            } catch (_: Exception) {
                finish(scope, error = AgentError(ErrorCode.INTERNAL_ERROR, scope.phase))
            } finally {
                cleanup(this)
            }
        }
    }
}
