package com.pulse.agent.execution

import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.AgentRequest
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/** Short commit/cancellation transitions; no lock is held during a platform call. */
class RequestScope(
    val sessionId: String,
    val request: AgentRequest,
    private val connectionAlive: () -> Boolean = { true },
    private val events: (Envelope) -> Boolean,
    private val clock: () -> Long = System::nanoTime,
) {
    private val lock = Any()
    private val deadline = clock() + TimeUnit.MILLISECONDS.toNanos(request.timeoutMs)
    private val cancellation = CompletableFuture<Unit>()
    private var stopped: AgentError? = null
    private var ended = false
    private var dispatched = false
    private var receiptSent = false
    @Volatile internal var lifecycleError: () -> AgentError? = { null }

    @Volatile var phase: Phase = Phase.QUEUE
        internal set

    val wasDispatched: Boolean get() = synchronized(lock) { dispatched }
    val isEnded: Boolean get() = synchronized(lock) { ended }
    val remainingMs: Long get() = TimeUnit.NANOSECONDS.toMillis((deadline - clock()).coerceAtLeast(0))

    fun checkActive() = synchronized(lock) { checkLocked() }

    fun onStopped(callback: () -> Unit) { cancellation.thenRun(callback) }

    fun cancel(code: ErrorCode): Boolean = synchronized(lock) {
        if (ended || stopped != null) return false
        stopped = AgentError(code, phase)
        cancellation.complete(Unit)
        true
    }

    fun <T> dispatch(block: () -> T): T {
        synchronized(lock) { checkLocked() }
        return block()
    }

    fun <T> commitAction(block: () -> T): T {
        synchronized(lock) {
            checkLocked()
            dispatched = true
            phase = Phase.COMPLETION
            sendDispatchedLocked()
        }
        checkActive()
        return block()
    }

    fun markDispatched() = synchronized(lock) {
        // Record an irreversible platform submission, even if its return crossed the deadline.
        dispatched = true
    }

    fun <T> await(future: CompletableFuture<T>): T {
        checkActive()
        try {
            CompletableFuture.anyOf(future, cancellation).get((deadline - clock()).coerceAtLeast(1), TimeUnit.NANOSECONDS)
            checkActive()
            return future.get()
        } catch (_: TimeoutException) {
            cancel(ErrorCode.TIMEOUT)
            checkActive()
            throw AgentException(AgentError(ErrorCode.TIMEOUT, phase))
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            cancel(ErrorCode.STOPPED)
            throw AgentException(AgentError(ErrorCode.STOPPED, phase))
        } catch (e: ExecutionException) {
            throw e.cause as? AgentException ?: AgentException(AgentError(ErrorCode.PLATFORM_FAILED, phase))
        }
    }

    fun dispatchedReceipt() = synchronized(lock) {
        checkLocked()
        check(dispatched)
        sendDispatchedLocked()
    }

    private fun sendDispatchedLocked() {
        if (receiptSent) return
        receiptSent = true
        if (!events(Envelope(request.requestId, sessionId, request.operation, true, false, mapOf("status" to "dispatched")))) {
            cancel(ErrorCode.DISCONNECTED)
            checkLocked()
        }
    }

    fun complete(result: Map<String, Any?>? = null, error: AgentError? = null): Envelope? {
        val envelope = synchronized(lock) {
            if (ended) return null
            refreshStop()
            val failure = stopped ?: error
            ended = true
            cancellation.complete(Unit)
            val finalResult = if (failure != null && request.operation == "action") {
                mapOf("status" to if (dispatched) "failed" else "rejected")
            } else if (stopped != null) null else result
            Envelope(request.requestId, sessionId, request.operation, failure == null, result = finalResult, error = failure)
        }
        events(envelope)
        return envelope
    }

    private fun checkLocked() {
        refreshStop()
        stopped?.let { throw AgentException(it) }
        if (ended) throw AgentException(AgentError(ErrorCode.STOPPED, phase))
    }

    private fun refreshStop() {
        if (ended || stopped != null) return
        lifecycleError()?.let {
            stopped = it
            cancellation.complete(Unit)
            return
        }
        val code = when {
            !runCatching(connectionAlive).getOrDefault(false) -> ErrorCode.DISCONNECTED
            clock() >= deadline -> ErrorCode.TIMEOUT
            else -> null
        }
        if (code != null) {
            stopped = AgentError(code, phase)
            cancellation.complete(Unit)
        }
    }
}
