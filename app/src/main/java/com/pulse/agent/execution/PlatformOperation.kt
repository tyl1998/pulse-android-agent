package com.pulse.agent.execution

import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** Shared lease survives backend/session replacement; only its exact owner can release it. */
class PlatformOperation<T>(
    private val lease: AtomicReference<Any?>,
    private val recovery: AtomicReference<Any?>,
    private val owner: AtomicReference<PlatformOperation<*>?>,
    private val post: (Runnable) -> Boolean,
    private val remove: (Runnable) -> Unit,
) {
    val result = CompletableFuture<T>()
    private val lock = Any()
    private val used = AtomicBoolean(false)
    private val enqueued = AtomicBoolean(false)
    private var started = false
    private var aborted = false
    private var resolved = false
    private var runnable: Runnable? = null
    @Volatile var actionInvoked = false

    fun acquire(): Boolean = synchronized(lock) {
        if (aborted || resolved || !used.compareAndSet(false, true) || !lease.compareAndSet(null, this)) return false
        owner.set(this)
        true
    }

    fun enqueue(scope: RequestScope, start: (PlatformOperation<T>) -> Unit) {
        val job = Runnable {
            if (!begin()) return@Runnable
            try { scope.checkActive(); start(this) }
            catch (e: AgentException) { fail(e) }
            catch (_: Exception) { fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, scope.phase))) }
        }
        synchronized(lock) {
            if (aborted || lease.get() !== this || !enqueued.compareAndSet(false, true)) return
            runnable = job
            try {
                if (!post(job)) { runnable = null; fail(AgentException(AgentError(ErrorCode.STOPPED, Phase.LIFECYCLE))) }
            } catch (_: Exception) {
                runnable = null
                fail(AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.LIFECYCLE)))
            }
        }
    }

    private fun begin(): Boolean = synchronized(lock) {
        runnable = null
        if (aborted) { release(); false }
        else if (started || resolved || lease.get() !== this || result.isDone) false
        else { started = true; true }
    }

    fun abortBeforeStart() {
        var removed = true
        synchronized(lock) {
            if (started || aborted) return
            aborted = true
            runnable?.let { job ->
                try { remove(job) } catch (_: Exception) { removed = false }
            }
            runnable = null
        }
        val error = AgentException(AgentError(ErrorCode.CANCELLED, Phase.LIFECYCLE))
        if (removed) fail(error)
        else {
            // Keep admission closed until the unremovable job is dequeued and suppressed.
            synchronized(lock) { resolved = true }
            result.completeExceptionally(error)
        }
    }

    private fun release() {
        recovery.compareAndSet(this, null)
        owner.compareAndSet(this, null)
        lease.compareAndSet(this, null)
    }

    fun complete(value: T) {
        val publish = synchronized(lock) {
            release()
            if (resolved) false else { resolved = true; true }
        }
        if (publish) result.complete(value)
    }

    fun quarantine(value: T) {
        val publish = synchronized(lock) {
            if (resolved) false else {
                if (lease.get() === this) recovery.compareAndSet(null, this)
                resolved = true
                true
            }
        }
        if (publish) result.complete(value)
    }

    fun fail(error: AgentException) {
        val failure = synchronized(lock) {
            if (resolved) return
            resolved = true
            if (actionInvoked) {
                if (lease.get() === this) recovery.compareAndSet(null, this)
                AgentException(AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION))
            } else { release(); error }
        }
        result.completeExceptionally(failure)
    }
}
