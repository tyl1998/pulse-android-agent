package com.pulse.agent.transport

import android.net.LocalServerSocket
import android.net.LocalSocket
import android.os.SystemClock
import android.os.Looper
import android.system.Os
import android.system.OsConstants
import android.system.StructPollfd
import android.util.Log
import com.pulse.agent.execution.DeviceCoordinator
import com.pulse.agent.execution.RequestScope
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import com.pulse.agent.protocol.ProtocolException
import com.pulse.agent.protocol.SafeLog
import java.io.IOException
import java.util.UUID
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CompletableFuture
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 监听 abstract 命名空间 `pulse-<instance>`，主机经
 * `adb forward tcp:<port> localabstract:pulse-<instance>` 访问；不监听任何网卡。
 */
class LocalSocketServer(
    instance: String,
    private val router: Router,
    private val coordinator: DeviceCoordinator,
    private val errorRecord: (String) -> Unit = {},
) {
    val socketName: String = "pulse-$instance"

    init {
        require(INSTANCE_PATTERN.matches(instance)) { "invalid instance id" }
    }

    private val workers = ThreadPoolExecutor(
        0, MAX_CONNECTIONS, 30, TimeUnit.SECONDS, SynchronousQueue<Runnable>(),
    ) { r -> Thread(r, "pulse-conn").apply { isDaemon = true } }
    private val deadlines = ScheduledThreadPoolExecutor(1) { r -> Thread(r, "pulse-io-deadline").apply { isDaemon = true } }
        .apply { removeOnCancelPolicy = true; executeExistingDelayedTasksAfterShutdownPolicy = false }
    private val clients = ConcurrentHashMap.newKeySet<Connection>()
    private var started = false
    private var stopped = false
    private val acceptExited = CompletableFuture<Unit>()

    @Volatile private var server: LocalServerSocket? = null
    private var acceptThread: Thread? = null

    @Synchronized
    fun start() {
        check(!started && !stopped) { "server is single-use" }
        teardownFences[socketName]?.get(1000, TimeUnit.MILLISECONDS)
        val s = LocalServerSocket(socketName)
        started = true
        server = s
        acceptThread = Thread({
            try { acceptLoop(s) }
            finally { acceptExited.complete(Unit); teardownFences.remove(socketName, acceptExited) }
        }, "pulse-accept").apply { isDaemon = true; start() }
    }

    @Synchronized
    fun stop() {
        if (stopped) return
        stopped = true
        val s = server
        server = null
        if (started) teardownFences[socketName] = acceptExited
        if (s != null) try { Os.shutdown(s.fileDescriptor, OsConstants.SHUT_RDWR) } catch (_: Exception) {}
        try { s?.close() } catch (_: IOException) {}
        clients.forEach { it.close(ErrorCode.STOPPED) }
        coordinator.close()
        val accepting = acceptThread
        if (accepting == null || !accepting.isAlive) {
            acceptExited.complete(Unit)
            teardownFences.remove(socketName, acceptExited)
        }
        accepting?.interrupt()
        acceptThread = null
        workers.shutdownNow()
        if (accepting != null && Looper.myLooper() == Looper.getMainLooper()) {
            try {
                deadlines.execute {
                    try { accepting.join(1000) } catch (_: InterruptedException) { Thread.currentThread().interrupt() }
                }
            } catch (_: RejectedExecutionException) {}
        } else try { accepting?.join(1000) } catch (_: InterruptedException) { Thread.currentThread().interrupt() }
        deadlines.shutdown()
    }

    private fun acceptLoop(s: LocalServerSocket) {
        while (server === s) {
            val client = try { s.accept() } catch (e: IOException) {
                if (server === s) log(null, AgentError(ErrorCode.PLATFORM_FAILED, Phase.TRANSPORT))
                break
            }
            if (runCatching {
                val peer = client.peerCredentials
                router.verifyPeer(peer.uid, peer.pid)
            }.isFailure) {
                log(null, AgentError(ErrorCode.FORBIDDEN_PEER, Phase.AUTH))
                try { client.close() } catch (_: IOException) {}
                continue
            }
            val connection = Connection(client, s)
            if (!register(connection, s)) { connection.close(); continue }
            try {
                workers.execute { handle(connection) }
            } catch (_: RejectedExecutionException) {
                // No worker is allocated just to read/authenticate an excess connection.
                connection.close()
                clients.remove(connection)
            }
        }
    }

    @Synchronized
    private fun register(connection: Connection, expected: LocalServerSocket): Boolean {
        if (server !== expected || stopped || clients.size >= MAX_CONNECTIONS) return false
        clients.add(connection)
        return true
    }

    private fun handle(connection: Connection) {
        var timer: ScheduledFuture<*>? = null
        var streaming = false
        val client = connection.socket
        try {
            client.soTimeout = IO_TIMEOUT_MS
            val peer = client.peerCredentials
            router.verifyPeer(peer.uid, peer.pid)
            val readDeadline = connection.readDeadline
            timer = deadlines.schedule({ connection.close(ErrorCode.TIMEOUT) }, (readDeadline - SystemClock.elapsedRealtimeNanos()).coerceAtLeast(0), TimeUnit.NANOSECONDS)
            val checkDeadline = {
                if (SystemClock.elapsedRealtimeNanos() >= readDeadline) throw ProtocolException(408, AgentError(ErrorCode.TIMEOUT, Phase.TRANSPORT))
            }
            val head = HttpCodec.readHeaders(client.inputStream, checkDeadline)
            router.authorize(head, peer.uid, peer.pid)
            val message = HttpCodec.readBody(head, client.inputStream, checkDeadline)
            timer.cancel(false)
            timer = deadlines.schedule({ connection.close(ErrorCode.TIMEOUT) }, IO_TIMEOUT_MS.toLong(), TimeUnit.MILLISECONDS)
            if (message.path == "/v1/health") {
                HttpCodec.writeResponse(client.outputStream, router.health(connection.id))
                return
            }
            val request = router.parse(message)
            if (request.operation == "cancel") {
                val cancelled = coordinator.cancel(checkNotNull(request.targetSessionId), checkNotNull(request.targetRequestId))
                HttpCodec.writeResponse(client.outputStream, HttpResponse(200, Envelope(
                    request.requestId, connection.id, "cancel", true, result = mapOf("cancelled" to cancelled),
                ).encode()))
                return
            }
            timer.cancel(false)
            val events = ArrayBlockingQueue<Envelope>(2)
            val scope = RequestScope(connection.id, request, connection::alive, events::offer, SystemClock::elapsedRealtimeNanos)
            connection.scope = scope
            timer = deadlines.schedule({ connection.close(ErrorCode.TIMEOUT) }, request.timeoutMs + IO_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            coordinator.submit(scope)
            HttpCodec.writeStreamHeaders(client.outputStream, connection.id)
            streaming = true
            while (connection.alive()) {
                val event = events.poll(25, TimeUnit.MILLISECONDS) ?: continue
                HttpCodec.writeEvent(client.outputStream, event.encode())
                if (event.terminal) break
            }
        } catch (e: ProtocolException) {
            log(e.requestId, e.error)
            if (!streaming) respondError(client, router.error(e.status, e.error, e.requestId, e.operation))
        } catch (e: HttpParseException) {
            val error = AgentError(if (e.status in setOf(413, 431)) ErrorCode.MESSAGE_TOO_LARGE else ErrorCode.INVALID_MESSAGE, Phase.TRANSPORT)
            log(connection.scope?.request?.requestId, error)
            if (!streaming) respondError(client, router.error(e.status, error))
        } catch (_: IOException) {
            log(connection.scope?.request?.requestId, AgentError(ErrorCode.DISCONNECTED, Phase.TRANSPORT))
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            log(connection.scope?.request?.requestId, AgentError(ErrorCode.STOPPED, Phase.LIFECYCLE))
        } catch (_: Exception) {
            val error = AgentError(ErrorCode.INTERNAL_ERROR, Phase.TRANSPORT)
            log(connection.scope?.request?.requestId, error)
            if (!streaming) respondError(client, router.error(500, error))
        } finally {
            timer?.cancel(false)
            connection.close()
            clients.remove(connection)
        }
    }

    private fun respondError(client: LocalSocket, response: HttpResponse) {
        try { HttpCodec.writeResponse(client.outputStream, response) } catch (_: Exception) {}
    }

    private inner class Connection(val socket: LocalSocket, private val listener: LocalServerSocket) {
        val id: String = UUID.randomUUID().toString()
        val readDeadline: Long = SystemClock.elapsedRealtimeNanos() + TimeUnit.MILLISECONDS.toNanos(READ_DEADLINE_MS)
        private val open = AtomicBoolean(true)
        @Volatile var scope: RequestScope? = null

        fun alive(): Boolean {
            if (!open.get() || server !== listener) return false
            return try {
                // After the one body, readable input means EOF or forbidden pipelining.
                val poll = StructPollfd().apply { fd = socket.fileDescriptor; events = OsConstants.POLLIN.toShort() }
                Os.poll(arrayOf(poll), 0)
                poll.revents.toInt() and (OsConstants.POLLIN or OsConstants.POLLHUP or OsConstants.POLLERR or OsConstants.POLLNVAL) == 0
            } catch (_: Exception) { false }
        }

        fun close(code: ErrorCode = ErrorCode.DISCONNECTED) {
            if (!open.compareAndSet(true, false)) return
            try { Os.shutdown(socket.fileDescriptor, OsConstants.SHUT_RDWR) } catch (_: Exception) {}
            try { socket.close() } catch (_: IOException) {}
            if (code == ErrorCode.TIMEOUT) log(scope?.request?.requestId, AgentError(code, Phase.TRANSPORT))
            if (code == ErrorCode.DISCONNECTED) coordinator.disconnect(id) else scope?.cancel(code)
            scope?.cancel(code)
        }
    }

    private fun log(requestId: String?, error: AgentError) {
        val line = SafeLog.error(requestId, error)
        Log.e(TAG, line)
        errorRecord(line)
    }

    private companion object {
        const val MAX_CONNECTIONS = 4
        const val IO_TIMEOUT_MS = 2000
        const val READ_DEADLINE_MS = 2000L
        val teardownFences = ConcurrentHashMap<String, CompletableFuture<Unit>>()
        const val TAG = "PulseLocalSocket"
        val INSTANCE_PATTERN = Regex("[A-Za-z0-9_-]{1,64}")
    }
}
