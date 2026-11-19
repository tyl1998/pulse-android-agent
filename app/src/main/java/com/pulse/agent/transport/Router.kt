package com.pulse.agent.transport

import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentProtocol
import com.pulse.agent.protocol.AgentRequest
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import com.pulse.agent.protocol.ProtocolException
import java.util.UUID

data class AdbPeer(val uid: Int, val pid: Int) {
    init { require(uid in setOf(0, 2000) && pid > 0) }
}

object PeerPolicy {
    fun parse(value: String): AdbPeer {
        require(value.length <= 32 && Regex("(0|2000):[1-9][0-9]{0,9}\\n?").matches(value))
        val fields = value.trimEnd('\n').split(':')
        return AdbPeer(fields[0].toInt(), fields[1].toInt())
    }

    // The UID profile alone is insufficient: shell commands do not have adbd's pinned PID.
    fun allowed(uid: Int, pid: Int, expected: AdbPeer): Boolean = uid == expected.uid && pid == expected.pid
}

class Router(
    private val tokens: TokenStore,
    protocolVersion: Int,
    private val agentVersion: String,
    private val expectedPeer: AdbPeer,
    private val capabilities: () -> Map<String, Any?> = { emptyMap() },
) {
    init { require(protocolVersion == AgentProtocol.VERSION) }

    /** Called after bounded headers, before allocating or reading the body. */
    fun verifyPeer(peerUid: Int, peerPid: Int) {
        if (!PeerPolicy.allowed(peerUid, peerPid, expectedPeer)) throw ProtocolException(403, AgentError(ErrorCode.FORBIDDEN_PEER, Phase.AUTH))
    }

    fun authorize(head: HttpRequest, peerUid: Int, peerPid: Int) {
        verifyPeer(peerUid, peerPid)
        if (!tokens.verify(head.header("authorization"))) throw ProtocolException(401, AgentError(ErrorCode.UNAUTHORIZED, Phase.AUTH))
        val method = when (head.path) {
            "/v1/health" -> "GET"
            "/v1/observe", "/v1/action", "/v1/cancel" -> "POST"
            else -> throw ProtocolException(404, AgentError(ErrorCode.NOT_FOUND, Phase.PROTOCOL))
        }
        if (head.method != method) throw ProtocolException(405, AgentError(ErrorCode.METHOD_NOT_ALLOWED, Phase.PROTOCOL))
        if (method == "POST" && head.header("content-type") !in setOf("application/json", "application/json; charset=utf-8")) {
            throw ProtocolException(400, AgentError(ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL))
        }
    }

    fun parse(request: HttpRequest): AgentRequest = AgentProtocol.parse(request.body, request.path.removePrefix("/v1/"))

    fun health(sessionId: String): HttpResponse = HttpResponse(200, Envelope(
        UUID.randomUUID().toString(), sessionId, "health", true,
        result = mapOf("status" to "ok", "agent_version" to agentVersion, "capabilities" to capabilities()),
    ).encode())

    fun error(status: Int, error: AgentError, requestId: String? = null, operation: String? = null): HttpResponse =
        HttpResponse(status, Envelope(requestId, null, operation, false, result = if (operation == "action") mapOf("status" to "rejected") else null, error = error).encode())
}
