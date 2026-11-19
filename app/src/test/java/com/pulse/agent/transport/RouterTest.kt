package com.pulse.agent.transport

import com.google.gson.JsonParser
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentProtocol
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import com.pulse.agent.protocol.ProtocolException
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.InputStream

class RouterTest {
    private val token = "t".repeat(40)
    private val tokens = TokenStore().apply { set(token) }
    private val expectedPeer = AdbPeer(2000, 1234)
    private val router = Router(tokens, protocolVersion = AgentProtocol.VERSION, agentVersion = "0.1.0", expectedPeer = expectedPeer)
    private val requestId = "123e4567-e89b-42d3-a456-426614174000"
    private val sessionId = "923e4567-e89b-42d3-a456-426614174001"
    private val routes = listOf(
        "GET" to "/v1/health",
        "POST" to "/v1/observe",
        "POST" to "/v1/action",
        "POST" to "/v1/cancel",
    )

    private fun req(
        method: String,
        path: String,
        auth: String? = "Bearer $token",
        body: String = "",
        contentType: String? = "application/json",
        length: Int = body.toByteArray(Charsets.UTF_8).size,
    ): String =
        buildString {
            append("$method $path HTTP/1.1\r\n")
            if (auth != null) append("Authorization: $auth\r\n")
            if (contentType != null) append("Content-Type: $contentType\r\n")
            append("Content-Length: $length\r\n\r\n")
            append(body)
        }

    private fun headers(raw: String): HttpRequest =
        HttpCodec.readHeaders(ByteArrayInputStream(raw.toByteArray(Charsets.UTF_8)))

    private fun authorizedRequest(path: String, body: String): HttpRequest {
        val input = ByteArrayInputStream(req("POST", path, body = body).toByteArray(Charsets.UTF_8))
        val head = HttpCodec.readHeaders(input)
        router.authorize(head, 2000, 1234)
        return HttpCodec.readBody(head, input)
    }

    private fun envelope(operation: String, params: Map<String, Any?>): String = Json.encode(
        mapOf("protocol_version" to 1, "request_id" to requestId, "operation" to operation, "params" to params),
    )

    private fun protocolFailure(
        status: Int,
        code: ErrorCode,
        phase: Phase,
        block: () -> Unit,
    ): ProtocolException {
        val error = assertThrows(ProtocolException::class.java) { block() }
        assertEquals(status, error.status)
        assertEquals(code, error.error.code)
        assertEquals(phase, error.error.phase)
        assertFalse(error.error.retryable)
        assertEquals(code.wire, error.message)
        return error
    }

    @Test fun healthWithToken() {
        val r = Router(tokens, 1, "0.1.0", expectedPeer) { mapOf("tree" to true, "screenshot" to false) }
        r.authorize(headers(req("GET", "/v1/health")), 2000, 1234)
        val response = r.health(sessionId)
        assertEquals(200, response.status)
        assertEquals("application/json; charset=utf-8", response.contentType)
        val body = JsonParser.parseString(response.body).asJsonObject
        assertEquals(
            setOf("protocol_version", "request_id", "session_id", "operation", "success", "terminal", "result", "error"),
            body.keySet(),
        )
        assertEquals(1, body["protocol_version"].asInt)
        assertTrue(AgentProtocol.validId(body["request_id"].asString))
        assertEquals(sessionId, body["session_id"].asString)
        assertEquals("health", body["operation"].asString)
        assertTrue(body["success"].asBoolean)
        assertTrue(body["terminal"].asBoolean)
        assertTrue(body["error"].isJsonNull)
        val result = body["result"].asJsonObject
        assertEquals(setOf("status", "agent_version", "capabilities"), result.keySet())
        assertEquals("ok", result["status"].asString)
        assertEquals("0.1.0", result["agent_version"].asString)
        assertTrue(result["capabilities"].asJsonObject["tree"].asBoolean)
        assertFalse(result["capabilities"].asJsonObject["screenshot"].asBoolean)
        assertFalse(body.has("schema_version"))
    }

    @Test fun healthAssignsOpaqueRequestIdsPerConnection() {
        val secondSession = "923e4567-e89b-42d3-a456-426614174002"
        val bodies = listOf(sessionId, secondSession).map { session ->
            router.authorize(headers(req("GET", "/v1/health")), 2000, 1234)
            JsonParser.parseString(router.health(session).body).asJsonObject.also {
                assertEquals(session, it["session_id"].asString)
                assertTrue(AgentProtocol.validId(it["request_id"].asString))
            }
        }
        assertNotEquals(bodies[0]["request_id"].asString, bodies[1]["request_id"].asString)
    }

    @Test fun missingTokenRejected() {
        routes.forEach { (method, path) ->
            router.authorize(headers(req(method, path)), 2000, 1234)
            repeat(2) {
                protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                    router.authorize(headers(req(method, path, auth = null)), 2000, 1234)
                }
            }
        }
    }

    @Test fun wrongTokenRejected() {
        routes.forEach { (method, path) ->
            router.authorize(headers(req(method, path)), 2000, 1234)
            listOf("Bearer nope", "Bearer ${"x".repeat(40)}", "bearer $token", "Basic $token", token).forEach { auth ->
                protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                    router.authorize(headers(req(method, path, auth = auth)), 2000, 1234)
                }
            }
        }
    }

    @Test fun unknownPathRequiresAuthBeforeRouting() {
        listOf("/nope", "/v1/action/$requestId", "/v1/logs").forEach { path ->
            protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                router.authorize(headers(req("GET", path, auth = null)), 2000, 1234)
            }
            protocolFailure(404, ErrorCode.NOT_FOUND, Phase.PROTOCOL) {
                router.authorize(headers(req("GET", path)), 2000, 1234)
            }
        }
    }

    @Test fun noTokenConfiguredRejectsEverything() {
        val r = Router(TokenStore(), 1, "x", expectedPeer)
        routes.forEach { (method, path) ->
            listOf(null, "Bearer ", "Bearer $token").forEach { auth ->
                repeat(2) {
                    protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                        r.authorize(headers(req(method, path, auth = auth)), 2000, 1234)
                    }
                }
            }
        }
    }

    @Test fun clearingTokenRevokesAllSubsequentConnections() {
        router.authorize(headers(req("GET", "/v1/health")), 2000, 1234)
        tokens.clear()
        routes.forEach { (method, path) ->
            repeat(2) {
                protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                    router.authorize(headers(req(method, path)), 2000, 1234)
                }
            }
        }
    }

    @Test fun rotatedTokenMustBePresentedOnEachConnection() {
        router.authorize(headers(req("GET", "/v1/health")), 2000, 1234)
        val replacement = "n".repeat(40)
        tokens.set(replacement)
        routes.forEach { (method, path) ->
            router.authorize(headers(req(method, path, auth = "Bearer $replacement")), 2000, 1234)
            protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                router.authorize(headers(req(method, path)), 2000, 1234)
            }
            protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                router.authorize(headers(req(method, path, auth = null)), 2000, 1234)
            }
        }
    }

    @Test fun shellAndRootPeersAreAllowedOnlyByTheirExplicitPin() {
        val rootPeer = AdbPeer(0, 1234)
        val rootRouter = Router(tokens, 1, "0.1.0", expectedPeer = rootPeer)
        listOf(expectedPeer to router, rootPeer to rootRouter).forEach { (peer, pinnedRouter) ->
            assertTrue(PeerPolicy.allowed(peer.uid, peer.pid, peer))
            pinnedRouter.verifyPeer(peer.uid, peer.pid)
            routes.forEach { (method, path) -> pinnedRouter.authorize(headers(req(method, path)), peer.uid, peer.pid) }
        }
    }

    @Test fun appSystemAndNegativePeerUidsAreDenied() {
        listOf(Int.MIN_VALUE, -1, 1, 1000, 1001, 1999, 2001, 9999, 10000, 10001, 101000, Int.MAX_VALUE).forEach { uid ->
            assertFalse("uid=$uid", PeerPolicy.allowed(uid, 1234, expectedPeer))
            routes.forEach { (method, path) ->
                protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                    router.authorize(headers(req(method, path)), uid, 1234)
                }
            }
        }
    }

    @Test fun sameShellUidDifferentPidIsRejectedOnEachConnection() {
        listOf(1, 1233, 1235, Int.MAX_VALUE).forEach { pid ->
            router.verifyPeer(2000, 1234)
            assertFalse(PeerPolicy.allowed(2000, pid, expectedPeer))
            protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) { router.verifyPeer(2000, pid) }
            routes.forEach { (method, path) ->
                router.authorize(headers(req(method, path)), 2000, 1234)
                repeat(2) {
                    protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                        router.authorize(headers(req(method, path)), 2000, pid)
                    }
                }
            }
        }
    }

    @Test fun sameRootUidDifferentPidIsRejectedByRootPinnedRouter() {
        val rootPeer = AdbPeer(0, 1234)
        val rootRouter = Router(tokens, 1, "0.1.0", expectedPeer = rootPeer)
        listOf(1, 1233, 1235, Int.MAX_VALUE).forEach { pid ->
            rootRouter.verifyPeer(0, 1234)
            assertFalse(PeerPolicy.allowed(0, pid, rootPeer))
            protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) { rootRouter.verifyPeer(0, pid) }
            routes.forEach { (method, path) ->
                rootRouter.authorize(headers(req(method, path)), 0, 1234)
                repeat(2) {
                    protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                        rootRouter.authorize(headers(req(method, path)), 0, pid)
                    }
                }
            }
        }
    }

    @Test fun uidMismatchAtPinnedPidIsRejectedIncludingShellRootCrossovers() {
        val rootPeer = AdbPeer(0, 1234)
        val rootRouter = Router(tokens, 1, "0.1.0", expectedPeer = rootPeer)
        listOf(expectedPeer to router, rootPeer to rootRouter).forEach { (peer, pinnedRouter) ->
            listOf(0, 2000, 1000, 10000, -1).filter { it != peer.uid }.forEach { uid ->
                pinnedRouter.verifyPeer(peer.uid, 1234)
                assertFalse(PeerPolicy.allowed(uid, 1234, peer))
                protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) { pinnedRouter.verifyPeer(uid, 1234) }
                routes.forEach { (method, path) ->
                    protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                        pinnedRouter.authorize(headers(req(method, path)), uid, 1234)
                    }
                }
            }
        }
    }

    @Test fun invalidKernelPeerIdentifiersAreRejectedBeforeAnyHeaderOrBodyRead() {
        val rootPeer = AdbPeer(0, 1234)
        val rootRouter = Router(tokens, 1, "0.1.0", expectedPeer = rootPeer)
        listOf(expectedPeer to router, rootPeer to rootRouter).forEach { (peer, pinnedRouter) ->
            val invalidPeers = listOf(peer.uid to 0, peer.uid to -1, peer.uid to Int.MIN_VALUE, -1 to 1234, Int.MIN_VALUE to 1234)
            invalidPeers.forEach { (uid, pid) ->
                val input = BodyGuardInput(req("POST", "/v1/action", length = 512))
                val error = protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                    pinnedRouter.verifyPeer(uid, pid)
                    val head = HttpCodec.readHeaders(input)
                    pinnedRouter.authorize(head, uid, pid)
                    HttpCodec.readBody(head, input)
                }
                assertNull(error.requestId)
                assertNull(error.operation)
                assertEquals(0, input.bytesRead)
                assertEquals(0, input.bodyReadAttempts)
            }
        }
    }

    @Test fun kernelIdentityMismatchIsRejectedBeforeAnyHeaderOrBodyRead() {
        val rootPeer = AdbPeer(0, 1234)
        val rootRouter = Router(tokens, 1, "0.1.0", expectedPeer = rootPeer)
        listOf(expectedPeer to router, rootPeer to rootRouter).forEach { (peer, pinnedRouter) ->
            val otherUid = if (peer.uid == 2000) 0 else 2000
            listOf(peer.uid to 1235, otherUid to 1234, 10000 to 1234).forEach { (uid, pid) ->
                val input = BodyGuardInput(req("POST", "/v1/action", length = HttpCodec.MAX_BODY_BYTES + 1))
                protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                    pinnedRouter.verifyPeer(uid, pid)
                    val head = HttpCodec.readHeaders(input)
                    pinnedRouter.authorize(head, uid, pid)
                    HttpCodec.readBody(head, input)
                }
                assertEquals(0, input.bytesRead)
                assertEquals(0, input.bodyReadAttempts)
            }
        }
    }

    @Test fun claimedPeerHeadersCannotOverrideKernelIdentity() {
        val head = headers(req("GET", "/v1/health")).let {
            it.copy(headers = it.headers + mapOf("x-adb-uid" to "2000", "x-adb-pid" to "1234"))
        }
        router.authorize(head, 2000, 1234)
        listOf(2000 to 1235, 0 to 1234, 10000 to 1234).forEach { (uid, pid) ->
            protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) { router.authorize(head, uid, pid) }
        }
    }

    @Test fun peerRejectionPrecedesTokenAndRouteValidation() {
        val unconfigured = Router(TokenStore(), 1, "x", expectedPeer)
        listOf(10000 to 1234, 2000 to 1235, 0 to 1234).forEach { (uid, pid) ->
            protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                unconfigured.authorize(headers(req("POST", "/nope", auth = null, contentType = null)), uid, pid)
            }
        }
    }

    @Test fun authenticationPrecedesMethodAndContentTypeValidation() {
        routes.forEach { (method, path) ->
            val wrongMethod = if (method == "GET") "POST" else "GET"
            protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                router.authorize(headers(req(wrongMethod, path, auth = null, contentType = null)), 2000, 1234)
            }
        }
    }

    @Test fun methodsMustMatchAuthenticatedRoutes() {
        routes.forEach { (method, path) ->
            val wrongMethod = if (method == "GET") "POST" else "GET"
            protocolFailure(405, ErrorCode.METHOD_NOT_ALLOWED, Phase.PROTOCOL) {
                router.authorize(headers(req(wrongMethod, path)), 2000, 1234)
            }
        }
    }

    @Test fun postRoutesRequireSupportedJsonContentType() {
        routes.filter { it.first == "POST" }.forEach { (method, path) ->
            listOf("application/json", "application/json; charset=utf-8").forEach { type ->
                router.authorize(headers(req(method, path, contentType = type)), 2000, 1234)
            }
            listOf(null, "text/plain", "application/octet-stream", "application/json; charset=iso-8859-1").forEach { type ->
                protocolFailure(400, ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL) {
                    router.authorize(headers(req(method, path, contentType = type)), 2000, 1234)
                }
            }
        }
        router.authorize(headers(req("GET", "/v1/health", contentType = null)), 2000, 1234)
    }

    @Test fun preBodyAuthenticationDoesNotReadEvenAnOversizedBody() {
        routes.forEach { (method, path) ->
            listOf(null, "Bearer wrong").forEach { auth ->
                val rawHead = req(method, path, auth = auth, length = HttpCodec.MAX_BODY_BYTES + 1)
                val input = BodyGuardInput(rawHead)
                val head = HttpCodec.readHeaders(input)
                assertArrayEquals(ByteArray(0), head.body)
                protocolFailure(401, ErrorCode.UNAUTHORIZED, Phase.AUTH) {
                    router.authorize(head, 2000, 1234)
                    HttpCodec.readBody(head, input)
                }
                assertEquals(rawHead.toByteArray(Charsets.UTF_8).size, input.bytesRead)
                assertEquals(0, input.bodyReadAttempts)
            }
        }
    }

    @Test fun preBodyPeerRejectionDoesNotReadBody() {
        listOf(10000 to 1234, 2000 to 1235, 0 to 1234, 2000 to 0, 2000 to -1).forEach { (uid, pid) ->
            val rawHead = req("POST", "/v1/action", length = 512)
            val input = BodyGuardInput(rawHead)
            val head = HttpCodec.readHeaders(input)
            protocolFailure(403, ErrorCode.FORBIDDEN_PEER, Phase.AUTH) {
                router.authorize(head, uid, pid)
                HttpCodec.readBody(head, input)
            }
            assertEquals(rawHead.toByteArray(Charsets.UTF_8).size, input.bytesRead)
            assertEquals(0, input.bodyReadAttempts)
        }
    }

    @Test fun observeRouteParsesRealAgentEnvelope() {
        val request = router.parse(authorizedRequest("/v1/observe", envelope("observe", mapOf("screenshot" to true))))
        assertEquals(requestId, request.requestId)
        assertEquals("observe", request.operation)
        assertEquals(5000L, request.timeoutMs)
        assertTrue(checkNotNull(request.observe).tree)
        assertTrue(checkNotNull(request.observe).screenshot)
        assertFalse(checkNotNull(request.observe).ocr)
        assertNull(request.action)
    }

    @Test fun actionRouteParsesRealPrimitiveNotAStub() {
        val request = router.parse(authorizedRequest("/v1/action", envelope("action", mapOf(
            "kind" to "tap", "expected_package" to "com.example.app", "x" to 10, "y" to 20,
        ))))
        val action = checkNotNull(request.action)
        assertEquals(requestId, request.requestId)
        assertEquals("action", request.operation)
        assertEquals("tap", action.kind)
        assertEquals("com.example.app", action.expectedPackage)
        assertEquals(10, action.x)
        assertEquals(20, action.y)
        assertEquals(80L, action.durationMs)
        assertNull(request.observe)
    }

    @Test fun cancelRouteParsesScopedTargetIds() {
        val targetId = "123e4567-e89b-42d3-a456-426614174003"
        val request = router.parse(authorizedRequest("/v1/cancel", envelope("cancel", mapOf(
            "target_session_id" to sessionId, "target_request_id" to targetId,
        ))))
        assertEquals(requestId, request.requestId)
        assertEquals("cancel", request.operation)
        assertEquals(sessionId, request.targetSessionId)
        assertEquals(targetId, request.targetRequestId)
        assertNull(request.observe)
        assertNull(request.action)
    }

    @Test fun operationMustMatchTheAuthenticatedPath() {
        val operations = listOf("observe", "action", "cancel")
        operations.forEach { pathOperation ->
            operations.filter { it != pathOperation }.forEach { bodyOperation ->
                val error = protocolFailure(400, ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL) {
                    router.parse(authorizedRequest("/v1/$pathOperation", envelope(bodyOperation, emptyMap())))
                }
                assertEquals(requestId, error.requestId)
                assertNull(error.operation)
            }
        }
    }

    @Test fun routerAcceptsOnlyAgentProtocolVersionOne() {
        assertEquals(1, AgentProtocol.VERSION)
        listOf(-1, 0, 2).forEach { version ->
            assertThrows(IllegalArgumentException::class.java) { Router(tokens, version, "x", expectedPeer) }
        }
    }

    @Test fun routerErrorIsStructuredAndTerminal() {
        val response = router.error(401, AgentError(ErrorCode.UNAUTHORIZED, Phase.AUTH))
        assertEquals(401, response.status)
        val body = JsonParser.parseString(response.body).asJsonObject
        assertEquals(1, body["protocol_version"].asInt)
        assertFalse(body["success"].asBoolean)
        assertTrue(body["terminal"].asBoolean)
        listOf("request_id", "session_id", "operation", "result").forEach { assertTrue(body[it].isJsonNull) }
        val error = body["error"].asJsonObject
        assertEquals(setOf("code", "phase", "retryable"), error.keySet())
        assertEquals("unauthorized", error["code"].asString)
        assertEquals("auth", error["phase"].asString)
        assertFalse(error["retryable"].asBoolean)
        assertFalse(response.body.contains(token))
    }

    @Test fun actionProtocolErrorIsRejectedNotDispatchedOrCompleted() {
        val response = router.error(400, AgentError(ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL), requestId, "action")
        assertEquals(400, response.status)
        val body = JsonParser.parseString(response.body).asJsonObject
        assertEquals(requestId, body["request_id"].asString)
        assertEquals("action", body["operation"].asString)
        assertFalse(body["success"].asBoolean)
        assertTrue(body["terminal"].asBoolean)
        assertEquals("rejected", body["result"].asJsonObject["status"].asString)
        assertFalse(response.body.contains("dispatched"))
        assertFalse(response.body.contains("completed"))
    }

    private class BodyGuardInput(rawHead: String) : InputStream() {
        private val headers = ByteArrayInputStream(rawHead.toByteArray(Charsets.UTF_8))
        var bytesRead = 0
            private set
        var bodyReadAttempts = 0
            private set

        override fun read(): Int {
            if (headers.available() == 0) {
                bodyReadAttempts++
                throw AssertionError("body read before authorization")
            }
            bytesRead++
            return headers.read()
        }

        override fun read(bytes: ByteArray, offset: Int, length: Int): Int {
            bodyReadAttempts++
            throw AssertionError("body read before authorization")
        }
    }
}
