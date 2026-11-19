package com.pulse.agent.protocol

import com.pulse.agent.transport.Json
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.lang.reflect.Modifier

class SafeLogTest {
    private val requestId = "123e4567-e89b-42d3-a456-426614174000"
    private val error = AgentError(ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL)

    private fun redacted(candidate: String?, vararg secrets: String) {
        val log = SafeLog.error(candidate, error)
        assertEquals("request_id=unassigned phase=protocol category=invalid_message", log)
        secrets.forEach { secret -> assertFalse(log.contains(secret)) }
        assertFalse(log.contains('\n'))
        assertFalse(log.contains('\r'))
    }

    @Test fun loggingBoundaryAcceptsOnlyAnIdAndClosedTypedError() {
        val methods = SafeLog::class.java.declaredMethods.filter { Modifier.isPublic(it.modifiers) && !it.isSynthetic }
        assertEquals(1, methods.size)
        val method = methods.single()
        assertEquals("error", method.name)
        assertEquals(String::class.java, method.returnType)
        assertArrayEquals(arrayOf(String::class.java, AgentError::class.java), method.parameterTypes)
        val errorFields = AgentError::class.java.declaredFields.filter { !Modifier.isStatic(it.modifiers) && !it.isSynthetic }
        assertEquals(setOf(ErrorCode::class.java, Phase::class.java, java.lang.Boolean.TYPE), errorFields.map { it.type }.toSet())
        assertTrue(Phase::class.java.isEnum)
        assertTrue(ErrorCode::class.java.isEnum)
    }

    @Test fun canonicalOpaqueIdIsTheOnlyCallerStringInLog() {
        assertEquals("request_id=$requestId phase=protocol category=invalid_message", SafeLog.error(requestId, error))
    }

    @Test fun nullMalformedUppercaseOrInjectedIdIsUnassigned() {
        listOf(null, "", "not-a-uuid", requestId.uppercase(), " $requestId", "$requestId ", "{$requestId}", "$requestId\nphase=secret", "$requestId/secret").forEach { id ->
            redacted(id, "secret")
        }
    }

    @Test fun payloadCannotAppearEvenWhenOfferedAsRequestId() {
        val secret = "private-payload-should-never-be-logged"
        val payload = Json.encode(mapOf("request_id" to requestId, "params" to mapOf("payload" to secret)))
        redacted(payload, secret, "params", "payload", requestId)
    }

    @Test fun tokenCannotAppearEvenWhenOfferedAsRequestId() {
        val token = "secret-token-" + "t".repeat(40)
        redacted("Bearer $token", token, "Bearer")
        redacted(token, token)
    }

    @Test fun inputCannotAppearEvenThroughNewlineInjection() {
        val input = "private-password-input\u4e2d\u6587"
        redacted(input, input, "private-password")
        redacted("$requestId\ninput=$input", input, requestId, "input=")
    }

    @Test fun screenshotCannotAppearEvenWhenOfferedAsRequestId() {
        val screenshot = "data:image/png;base64,iVBOR-private-screenshot-bitmap"
        redacted(screenshot, screenshot, "image/png", "private-screenshot")
    }

    @Test fun throwableMessageCannotAppearAtTypedLoggingBoundary() {
        val failure = IllegalStateException("private-throwable-token-input-screenshot")
        redacted(failure.message, checkNotNull(failure.message), "IllegalStateException")
        redacted(failure.toString(), checkNotNull(failure.message), "IllegalStateException")
        assertEquals(ErrorCode.INVALID_MESSAGE.wire, AgentException(error).message)
        assertEquals(ErrorCode.INVALID_MESSAGE.wire, ProtocolException(400, error).message)
    }

    @Test fun everyPhaseAndErrorCodeUsesOnlyClosedEnumWireValues() {
        assertEquals(Phase.entries.size, Phase.entries.map { it.wire }.toSet().size)
        assertEquals(ErrorCode.entries.size, ErrorCode.entries.map { it.wire }.toSet().size)
        Phase.entries.forEach { phase ->
            assertTrue(Regex("[a-z_]+").matches(phase.wire))
            ErrorCode.entries.forEach { code ->
                assertTrue(Regex("[a-z_]+").matches(code.wire))
                assertEquals("request_id=$requestId phase=${phase.wire} category=${code.wire}", SafeLog.error(requestId, AgentError(code, phase)))
            }
        }
    }

    @Test fun retryabilityCannotIntroduceExtraLogFields() {
        assertEquals(SafeLog.error(requestId, error), SafeLog.error(requestId, error.copy(retryable = true)))
        assertFalse(SafeLog.error(requestId, error.copy(retryable = true)).contains("retryable"))
    }
}
