package com.pulse.agent.protocol

import com.google.gson.JsonParser
import com.pulse.agent.transport.Json
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test

class AgentProtocolTest {
    private val requestId = "123e4567-e89b-42d3-a456-426614174000"
    private val sessionId = "923e4567-e89b-42d3-a456-426614174001"
    private val targetId = "123e4567-e89b-42d3-a456-426614174002"

    private fun fields(operation: String = "observe", params: Any? = emptyMap<String, Any?>()): Map<String, Any?> =
        mapOf("protocol_version" to 1, "request_id" to requestId, "operation" to operation, "params" to params)

    private fun body(
        operation: String = "observe",
        params: Any? = emptyMap<String, Any?>(),
        changes: Map<String, Any?> = emptyMap(),
    ): ByteArray = Json.encode(fields(operation, params) + changes).toByteArray(Charsets.UTF_8)

    private fun failure(
        bytes: ByteArray,
        operation: String = "observe",
        status: Int = 400,
        code: ErrorCode = ErrorCode.INVALID_MESSAGE,
    ): ProtocolException {
        val error = assertThrows(ProtocolException::class.java) { AgentProtocol.parse(bytes, operation) }
        assertEquals(status, error.status)
        assertEquals(code, error.error.code)
        assertEquals(Phase.PROTOCOL, error.error.phase)
        assertFalse(error.error.retryable)
        assertEquals(code.wire, error.message)
        return error
    }

    private fun actionParams(kind: String = "tap"): Map<String, Any?> {
        val common = mapOf("kind" to kind, "expected_package" to "com.example.app")
        return common + when (kind) {
            "tap" -> mapOf("x" to 10, "y" to 20)
            "swipe" -> mapOf("x" to 10, "y" to 20, "end_x" to 30, "end_y" to 40)
            "node_click" -> mapOf("node_path" to listOf(0, 1))
            "set_text" -> mapOf("node_path" to listOf(0, 1), "text" to "hello")
            else -> emptyMap()
        }
    }

    private fun action(changes: Map<String, Any?> = emptyMap(), kind: String = "tap"): ActionParams =
        checkNotNull(AgentProtocol.parse(body("action", actionParams(kind) + changes), "action").action)

    private fun invalidAction(params: Any?): ProtocolException = failure(body("action", params), "action").also {
        assertEquals(requestId, it.requestId)
        assertEquals("action", it.operation)
    }

    @Test fun observeDefaultsAreAnAgentRequestNotADslSchema() {
        val request = AgentProtocol.parse(body(), "observe")
        assertEquals(1, AgentProtocol.VERSION)
        assertEquals(requestId, request.requestId)
        assertEquals("observe", request.operation)
        assertEquals(5000L, request.timeoutMs)
        assertEquals(ObserveParams(tree = true, screenshot = false, ocr = false), request.observe)
        assertNull(request.action)
        assertNull(request.targetSessionId)
        assertNull(request.targetRequestId)
    }

    @Test fun allNonemptyObservationFlagCombinationsAreAccepted() {
        for (flags in 1..7) {
            val params = mapOf("tree" to (flags and 1 != 0), "screenshot" to (flags and 2 != 0), "ocr" to (flags and 4 != 0))
            val observe = checkNotNull(AgentProtocol.parse(body(params = params), "observe").observe)
            assertEquals(flags and 1 != 0, observe.tree)
            assertEquals(flags and 2 != 0, observe.screenshot)
            assertEquals(flags and 4 != 0, observe.ocr)
        }
    }

    @Test fun observationCannotRequestNothing() {
        failure(body(params = mapOf("tree" to false, "screenshot" to false, "ocr" to false)))
    }

    @Test fun observationFlagsRequireBooleanPrimitives() {
        listOf("tree", "screenshot", "ocr").forEach { key ->
            listOf(null, "true", 1, 0, emptyList<Any>(), emptyMap<String, Any?>()).forEach { value ->
                failure(body(params = mapOf(key to value)))
            }
        }
    }

    @Test fun requiredEnvelopeFieldsCannotBeOmitted() {
        listOf("protocol_version", "request_id", "operation", "params").forEach { key ->
            failure(Json.encode(fields() - key).toByteArray(Charsets.UTF_8))
        }
    }

    @Test fun envelopeAndParamsMustBeObjects() {
        listOf("null", "[]", "true", "1", "\"observe\"").forEach { raw -> failure(raw.toByteArray(Charsets.UTF_8)) }
        listOf(null, emptyList<Any>(), "params", 1, true).forEach { params -> failure(body(params = params)) }
    }

    @Test fun requestIdMustBeCanonicalLowercaseUuid() {
        listOf(requestId, "00000000-0000-0000-0000-000000000000").forEach { id ->
            assertTrue(AgentProtocol.validId(id))
            assertEquals(id, AgentProtocol.parse(body(changes = mapOf("request_id" to id)), "observe").requestId)
        }
        listOf(
            "", "not-a-uuid", requestId.uppercase(), " $requestId", "$requestId ", "{$requestId}",
            requestId.replace("-", ""), "1-1-1-1-1", requestId.dropLast(1), "$requestId\n",
            requestId.replace('-', '\u2010'), "$requestId-private-input",
        ).forEach { id ->
            assertFalse(AgentProtocol.validId(id))
            val error = failure(body(changes = mapOf("request_id" to id)))
            assertNull(error.requestId)
        }
        assertFalse(AgentProtocol.validId(null))
    }

    @Test fun requestIdCannotBeNullNumberBooleanArrayOrObject() {
        listOf(null, 123, true, emptyList<Any>(), mapOf("id" to requestId)).forEach { id ->
            assertNull(failure(body(changes = mapOf("request_id" to id))).requestId)
        }
    }

    @Test fun unknownIntegralProtocolVersionsRequireUpgrade() {
        listOf(-1L, 0L, 2L, Long.MAX_VALUE).forEach { version ->
            val error = failure(body(changes = mapOf("protocol_version" to version)), status = 426, code = ErrorCode.UNSUPPORTED_PROTOCOL)
            assertEquals(requestId, error.requestId)
            assertEquals("observe", error.operation)
        }
    }

    @Test fun protocolVersionRequiresIntegerPrimitiveNotStringOrDraft() {
        listOf(null, "1", "1-draft", true, 1.0, emptyList<Any>(), emptyMap<String, Any?>()).forEach { version ->
            failure(body(changes = mapOf("protocol_version" to version)))
        }
    }

    @Test fun schemaVersionAndDslFieldsCannotStandInForAgentProtocol() {
        val dsl = fields() - "protocol_version" + mapOf("schema_version" to "1-draft", "steps" to emptyList<Any>())
        failure(Json.encode(dsl).toByteArray(Charsets.UTF_8))
        failure(body(changes = mapOf("schema_version" to 1)))
        failure(body(changes = mapOf("steps" to listOf(mapOf("action" to "tap")))))
        failure(body(operation = "run", params = mapOf("case" to mapOf("schema_version" to "1-draft"))), "run")
    }

    @Test fun timeoutRangeIsInclusiveAndOmissionUsesDefault() {
        assertEquals(5000L, AgentProtocol.parse(body(), "observe").timeoutMs)
        listOf(100L, 10_000L).forEach { timeout ->
            assertEquals(timeout, AgentProtocol.parse(body(changes = mapOf("timeout_ms" to timeout)), "observe").timeoutMs)
        }
        assertEquals(10_000L, AgentProtocol.MAX_TIMEOUT_MS)
    }

    @Test fun timeoutRejectsNullNonintegersAndOutOfRangeValues() {
        listOf(null, -1L, 0L, 99L, 10_001L, Long.MAX_VALUE, "100", true, 100.5, emptyList<Any>(), emptyMap<String, Any?>()).forEach { timeout ->
            failure(body(changes = mapOf("timeout_ms" to timeout)))
        }
        val raw = String(body(), Charsets.UTF_8).dropLast(1) + ",\"timeout_ms\":1e3}"
        failure(raw.toByteArray(Charsets.UTF_8))
    }

    @Test fun operationMustMatchExpectedRoute() {
        val operations = listOf("observe", "action", "cancel")
        operations.forEach { operation ->
            operations.filter { it != operation }.forEach { expected ->
                val error = failure(body(operation = operation), expected)
                assertEquals(requestId, error.requestId)
                assertNull(error.operation)
            }
        }
    }

    @Test fun unknownOperationsAndInvalidOperationPrimitivesAreRejected() {
        listOf("health", "run", "execute", "shell", "observe_extra", "").forEach { operation ->
            failure(body(operation = operation), operation)
        }
        listOf(null, 1, true, emptyList<Any>(), emptyMap<String, Any?>()).forEach { operation ->
            failure(body(changes = mapOf("operation" to operation)))
        }
    }

    @Test fun unknownEnvelopeFieldsAreRejectedRatherThanIgnored() {
        listOf("token", "payload", "requestId", "timeout", "dsl", "schema_version").forEach { key ->
            val error = failure(body(changes = mapOf(key to "private-payload")))
            assertEquals(requestId, error.requestId)
            assertEquals("observe", error.operation)
        }
    }

    @Test fun unknownParamsAreRejectedForEveryOperation() {
        failure(body(params = mapOf("unknown" to true)))
        invalidAction(actionParams() + mapOf("unknown" to 1))
        failure(body("cancel", mapOf("target_session_id" to sessionId, "target_request_id" to targetId, "unknown" to true)), "cancel")
    }

    @Test fun jsonSyntaxIsStrictAndRequiresExactlyOneDocument() {
        val valid = String(body(), Charsets.UTF_8)
        listOf(
            "", " ", "{", valid.dropLast(1), "$valid{}", "$valid null", "$valid trailing",
            "/* comment */$valid", "//$valid", valid.replace("\"protocol_version\"", "protocol_version"),
            valid.replace("\"observe\"", "'observe'"), valid.dropLast(1) + ",}",
            valid.replace("\"observe\"", "\"ob\u0001serve\""),
            valid.replace("\"protocol_version\":1", "\"protocol_version\":01"),
            valid.replace("\"protocol_version\":1", "\"protocol_version\":NaN"),
        ).forEach { raw -> failure(raw.toByteArray(Charsets.UTF_8)) }
    }

    @Test fun duplicateEnvelopeFieldsAreRejectedEvenWhenValuesAgree() {
        val valid = String(body(), Charsets.UTF_8).dropLast(1)
        mapOf("protocol_version" to "1", "request_id" to "\"$requestId\"", "operation" to "\"observe\"", "params" to "{}").forEach { (key, value) ->
            failure("$valid,\"$key\":$value}".toByteArray(Charsets.UTF_8))
        }
        val timeout = "$valid,\"timeout_ms\":100,\"timeout_ms\":100}"
        failure(timeout.toByteArray(Charsets.UTF_8))
    }

    @Test fun duplicateNestedAndEscapedEquivalentFieldNamesAreRejected() {
        val prefix = "{\"protocol_version\":1,\"request_id\":\"$requestId\",\"operation\":\"observe\",\"params\":"
        listOf("{\"tree\":true,\"tree\":false}", "{\"tree\":true,\"tr\\u0065e\":true}").forEach { params ->
            failure("$prefix$params}".toByteArray(Charsets.UTF_8))
        }
        val duplicateId = String(body(), Charsets.UTF_8).dropLast(1) + ",\"request\\u005fid\":\"$requestId\"}"
        failure(duplicateId.toByteArray(Charsets.UTF_8))
        val action = "{\"kind\":\"tap\",\"expected_package\":\"com.example.app\",\"x\":1,\"x\":2,\"y\":2}"
        val raw = "{\"protocol_version\":1,\"request_id\":\"$requestId\",\"operation\":\"action\",\"params\":$action}"
        failure(raw.toByteArray(Charsets.UTF_8), "action")
    }

    @Test fun malformedUtf8IsRejectedWithoutReplacementDecoding() {
        val prefix = "{\"protocol_version\":1,\"request_id\":\"$requestId\",\"operation\":\"action\",\"params\":{\"kind\":\"set_text\",\"expected_package\":\"com.example.app\",\"node_path\":[],\"text\":\""
        val suffix = "\"}}"
        listOf(
            byteArrayOf(0x80.toByte()), byteArrayOf(0xc3.toByte()), byteArrayOf(0xc0.toByte(), 0xaf.toByte()),
            byteArrayOf(0xed.toByte(), 0xa0.toByte(), 0x80.toByte()),
            byteArrayOf(0xf4.toByte(), 0x90.toByte(), 0x80.toByte(), 0x80.toByte()),
            byteArrayOf(0xe2.toByte(), 0x28, 0xa1.toByte()),
        ).forEach { invalid ->
            val error = failure(prefix.toByteArray(Charsets.UTF_8) + invalid + suffix.toByteArray(Charsets.UTF_8), "action")
            assertNull(error.requestId)
            assertNull(error.operation)
        }
    }

    @Test fun validUtf8AndEscapedInputRoundTripExactly() {
        val text = "\u4e2d\u6587\ud83d\ude80\"\\\n\r\t\b\u000c\u0000"
        assertEquals(text, action(mapOf("text" to text), "set_text").text)
    }

    @Test fun exactMessageByteLimitIsAccepted() {
        val valid = body()
        val bytes = valid + ByteArray(AgentProtocol.MAX_MESSAGE_BYTES - valid.size) { ' '.code.toByte() }
        assertEquals(64 * 1024, bytes.size)
        assertEquals(requestId, AgentProtocol.parse(bytes, "observe").requestId)
    }

    @Test fun oversizedMessageIsRejectedBeforeJsonOrUtf8Decoding() {
        val valid = body()
        val bytes = valid + ByteArray(AgentProtocol.MAX_MESSAGE_BYTES + 1 - valid.size) { ' '.code.toByte() }
        val error = failure(bytes, status = 413, code = ErrorCode.MESSAGE_TOO_LARGE)
        assertNull(error.requestId)
        assertNull(error.operation)
        failure(ByteArray(AgentProtocol.MAX_MESSAGE_BYTES + 1) { 0xff.toByte() }, status = 413, code = ErrorCode.MESSAGE_TOO_LARGE)
    }

    @Test fun jsonDepthLimitIsInclusiveBeforeSchemaValidation() {
        fun nested(arrays: Int): ByteArray {
            val value = "[".repeat(arrays) + "true" + "]".repeat(arrays)
            return "{\"protocol_version\":1,\"request_id\":\"$requestId\",\"operation\":\"observe\",\"params\":{\"tree\":$value}}"
                .toByteArray(Charsets.UTF_8)
        }
        // Depth 16 decodes, then fails the boolean schema. Depth 17 fails before envelope assignment.
        assertEquals(requestId, failure(nested(14)).requestId)
        assertNull(failure(nested(15)).requestId)
        assertNull(failure(nested(64)).requestId)
    }

    @Test fun genericJsonArrayLimitCannotBypassSmallerNodePathLimit() {
        val atLimit = failure(body("action", actionParams("node_click") + mapOf("node_path" to List(512) { 0 })), "action")
        assertEquals(requestId, atLimit.requestId)
        assertEquals("action", atLimit.operation)
        val overLimit = failure(body("action", actionParams("node_click") + mapOf("node_path" to List(513) { 0 })), "action")
        assertNull(overLimit.requestId)
        assertNull(overLimit.operation)
    }

    @Test fun jsonObjectFieldCountIsBoundedBeforeSchemaValidation() {
        val atLimit = (0 until 32).associate { "unknown_$it" to true }
        assertEquals(requestId, failure(body(params = atLimit)).requestId)
        val overLimit = atLimit + mapOf("unknown_32" to true)
        assertNull(failure(body(params = overLimit)).requestId)
    }

    @Test fun jsonFieldNameLengthIsBoundedBeforeSchemaValidation() {
        assertEquals(requestId, failure(body(params = mapOf("x".repeat(64) to true))).requestId)
        assertNull(failure(body(params = mapOf("x".repeat(65) to true))).requestId)
    }

    @Test fun protocolErrorsCarryOnlyValidatedContextAndClosedErrorFields() {
        val secret = "private-token-input-screenshot-payload"
        val error = failure(body(changes = mapOf("unknown" to secret)))
        assertEquals(requestId, error.requestId)
        assertEquals("observe", error.operation)
        assertEquals("invalid_message", error.message)
        assertFalse(error.toString().contains(secret))
        assertEquals(mapOf("code" to "invalid_message", "phase" to "protocol", "retryable" to false), error.error.toWire())
        assertNull(failure(body(changes = mapOf("request_id" to secret))).requestId)
        assertNull(failure(body(changes = mapOf("operation" to secret))).operation)
    }

    @Test fun tapDefaultsIncludeOnlyValidatedPrimitiveArguments() {
        val tap = action()
        assertEquals("tap", tap.kind)
        assertEquals("com.example.app", tap.expectedPackage)
        assertEquals(10, tap.x)
        assertEquals(20, tap.y)
        assertEquals(tap.x, tap.endX)
        assertEquals(tap.y, tap.endY)
        assertEquals(80L, tap.durationMs)
        assertTrue(tap.nodePath.isEmpty())
        assertNull(tap.text)
    }

    @Test fun tapCoordinateAndDurationLimitsAreInclusive() {
        listOf(50L, 1000L).forEach { duration ->
            val tap = action(mapOf("x" to 0, "y" to 8191, "duration_ms" to duration))
            assertEquals(0, tap.x)
            assertEquals(8191, tap.y)
            assertEquals(duration, tap.durationMs)
        }
    }

    @Test fun swipeParsesBothEndpointsAndDefaultDuration() {
        val swipe = action(kind = "swipe")
        assertEquals("swipe", swipe.kind)
        assertEquals(10, swipe.x)
        assertEquals(20, swipe.y)
        assertEquals(30, swipe.endX)
        assertEquals(40, swipe.endY)
        assertEquals(300L, swipe.durationMs)
    }

    @Test fun swipeCoordinateAndDurationLimitsAreInclusive() {
        listOf(50L, 1000L).forEach { duration ->
            val swipe = action(mapOf("x" to 0, "y" to 8191, "end_x" to 8191, "end_y" to 0, "duration_ms" to duration), "swipe")
            assertEquals(0, swipe.x)
            assertEquals(8191, swipe.y)
            assertEquals(8191, swipe.endX)
            assertEquals(0, swipe.endY)
            assertEquals(duration, swipe.durationMs)
        }
    }

    @Test fun tapAndSwipeRequireEveryCoordinate() {
        listOf("tap", "swipe").forEach { kind ->
            val required = if (kind == "tap") listOf("x", "y") else listOf("x", "y", "end_x", "end_y")
            required.forEach { key -> invalidAction(actionParams(kind) - key) }
        }
    }

    @Test fun coordinatesRejectWrongPrimitiveTypesAndOutOfRangeValues() {
        listOf("tap", "swipe").forEach { kind ->
            val keys = if (kind == "tap") listOf("x", "y") else listOf("x", "y", "end_x", "end_y")
            keys.forEach { key ->
                listOf(null, -1L, 8192L, Long.MAX_VALUE, "1", true, 1.5, emptyList<Any>(), emptyMap<String, Any?>()).forEach { value ->
                    invalidAction(actionParams(kind) + mapOf(key to value))
                }
            }
        }
    }

    @Test fun durationsRejectExplicitNullWrongPrimitivesAndOutOfRangeValues() {
        listOf("tap", "swipe").forEach { kind ->
            listOf(null, 0L, 49L, 1001L, Long.MAX_VALUE, "80", true, 80.5, emptyList<Any>(), emptyMap<String, Any?>()).forEach { duration ->
                invalidAction(actionParams(kind) + mapOf("duration_ms" to duration))
            }
        }
    }

    @Test fun primitiveActionsRequireKindAndExpectedPackage() {
        listOf("tap", "swipe", "node_click", "set_text").forEach { kind ->
            listOf("kind", "expected_package").forEach { key -> invalidAction(actionParams(kind) - key) }
        }
    }

    @Test fun expectedPackageMustBeBoundedPackageNameNotCommandOrUrl() {
        listOf(null, 1, true, "", "app", ".com.example", "com.example.", "com..example", "com.123", "com example.app", "com.\u4e2d", "http://app", "com.app; id").forEach { pkg ->
            invalidAction(actionParams() + mapOf("expected_package" to pkg))
        }
        invalidAction(actionParams() + mapOf("expected_package" to "a.${"b".repeat(254)}"))
    }

    @Test fun exactExpectedPackageLengthAndUnderscoresAreAccepted() {
        val pkg = "a.${"b".repeat(253)}"
        assertEquals(255, pkg.length)
        assertEquals(pkg, action(mapOf("expected_package" to pkg)).expectedPackage)
        assertEquals("com.example_app1", action(mapOf("expected_package" to "com.example_app1")).expectedPackage)
    }

    @Test fun unknownActionKindsAndInvalidKindPrimitivesAreRejected() {
        listOf(null, 1, true, "", "click", "input", "shell", "script", "wait", "tap_extra", emptyList<Any>(), emptyMap<String, Any?>()).forEach { kind ->
            invalidAction(actionParams() + mapOf("kind" to kind))
        }
    }

    @Test fun actionFieldsCannotCrossPrimitiveBoundaries() {
        listOf("end_x", "end_y", "text", "node_path", "command").forEach { key ->
            invalidAction(actionParams("tap") + mapOf(key to 1))
        }
        listOf("text", "node_path", "command").forEach { key -> invalidAction(actionParams("swipe") + mapOf(key to "secret")) }
        listOf("x", "y", "end_x", "duration_ms", "command").forEach { key ->
            listOf("node_click", "set_text").forEach { kind -> invalidAction(actionParams(kind) + mapOf(key to 1)) }
        }
    }

    @Test fun nodePathLengthAndIndexLimitsAreInclusive() {
        val empty = action(mapOf("node_path" to emptyList<Int>()), "node_click")
        assertTrue(empty.nodePath.isEmpty())
        assertNull(empty.text)
        val indices = List(32) { if (it % 2 == 0) 0 else 511 }
        listOf("node_click", "set_text").forEach { kind ->
            assertEquals(indices, action(mapOf("node_path" to indices), kind).nodePath)
        }
    }

    @Test fun nodePathRequiresArrayAndAtMostThirtyTwoIndices() {
        listOf("node_click", "set_text").forEach { kind ->
            invalidAction(actionParams(kind) - "node_path")
            listOf(null, "0/1", 1, true, emptyMap<String, Any?>(), List(33) { 0 }).forEach { path ->
                invalidAction(actionParams(kind) + mapOf("node_path" to path))
            }
        }
    }

    @Test fun nodePathElementsRequireBoundedIntegerPrimitives() {
        listOf("node_click", "set_text").forEach { kind ->
            listOf(null, -1L, 512L, Long.MAX_VALUE, "1", true, 1.5, emptyList<Any>(), emptyMap<String, Any?>()).forEach { index ->
                invalidAction(actionParams(kind) + mapOf("node_path" to listOf(index)))
            }
        }
    }

    @Test fun setTextAllowsEmptyAndExactTextCharacterLimit() {
        assertEquals("", action(mapOf("text" to ""), "set_text").text)
        val text = "t".repeat(AgentProtocol.MAX_TEXT_CHARS)
        assertEquals(4096, text.length)
        assertEquals(text, action(mapOf("text" to text), "set_text").text)
    }

    @Test fun setTextRequiresBoundedStringPrimitive() {
        invalidAction(actionParams("set_text") - "text")
        listOf(null, 1, true, emptyList<Any>(), emptyMap<String, Any?>(), "t".repeat(AgentProtocol.MAX_TEXT_CHARS + 1)).forEach { text ->
            invalidAction(actionParams("set_text") + mapOf("text" to text))
        }
    }

    @Test fun nodeClickRejectsTextEvenWhenNullOrEmpty() {
        listOf(null, "", "private-input").forEach { text -> invalidAction(actionParams("node_click") + mapOf("text" to text)) }
    }

    @Test fun cancelRequestCarriesCanonicalSessionAndRequestTargets() {
        val request = AgentProtocol.parse(body("cancel", mapOf("target_session_id" to sessionId, "target_request_id" to targetId)), "cancel")
        assertEquals(requestId, request.requestId)
        assertEquals("cancel", request.operation)
        assertEquals(5000L, request.timeoutMs)
        assertEquals(sessionId, request.targetSessionId)
        assertEquals(targetId, request.targetRequestId)
        assertNull(request.observe)
        assertNull(request.action)
    }

    @Test fun cancelRequiresBothTargetIdsAndRejectsAdditionalFields() {
        val params = mapOf("target_session_id" to sessionId, "target_request_id" to targetId)
        params.keys.forEach { key -> failure(body("cancel", params - key), "cancel") }
        listOf("session_id", "request_id", "all", "token").forEach { key -> failure(body("cancel", params + mapOf(key to true)), "cancel") }
    }

    @Test fun cancelTargetsRequireCanonicalUuidStringPrimitives() {
        val params = mapOf("target_session_id" to sessionId, "target_request_id" to targetId)
        params.keys.forEach { key ->
            listOf(null, 1, true, "", "not-a-uuid", sessionId.uppercase(), " $sessionId", "$sessionId\n", emptyList<Any>(), emptyMap<String, Any?>()).forEach { id ->
                failure(body("cancel", params + mapOf(key to id)), "cancel")
            }
        }
    }

    @Test fun successEnvelopeHasStructuredVersionIdsAndTerminalResult() {
        val body = JsonParser.parseString(Envelope(requestId, sessionId, "observe", true, result = mapOf("tree" to emptyList<Any>())).encode()).asJsonObject
        assertEquals(
            setOf("protocol_version", "request_id", "session_id", "operation", "success", "terminal", "result", "error"),
            body.keySet(),
        )
        assertEquals(1, body["protocol_version"].asInt)
        assertEquals(requestId, body["request_id"].asString)
        assertEquals(sessionId, body["session_id"].asString)
        assertEquals("observe", body["operation"].asString)
        assertTrue(body["success"].asBoolean)
        assertTrue(body["terminal"].asBoolean)
        assertTrue(body["error"].isJsonNull)
        assertEquals(0, body["result"].asJsonObject["tree"].asJsonArray.size())
        assertFalse(body.has("schema_version"))
    }

    @Test fun errorEnvelopeHasClosedCodePhaseRetryabilityAndNullResult() {
        val encoded = Envelope(requestId, sessionId, "observe", false, error = AgentError(ErrorCode.BUSY, Phase.QUEUE, true)).encode()
        val body = JsonParser.parseString(encoded).asJsonObject
        assertFalse(body["success"].asBoolean)
        assertTrue(body["terminal"].asBoolean)
        assertTrue(body["result"].isJsonNull)
        val error = body["error"].asJsonObject
        assertEquals(setOf("code", "phase", "retryable"), error.keySet())
        assertEquals("busy", error["code"].asString)
        assertEquals("queue", error["phase"].asString)
        assertTrue(error["retryable"].asBoolean)
        assertFalse(error.has("message"))
        assertFalse(error.has("stack_trace"))
    }

    @Test fun actionDispatchedEnvelopeIsNonterminalAndCompletionIsTerminal() {
        val dispatched = JsonParser.parseString(Envelope(requestId, sessionId, "action", true, terminal = false, result = mapOf("status" to "dispatched")).encode()).asJsonObject
        val completed = JsonParser.parseString(Envelope(requestId, sessionId, "action", true, result = mapOf("status" to "completed")).encode()).asJsonObject
        assertFalse(dispatched["terminal"].asBoolean)
        assertEquals("dispatched", dispatched["result"].asJsonObject["status"].asString)
        assertTrue(completed["terminal"].asBoolean)
        assertEquals("completed", completed["result"].asJsonObject["status"].asString)
        listOf(dispatched, completed).forEach { event ->
            assertEquals(requestId, event["request_id"].asString)
            assertEquals(sessionId, event["session_id"].asString)
            assertEquals("action", event["operation"].asString)
            assertTrue(event["success"].asBoolean)
            assertTrue(event["error"].isJsonNull)
        }
    }

    @Test fun envelopeEscapesResultTextWithoutChangingUtf8Data() {
        val text = "\u4e2d\u6587\ud83d\ude80\"\\\n\r\t\u0000\u2028\u2029"
        val encoded = Envelope(requestId, sessionId, "observe", true, result = mapOf("text" to text)).encode()
        assertFalse(encoded.any { it.code < 32 })
        assertTrue(encoded.contains("\\u0000"))
        assertTrue(encoded.contains("\\u2028\\u2029"))
        val decoded = JsonParser.parseString(String(encoded.toByteArray(Charsets.UTF_8), Charsets.UTF_8)).asJsonObject
        assertEquals(text, decoded["result"].asJsonObject["text"].asString)
    }
}
