package com.pulse.agent.protocol

import com.google.gson.Strictness
import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.pulse.agent.transport.Json
import java.io.StringReader
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets

enum class Phase(val wire: String) {
    TRANSPORT("transport"), AUTH("auth"), PROTOCOL("protocol"), QUEUE("queue"),
    OBSERVE("observe"), DISPATCH("dispatch"), COMPLETION("completion"), LIFECYCLE("lifecycle"),
}

enum class ErrorCode(val wire: String) {
    UNAUTHORIZED("unauthorized"), FORBIDDEN_PEER("forbidden_peer"), PROVISIONING_REQUIRED("provisioning_required"), INVALID_MESSAGE("invalid_message"),
    MESSAGE_TOO_LARGE("message_too_large"), UNSUPPORTED_PROTOCOL("unsupported_protocol"),
    NOT_FOUND("not_found"), METHOD_NOT_ALLOWED("method_not_allowed"), BUSY("busy"),
    TIMEOUT("timeout"), CANCELLED("cancelled"), DISCONNECTED("disconnected"), STOPPED("stopped"),
    ACCESSIBILITY_UNAVAILABLE("accessibility_unavailable"), ACCESSIBILITY_INTERRUPTED("accessibility_interrupted"),
    WINDOW_UNAVAILABLE("window_unavailable"), UNSUPPORTED_DISPLAY("unsupported_display"), TARGET_CHANGED("target_changed"), TARGET_UNAVAILABLE("target_unavailable"),
    DISPATCH_REJECTED("dispatch_rejected"), GESTURE_CANCELLED("gesture_cancelled"), ACTION_UNCERTAIN("action_uncertain"), PLATFORM_FAILED("platform_failed"),
    NOT_REQUESTED("not_requested"), SCREENSHOT_UNAVAILABLE("screenshot_unavailable"),
    SCREENSHOT_RATE_LIMITED("screenshot_rate_limited"), SCREEN_PROTECTED("screen_protected"),
    IMAGE_TOO_LARGE("image_too_large"), OCR_UNAVAILABLE("ocr_unavailable"), OCR_FAILED("ocr_failed"),
    RESPONSE_TOO_LARGE("response_too_large"), INTERNAL_ERROR("internal_error"),
}

data class AgentError(val code: ErrorCode, val phase: Phase, val retryable: Boolean = false) {
    fun toWire(): Map<String, Any?> = mapOf("code" to code.wire, "phase" to phase.wire, "retryable" to retryable)
}

class AgentException(val error: AgentError) : Exception(error.code.wire)

class ProtocolException(
    val status: Int,
    val error: AgentError,
    val requestId: String? = null,
    val operation: String? = null,
) : Exception(error.code.wire)

data class ObserveParams(val tree: Boolean = true, val screenshot: Boolean = false, val ocr: Boolean = false)

data class ActionParams(
    val kind: String,
    val expectedPackage: String,
    val nodePath: List<Int> = emptyList(),
    val text: String? = null,
    val x: Int = 0,
    val y: Int = 0,
    val endX: Int = 0,
    val endY: Int = 0,
    val durationMs: Long = 80,
)

data class AgentRequest(
    val requestId: String,
    val operation: String,
    val timeoutMs: Long,
    val observe: ObserveParams? = null,
    val action: ActionParams? = null,
    val targetSessionId: String? = null,
    val targetRequestId: String? = null,
)

data class Envelope(
    val requestId: String?,
    val sessionId: String?,
    val operation: String?,
    val success: Boolean,
    val terminal: Boolean = true,
    val result: Map<String, Any?>? = null,
    val error: AgentError? = null,
) {
    fun encode(): String = Json.encode(
        mapOf(
            "protocol_version" to AgentProtocol.VERSION,
            "request_id" to requestId,
            "session_id" to sessionId,
            "operation" to operation,
            "success" to success,
            "terminal" to terminal,
            "result" to result,
            "error" to error?.toWire(),
        ),
    )
}

/** Agent wire version only. This is not the case DSL or its 1-draft Schema. */
object AgentProtocol {
    const val VERSION = 1
    const val MAX_TIMEOUT_MS = 10_000L
    const val MAX_TEXT_CHARS = 4096
    const val MAX_MESSAGE_BYTES = 64 * 1024
    private const val MAX_JSON_DEPTH = 16
    private val UUID_PATTERN = Regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
    private val PACKAGE_PATTERN = Regex("[A-Za-z][A-Za-z0-9_]*(\\.[A-Za-z][A-Za-z0-9_]*)+")

    fun validId(value: String?): Boolean = value != null && UUID_PATTERN.matches(value)

    fun parse(body: ByteArray, expectedOperation: String): AgentRequest {
        var id: String? = null
        var operation: String? = null
        try {
            if (body.size > MAX_MESSAGE_BYTES) throw ProtocolException(413, AgentError(ErrorCode.MESSAGE_TOO_LARGE, Phase.PROTOCOL))
            val text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body)).toString()
            val root = JsonReader(StringReader(text)).use { reader ->
                reader.strictness = Strictness.STRICT
                val value = readValue(reader, 0)
                require(reader.peek() == JsonToken.END_DOCUMENT)
                asObject(value)
            }
            id = root["request_id"] as? String
            require(validId(id))
            operation = root["operation"] as? String
            require(operation == expectedOperation)
            requireKeys(root, setOf("protocol_version", "request_id", "operation", "timeout_ms", "params"))
            val version = root["protocol_version"] as? Long ?: invalid()
            if (version != VERSION.toLong()) {
                throw ProtocolException(426, AgentError(ErrorCode.UNSUPPORTED_PROTOCOL, Phase.PROTOCOL), id, operation)
            }
            val timeout = if ("timeout_ms" in root) asLong(root["timeout_ms"], 100, MAX_TIMEOUT_MS) else 5000L
            val params = asObject(root["params"])
            return when (operation) {
                "observe" -> {
                    requireKeys(params, setOf("tree", "screenshot", "ocr"))
                    val observe = ObserveParams(bool(params, "tree", true), bool(params, "screenshot", false), bool(params, "ocr", false))
                    require(observe.tree || observe.screenshot || observe.ocr)
                    AgentRequest(id!!, operation, timeout, observe = observe)
                }
                "action" -> AgentRequest(id!!, operation, timeout, action = parseAction(params))
                "cancel" -> {
                    requireKeys(params, setOf("target_session_id", "target_request_id"))
                    val session = params["target_session_id"] as? String
                    val target = params["target_request_id"] as? String
                    require(validId(session) && validId(target))
                    AgentRequest(id!!, operation, timeout, targetSessionId = session, targetRequestId = target)
                }
                else -> invalid()
            }
        } catch (e: ProtocolException) {
            throw e
        } catch (_: Exception) {
            throw ProtocolException(400, AgentError(ErrorCode.INVALID_MESSAGE, Phase.PROTOCOL), id?.takeIf(::validId), operation?.takeIf { it == expectedOperation })
        }
    }

    private fun parseAction(params: Map<String, Any?>): ActionParams {
        val kind = params["kind"] as? String ?: invalid()
        val pkg = params["expected_package"] as? String ?: invalid()
        require(pkg.length <= 255 && PACKAGE_PATTERN.matches(pkg))
        val common = setOf("kind", "expected_package")
        return when (kind) {
            "tap", "swipe" -> {
                requireKeys(params, common + setOf("x", "y", "end_x", "end_y", "duration_ms"))
                if (kind == "tap") require("end_x" !in params && "end_y" !in params)
                val x = asLong(params["x"], 0, 8191).toInt()
                val y = asLong(params["y"], 0, 8191).toInt()
                val endX = if (kind == "swipe") asLong(params["end_x"], 0, 8191).toInt() else x
                val endY = if (kind == "swipe") asLong(params["end_y"], 0, 8191).toInt() else y
                val duration = if ("duration_ms" in params) asLong(params["duration_ms"], 50, 1000) else if (kind == "tap") 80L else 300L
                ActionParams(kind, pkg, x = x, y = y, endX = endX, endY = endY, durationMs = duration)
            }
            "set_text", "node_click" -> {
                requireKeys(params, common + setOf("node_path", "text"))
                val path = params["node_path"] as? List<*> ?: invalid()
                require(path.size <= 32)
                val indices = path.map { asLong(it, 0, 511).toInt() }
                val text = if (kind == "set_text") params["text"] as? String ?: invalid() else null
                require(text == null || text.length <= MAX_TEXT_CHARS)
                if (kind == "node_click") require("text" !in params)
                ActionParams(kind, pkg, nodePath = indices, text = text)
            }
            else -> invalid()
        }
    }

    private fun readValue(reader: JsonReader, depth: Int): Any? {
        require(depth <= MAX_JSON_DEPTH)
        return when (reader.peek()) {
            JsonToken.BEGIN_OBJECT -> {
                reader.beginObject()
                val result = linkedMapOf<String, Any?>()
                while (reader.hasNext()) {
                    require(result.size < 32)
                    val name = reader.nextName()
                    require(name.length <= 64 && name !in result)
                    result[name] = readValue(reader, depth + 1)
                }
                reader.endObject()
                result
            }
            JsonToken.BEGIN_ARRAY -> {
                reader.beginArray()
                val result = mutableListOf<Any?>()
                while (reader.hasNext()) {
                    require(result.size < 512)
                    result.add(readValue(reader, depth + 1))
                }
                reader.endArray()
                result
            }
            JsonToken.STRING -> reader.nextString()
            JsonToken.NUMBER -> {
                val literal = reader.nextString()
                require(literal.length <= 64)
                literal.toLongOrNull() ?: literal.toBigDecimalOrNull() ?: invalid()
            }
            JsonToken.BOOLEAN -> reader.nextBoolean()
            JsonToken.NULL -> { reader.nextNull(); null }
            else -> invalid()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun asObject(value: Any?): Map<String, Any?> = value as? Map<String, Any?> ?: invalid()

    private fun requireKeys(value: Map<String, Any?>, keys: Set<String>) = require(value.keys.all { it in keys })

    private fun asLong(value: Any?, min: Long, max: Long): Long = (value as? Long)?.takeIf { it in min..max } ?: invalid()

    private fun bool(value: Map<String, Any?>, key: String, default: Boolean): Boolean =
        if (key !in value) default else value[key] as? Boolean ?: invalid()

    private fun invalid(): Nothing = throw IllegalArgumentException("invalid_message")
}
