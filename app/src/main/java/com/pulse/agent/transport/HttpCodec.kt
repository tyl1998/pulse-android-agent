package com.pulse.agent.transport

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.util.Locale
import com.pulse.agent.protocol.AgentProtocol

data class HttpRequest(
    val method: String,
    val path: String,
    val headers: Map<String, String>,
    val body: ByteArray,
) {
    /** 头名不区分大小写，解析时统一为小写。 */
    fun header(name: String): String? = headers[name.lowercase(Locale.ROOT)]
}

data class HttpResponse(
    val status: Int,
    val body: String,
    val contentType: String = "application/json; charset=utf-8",
)

class HttpParseException(val status: Int, message: String) : Exception(message)

/** 极简 HTTP/1.1 编解码：一连接一请求，仅支持 Content-Length，不支持 chunked。 */
object HttpCodec {
    const val MAX_HEADER_BYTES = 8 * 1024
    const val MAX_BODY_BYTES = AgentProtocol.MAX_MESSAGE_BYTES
    const val MAX_RESPONSE_BYTES = 8 * 1024 * 1024
    private val HEADER_NAME = Regex("[!#$%&'*+.^_`|~0-9A-Za-z-]+")

    fun readRequest(input: InputStream): HttpRequest = readBody(readHeaders(input), input)

    fun readHeaders(input: InputStream, checkDeadline: () -> Unit = {}): HttpRequest {
        val head = readHead(input, checkDeadline)
        val lines = head.split("\r\n")
        val parts = lines.first().split(' ')
        if (parts.size != 3 || parts[2] != "HTTP/1.1" || parts[0] !in setOf("GET", "POST") ||
            !Regex("/[A-Za-z0-9/_-]+").matches(parts[1]) || lines.size > 33 || lines.any { it.length > 2048 }) {
            throw HttpParseException(400, "malformed request line")
        }
        val headers = LinkedHashMap<String, String>()
        for (line in lines.drop(1)) {
            if (line.isEmpty()) continue
            val idx = line.indexOf(':')
            if (idx <= 0) throw HttpParseException(400, "malformed header")
            val name = line.substring(0, idx)
            val value = line.substring(idx + 1).trim(' ', '\t')
            val key = name.lowercase(Locale.ROOT)
            if (!HEADER_NAME.matches(name) || key in headers || value.any { it.code !in 32..126 }) {
                throw HttpParseException(400, "malformed header")
            }
            headers[key] = value
        }
        if (headers.containsKey("transfer-encoding") || headers.containsKey("expect")) {
            throw HttpParseException(400, "unsupported framing")
        }
        return HttpRequest(parts[0], parts[1], headers, ByteArray(0))
    }

    fun readBody(head: HttpRequest, input: InputStream, checkDeadline: () -> Unit = {}): HttpRequest {
        val headers = head.headers
        val length = headers["content-length"]?.let {
            if (!Regex("0|[1-9][0-9]{0,9}").matches(it)) throw HttpParseException(400, "bad content-length")
            it.toLongOrNull() ?: throw HttpParseException(400, "bad content-length")
        } ?: 0
        if (length > MAX_BODY_BYTES) throw HttpParseException(413, "body too large")
        if ((head.method == "POST" && length == 0L) || (head.method == "GET" && length != 0L)) {
            throw HttpParseException(400, "bad content-length")
        }
        val body = ByteArray(length.toInt())
        var read = 0
        while (read < body.size) {
            checkDeadline()
            val n = input.read(body, read, body.size - read)
            if (n < 0) throw HttpParseException(400, "unexpected end of body")
            read += n
        }
        checkDeadline()
        return head.copy(body = body)
    }

    private fun readHead(input: InputStream, checkDeadline: () -> Unit): String {
        val buf = ByteArrayOutputStream()
        var matched = 0 // 已匹配的 \r\n\r\n 字节数
        while (true) {
            checkDeadline()
            val b = input.read()
            if (b < 0) throw HttpParseException(400, "unexpected end of headers")
            if (b > 126 || (b < 32 && b !in listOf(9, 10, 13))) throw HttpParseException(400, "malformed header")
            buf.write(b)
            if (buf.size() > MAX_HEADER_BYTES) throw HttpParseException(431, "headers too large")
            matched = when {
                (matched == 0 || matched == 2) && b == '\r'.code -> matched + 1
                (matched == 1 || matched == 3) && b == '\n'.code -> matched + 1
                b == '\r'.code -> 1
                else -> 0
            }
            if (matched == 4) break
        }
        val bytes = buf.toByteArray()
        return String(bytes, 0, bytes.size - 4, StandardCharsets.ISO_8859_1)
    }

    fun writeResponse(output: OutputStream, response: HttpResponse) {
        val body = response.body.toByteArray(StandardCharsets.UTF_8)
        require(body.size <= MAX_RESPONSE_BYTES)
        val head = buildString {
            append("HTTP/1.1 ").append(response.status).append(' ').append(reason(response.status)).append("\r\n")
            append("Content-Type: ").append(response.contentType).append("\r\n")
            append("Content-Length: ").append(body.size).append("\r\n")
            append("Cache-Control: no-store\r\n")
            append("Connection: close\r\n\r\n")
        }
        output.write(head.toByteArray(StandardCharsets.ISO_8859_1))
        output.write(body)
        output.flush()
    }

    fun writeStreamHeaders(output: OutputStream, sessionId: String) {
        require(AgentProtocol.validId(sessionId))
        output.write(("HTTP/1.1 200 OK\r\nContent-Type: application/x-ndjson; charset=utf-8\r\n" +
            "Cache-Control: no-store\r\nX-Pulse-Session: $sessionId\r\nConnection: close\r\n\r\n").toByteArray(StandardCharsets.US_ASCII))
        output.flush()
    }

    fun writeEvent(output: OutputStream, encoded: String) {
        val bytes = encoded.toByteArray(StandardCharsets.UTF_8)
        if (bytes.size > MAX_RESPONSE_BYTES) throw HttpParseException(413, "response too large")
        output.write(bytes)
        output.write('\n'.code)
        output.flush()
    }

    private fun reason(status: Int): String = when (status) {
        200 -> "OK"
        400 -> "Bad Request"
        401 -> "Unauthorized"
        403 -> "Forbidden"
        404 -> "Not Found"
        405 -> "Method Not Allowed"
        408 -> "Request Timeout"
        413 -> "Payload Too Large"
        426 -> "Upgrade Required"
        431 -> "Request Header Fields Too Large"
        500 -> "Internal Server Error"
        501 -> "Not Implemented"
        503 -> "Service Unavailable"
        else -> "Status"
    }
}
