package com.pulse.agent.transport

import com.google.gson.JsonParser
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentProtocol
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import com.pulse.agent.protocol.ProtocolException
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class TransportPrimitivesTest {
    private val sessionId = "923e4567-e89b-42d3-a456-426614174001"
    private val expectedPeer = AdbPeer(2000, 1234)

    private fun input(raw: String): ByteArrayInputStream = ByteArrayInputStream(raw.toByteArray(Charsets.UTF_8))

    private fun parseFailure(status: Int, block: () -> Unit): HttpParseException {
        val error = assertThrows(HttpParseException::class.java) { block() }
        assertEquals(status, error.status)
        return error
    }

    private fun postHead(length: String?): HttpRequest = HttpRequest(
        "POST", "/v1/observe", if (length == null) emptyMap() else mapOf("content-length" to length), ByteArray(0),
    )

    @Test fun parsesHeadersCaseInsensitivelyAndBody() {
        val raw = "POST /v1/observe HTTP/1.1\r\nX-Foo: bar\r\ncOnTeNt-LeNgTh: 5\r\n\r\nhello"
        val req = HttpCodec.readRequest(input(raw))
        assertEquals("POST", req.method)
        assertEquals("/v1/observe", req.path)
        assertEquals("bar", req.header("x-foo"))
        assertEquals("bar", req.header("X-FOO"))
        assertEquals("5", req.header("CONTENT-LENGTH"))
        assertArrayEquals("hello".toByteArray(Charsets.UTF_8), req.body)
    }

    @Test fun boundedHeaderPhaseLeavesAllBodyBytesUnread() {
        val stream = input("POST /v1/observe HTTP/1.1\r\nContent-Length: 5\r\n\r\nhello")
        val head = HttpCodec.readHeaders(stream)
        assertArrayEquals(ByteArray(0), head.body)
        assertEquals(5, stream.available())
        assertArrayEquals("hello".toByteArray(Charsets.UTF_8), HttpCodec.readBody(head, stream).body)
        assertEquals(0, stream.available())
    }

    @Test fun malformedRequestLinesAndHttpVersionsAreRejected() {
        listOf(
            "garbage", "GET /v1/health HTTP/1.0", "GET /v1/health HTTP/2.0",
            "GET /v1/health HTTP/1.2", "GET /v1/health", "GET  /v1/health HTTP/1.1",
            "GET\t/v1/health HTTP/1.1", " GET /v1/health HTTP/1.1", "GET /v1/health HTTP/1.1 extra",
            "PUT /v1/action HTTP/1.1", "get /v1/health HTTP/1.1",
        ).forEach { line -> parseFailure(400) { HttpCodec.readHeaders(input("$line\r\n\r\n")) } }
    }

    @Test fun absoluteQueryFragmentAndEncodedPathsAreRejected() {
        listOf(
            "http://localhost/v1/health", "https://localhost/v1/health", "/v1/health?x=1",
            "/v1/observe?", "/v1/health#fragment", "/v1/%68ealth", "*",
        ).forEach { path -> parseFailure(400) { HttpCodec.readHeaders(input("GET $path HTTP/1.1\r\n\r\n")) } }
    }

    @Test fun incompleteOrLfOnlyHeadersAreRejected() {
        listOf("", "GET /v1/health HTTP/1.1", "GET /v1/health HTTP/1.1\r\n", "GET /v1/health HTTP/1.1\n\n").forEach { raw ->
            parseFailure(400) { HttpCodec.readHeaders(input(raw)) }
        }
    }

    @Test fun malformedHeaderNamesValuesAndObsoleteFoldingAreRejected() {
        listOf(
            "No-Colon", ": value", "Bad Name: value", "Bad\tName: value", " X-Folded: value",
            "X-Good: value\r\n continued", "X-Control: a\u0000b", "X-Control: a\u0001b",
            "X-Control: a\u007fb", "X-Control: a\nb", "X-Control: a\rb", "X-Control: \u00e9",
        ).forEach { header ->
            parseFailure(400) { HttpCodec.readHeaders(input("GET /v1/health HTTP/1.1\r\n$header\r\n\r\n")) }
        }
    }

    @Test fun duplicateHeadersAreRejectedEvenWithSameValueOrDifferentCase() {
        listOf(
            "Content-Length: 5\r\ncontent-length: 5", "Content-Length: 5\r\nContent-Length: 6",
            "Authorization: Bearer one\r\naUtHoRiZaTiOn: Bearer two", "X-Foo: value\r\nx-foo: value",
        ).forEach { headers ->
            parseFailure(400) { HttpCodec.readHeaders(input("POST /v1/observe HTTP/1.1\r\n$headers\r\n\r\n")) }
        }
    }

    @Test fun chunkedAndAllTransferEncodingFramingAreRejected() {
        listOf("chunked", "identity", "gzip, chunked", "").forEach { value ->
            parseFailure(400) {
                HttpCodec.readHeaders(input("POST /v1/observe HTTP/1.1\r\nTransfer-Encoding: $value\r\nContent-Length: 5\r\n\r\n"))
            }
        }
    }

    @Test fun expectContinueIsRejectedBeforeAnyBodyRead() {
        val stream = input("POST /v1/observe HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello")
        parseFailure(400) { HttpCodec.readHeaders(stream) }
        assertEquals(5, stream.available())
    }

    @Test fun contentLengthMustBeCanonicalUnsignedDecimal() {
        listOf("-1", "+1", "01", "00", "1.0", "1e3", "NaN", "one", "1 0", "99999999999").forEach { length ->
            val stream = input("hello")
            parseFailure(400) { HttpCodec.readBody(postHead(length), stream) }
            assertEquals(5, stream.available())
        }
    }

    @Test fun postRequiresNonzeroContentLength() {
        listOf(null, "0").forEach { length ->
            val stream = input("hello")
            parseFailure(400) { HttpCodec.readBody(postHead(length), stream) }
            assertEquals(5, stream.available())
        }
    }

    @Test fun getRejectsNonemptyBodyWithoutReadingIt() {
        val stream = input("hello")
        val head = HttpRequest("GET", "/v1/health", mapOf("content-length" to "5"), ByteArray(0))
        parseFailure(400) { HttpCodec.readBody(head, stream) }
        assertEquals(5, stream.available())
    }

    @Test fun getAllowsAbsentOrZeroContentLength() {
        listOf(emptyMap(), mapOf("content-length" to "0")).forEach { headers ->
            val stream = input("next-request")
            val request = HttpCodec.readBody(HttpRequest("GET", "/v1/health", headers, ByteArray(0)), stream)
            assertArrayEquals(ByteArray(0), request.body)
            assertEquals("next-request".length, stream.available())
        }
    }

    @Test fun truncatedBodyIsRejected() {
        parseFailure(400) { HttpCodec.readBody(postHead("5"), input("hell")) }
    }

    @Test fun oversizedDeclaredBodyIsRejectedBeforeAllocationOrReading() {
        assertEquals(AgentProtocol.MAX_MESSAGE_BYTES, HttpCodec.MAX_BODY_BYTES)
        listOf((HttpCodec.MAX_BODY_BYTES + 1).toString(), "2147483648", "9999999999").forEach { length ->
            val stream = input("payload")
            parseFailure(413) { HttpCodec.readBody(postHead(length), stream) }
            assertEquals(7, stream.available())
        }
    }

    @Test fun exactBodyByteLimitIsAccepted() {
        val bytes = ByteArray(HttpCodec.MAX_BODY_BYTES) { (it % 128).toByte() }
        val stream = ByteArrayInputStream(bytes)
        val request = HttpCodec.readBody(postHead(bytes.size.toString()), stream)
        assertEquals(HttpCodec.MAX_BODY_BYTES, request.body.size)
        assertArrayEquals(bytes, request.body)
        assertEquals(0, stream.available())
    }

    @Test fun bodyReaderDoesNotConsumeBytesBeyondContentLength() {
        val stream = input("hellonext-request")
        assertArrayEquals("hello".toByteArray(Charsets.UTF_8), HttpCodec.readBody(postHead("5"), stream).body)
        assertEquals("next-request".length, stream.available())
        assertEquals('n'.code, stream.read())
    }

    @Test fun exactHeaderByteLimitIsAccepted() {
        val raw = paddedHeaders(HttpCodec.MAX_HEADER_BYTES)
        assertEquals(HttpCodec.MAX_HEADER_BYTES, raw.toByteArray(Charsets.US_ASCII).size)
        val request = HttpCodec.readHeaders(input(raw))
        assertEquals("GET", request.method)
        assertEquals(4, request.headers.size)
        assertArrayEquals(ByteArray(0), request.body)
    }

    @Test fun oneByteOverHeaderLimitIsRejected() {
        val raw = paddedHeaders(HttpCodec.MAX_HEADER_BYTES + 1)
        assertEquals(HttpCodec.MAX_HEADER_BYTES + 1, raw.toByteArray(Charsets.US_ASCII).size)
        parseFailure(431) { HttpCodec.readHeaders(input(raw)) }
    }

    @Test fun headerLineLengthBoundaryIsInclusive() {
        val prefix = "X-Pad: "
        val value = "p".repeat(2048 - prefix.length)
        val request = HttpCodec.readHeaders(input("GET /v1/health HTTP/1.1\r\n$prefix$value\r\n\r\n"))
        assertEquals(value, request.header("x-pad"))
        parseFailure(400) { HttpCodec.readHeaders(input("GET /v1/health HTTP/1.1\r\n$prefix${value}p\r\n\r\n")) }
    }

    @Test fun headerCountBoundaryIsInclusive() {
        val lines = (0 until 32).joinToString("") { "X-$it: value\r\n" }
        val request = HttpCodec.readHeaders(input("GET /v1/health HTTP/1.1\r\n$lines\r\n"))
        assertEquals(32, request.headers.size)
        parseFailure(400) { HttpCodec.readHeaders(input("GET /v1/health HTTP/1.1\r\n${lines}X-32: value\r\n\r\n")) }
    }

    @Test fun headerSlowDripCannotExtendAbsoluteDeadline() {
        val raw = "GET /v1/health HTTP/1.1\r\n\r\n"
        val stream = DripInput(raw.toByteArray(Charsets.US_ASCII))
        var checks = 0
        val error = assertThrows(ProtocolException::class.java) {
            HttpCodec.readHeaders(stream) {
                checks++
                if (stream.elapsedTicks >= 7) deadlineExceeded()
            }
        }
        assertEquals(408, error.status)
        assertEquals(ErrorCode.TIMEOUT, error.error.code)
        assertEquals(Phase.TRANSPORT, error.error.phase)
        assertEquals(7, stream.elapsedTicks)
        assertEquals(raw.length - 7, stream.available())
        assertTrue(checks > stream.elapsedTicks)
    }

    @Test fun bodySlowDripCannotExtendAbsoluteDeadline() {
        val stream = DripInput("hello".toByteArray(Charsets.UTF_8))
        var checks = 0
        val error = assertThrows(ProtocolException::class.java) {
            HttpCodec.readBody(postHead("5"), stream) {
                checks++
                if (stream.elapsedTicks >= 3) deadlineExceeded()
            }
        }
        assertEquals(408, error.status)
        assertEquals(ErrorCode.TIMEOUT, error.error.code)
        assertEquals(Phase.TRANSPORT, error.error.phase)
        assertEquals(3, stream.elapsedTicks)
        assertEquals(2, stream.available())
        assertTrue(checks > stream.elapsedTicks)
    }

    @Test fun alreadyExpiredDeadlinePreventsFirstHeaderOrBodyRead() {
        val headStream = DripInput("GET /v1/health HTTP/1.1\r\n\r\n".toByteArray(Charsets.US_ASCII))
        assertThrows(ProtocolException::class.java) { HttpCodec.readHeaders(headStream, ::deadlineExceeded) }
        assertEquals(0, headStream.elapsedTicks)
        val bodyStream = DripInput("hello".toByteArray(Charsets.UTF_8))
        assertThrows(ProtocolException::class.java) { HttpCodec.readBody(postHead("5"), bodyStream, ::deadlineExceeded) }
        assertEquals(0, bodyStream.elapsedTicks)
    }

    @Test fun deadlineIsCheckedAfterTheFinalBodyByteAndForEmptyBody() {
        val stream = DripInput("hello".toByteArray(Charsets.UTF_8))
        assertThrows(ProtocolException::class.java) {
            HttpCodec.readBody(postHead("5"), stream) { if (stream.elapsedTicks >= 5) deadlineExceeded() }
        }
        assertEquals(5, stream.elapsedTicks)
        assertEquals(0, stream.available())
        val emptyHead = HttpRequest("GET", "/v1/health", emptyMap(), ByteArray(0))
        assertThrows(ProtocolException::class.java) { HttpCodec.readBody(emptyHead, input(""), ::deadlineExceeded) }
    }

    @Test fun jsonEscapesControlAndQuotes() {
        assertEquals(
            """{"a":"q\"b\\s\n\u0001","n":[1,true,null]}""",
            Json.encode(mapOf("a" to "q\"b\\s\n\u0001", "n" to listOf(1, true, null))),
        )
    }

    @Test fun jsonEscapesEveryControlAndLineSeparatorAndPreservesUtf8() {
        val value = "\"\\\n\r\t\b\u000c\u0000\u001f\u2028\u2029\u4e2d\ud83d\ude80"
        val encoded = Json.encode(mapOf("key\"\n" to value))
        assertTrue(encoded.contains("\\\"\\\\\\n\\r\\t\\b\\f\\u0000\\u001f\\u2028\\u2029"))
        assertFalse(encoded.any { it.code < 32 })
        assertEquals(value, JsonParser.parseString(encoded).asJsonObject["key\"\n"].asString)
    }

    @Test fun jsonRejectsNonFiniteNumbersAndUnsupportedObjects() {
        listOf(Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Float.NaN, Float.POSITIVE_INFINITY).forEach { value ->
            assertThrows(IllegalArgumentException::class.java) { Json.encode(mapOf("value" to value)) }
        }
        assertThrows(IllegalArgumentException::class.java) { Json.encode(Any()) }
    }

    @Test fun httpContentLengthCountsUtf8BytesNotCharacters() {
        val body = Json.encode(mapOf("text" to "\u4e2d\u6587\ud83d\ude80\n\"\\"))
        val output = ByteArrayOutputStream()
        HttpCodec.writeResponse(output, HttpResponse(200, body))
        val wire = output.toByteArray()
        val headLength = output.toString(Charsets.UTF_8.name()).indexOf("\r\n\r\n") + 4
        val head = String(wire, 0, headLength, Charsets.US_ASCII)
        val bodyBytes = body.toByteArray(Charsets.UTF_8)
        assertNotEquals(body.length, bodyBytes.size)
        assertTrue(head.startsWith("HTTP/1.1 200 OK\r\n"))
        assertTrue(head.contains("Content-Type: application/json; charset=utf-8\r\n"))
        assertTrue(head.contains("Content-Length: ${bodyBytes.size}\r\n"))
        assertTrue(head.contains("Connection: close\r\n"))
        assertArrayEquals(bodyBytes, wire.copyOfRange(headLength, wire.size))
    }

    @Test fun ordinarySuccessAndErrorResponsesAreNoStore() {
        listOf(200, 400, 401, 403, 413, 426, 431, 500, 503).forEach { status ->
            val output = ByteArrayOutputStream()
            HttpCodec.writeResponse(output, HttpResponse(status, "{}"))
            assertTrue("status=$status", output.toString(Charsets.US_ASCII.name()).contains("Cache-Control: no-store\r\n"))
        }
    }

    @Test fun exactResponseOutputLimitIsAccepted() {
        val output = ByteArrayOutputStream()
        HttpCodec.writeResponse(output, HttpResponse(200, "a".repeat(HttpCodec.MAX_RESPONSE_BYTES)))
        assertTrue(output.size() > HttpCodec.MAX_RESPONSE_BYTES)
        val head = String(output.toByteArray(), 0, 200, Charsets.US_ASCII)
        assertTrue(head.contains("Content-Length: ${HttpCodec.MAX_RESPONSE_BYTES}\r\n"))
    }

    @Test fun oversizedResponseIsRejectedBeforeWritingHeaders() {
        val output = ByteArrayOutputStream()
        assertThrows(IllegalArgumentException::class.java) {
            HttpCodec.writeResponse(output, HttpResponse(200, "a".repeat(HttpCodec.MAX_RESPONSE_BYTES + 1)))
        }
        assertEquals(0, output.size())
    }

    @Test fun outputLimitIsMeasuredInUtf8Bytes() {
        val body = "\u4e2d".repeat(HttpCodec.MAX_RESPONSE_BYTES / 3 + 1)
        assertTrue(body.length < HttpCodec.MAX_RESPONSE_BYTES)
        assertTrue(body.toByteArray(Charsets.UTF_8).size > HttpCodec.MAX_RESPONSE_BYTES)
        val output = ByteArrayOutputStream()
        assertThrows(IllegalArgumentException::class.java) { HttpCodec.writeResponse(output, HttpResponse(200, body)) }
        assertEquals(0, output.size())
        parseFailure(413) { HttpCodec.writeEvent(output, body) }
        assertEquals(0, output.size())
    }

    @Test fun streamHeadersAreNoStoreAndUseAnOpaqueSessionId() {
        val output = ByteArrayOutputStream()
        HttpCodec.writeStreamHeaders(output, sessionId)
        val wire = output.toString(Charsets.US_ASCII.name())
        assertTrue(wire.startsWith("HTTP/1.1 200 OK\r\n"))
        assertTrue(wire.contains("Content-Type: application/x-ndjson; charset=utf-8\r\n"))
        assertTrue(wire.contains("Cache-Control: no-store\r\n"))
        assertTrue(wire.contains("X-Pulse-Session: $sessionId\r\n"))
        assertTrue(wire.endsWith("Connection: close\r\n\r\n"))
        assertFalse(wire.contains("Transfer-Encoding"))
        assertFalse(wire.contains("Content-Length"))
    }

    @Test fun invalidSessionIdCannotInjectStreamHeaders() {
        listOf("bad", sessionId.uppercase(), "$sessionId\r\nX-Injected: secret").forEach { id ->
            val output = ByteArrayOutputStream()
            assertThrows(IllegalArgumentException::class.java) { HttpCodec.writeStreamHeaders(output, id) }
            assertEquals(0, output.size())
        }
    }

    @Test fun eventIsOneUtf8JsonLineWithStructuredTerminalError() {
        val encoded = Envelope(
            sessionId, sessionId, "action", false,
            result = mapOf("status" to "failed", "text" to "\u4e2d\n\"\\"),
            error = AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH),
        ).encode()
        val output = ByteArrayOutputStream()
        HttpCodec.writeEvent(output, encoded)
        assertArrayEquals((encoded + "\n").toByteArray(Charsets.UTF_8), output.toByteArray())
        assertEquals(1, output.toString(Charsets.UTF_8.name()).count { it == '\n' })
        val body = JsonParser.parseString(output.toString(Charsets.UTF_8.name())).asJsonObject
        assertTrue(body["terminal"].asBoolean)
        assertFalse(body["success"].asBoolean)
        assertEquals("platform_failed", body["error"].asJsonObject["code"].asString)
        assertEquals("\u4e2d\n\"\\", body["result"].asJsonObject["text"].asString)
    }

    @Test fun exactEventOutputLimitIsAcceptedAndGetsOneFramingNewline() {
        val output = ByteArrayOutputStream()
        HttpCodec.writeEvent(output, "a".repeat(HttpCodec.MAX_RESPONSE_BYTES))
        assertEquals(HttpCodec.MAX_RESPONSE_BYTES + 1, output.size())
        assertEquals('\n'.code.toByte(), output.toByteArray().last())
    }

    @Test fun oversizedEventIsRejectedBeforeWriting() {
        val output = ByteArrayOutputStream()
        parseFailure(413) { HttpCodec.writeEvent(output, "a".repeat(HttpCodec.MAX_RESPONSE_BYTES + 1)) }
        assertEquals(0, output.size())
    }

    @Test fun tokenRequiresBearerAndMinLength() {
        val t = TokenStore()
        assertFalse(t.verify("Bearer anything"))
        t.set("k".repeat(32))
        assertTrue(t.verify("Bearer " + "k".repeat(32)))
        assertFalse(t.verify("k".repeat(32)))
        assertFalse(t.verify("Bearer " + "k".repeat(31)))
        t.clear()
        assertFalse(t.verify("Bearer " + "k".repeat(32)))
    }

    @Test fun tokenRejectsWhitespaceControlsNonAsciiAndInvalidLengths() {
        listOf(
            "k".repeat(31), "k".repeat(129), " " + "k".repeat(32), "k".repeat(32) + " ",
            "k".repeat(31) + "\t", "k".repeat(31) + "\n", "k".repeat(31) + "\u007f",
            "\u00e9".repeat(32), "\u4e2d".repeat(32),
        ).forEach { value -> assertThrows(IllegalArgumentException::class.java) { TokenStore().set(value) } }
        listOf(32, 128).forEach { length ->
            val t = TokenStore().apply { set("k".repeat(length)) }
            assertTrue(t.verify("Bearer ${"k".repeat(length)}"))
        }
    }

    @Test fun bearerPrefixAndTokenAreExactAndNeverTrimmed() {
        val token = "k".repeat(32)
        val t = TokenStore().apply { set(token) }
        listOf(null, token, "bearer $token", "Bearer\t$token", "Bearer  $token", "Bearer $token ", "Bearer $token\n").forEach { auth ->
            assertFalse(t.verify(auth))
        }
        assertTrue(t.verify("Bearer $token"))
    }

    @Test fun generatedTokenIsBoundedOpaqueHexAndRotates() {
        val t = TokenStore()
        val first = t.generate()
        assertTrue(Regex("[0-9a-f]{64}").matches(first))
        assertTrue(t.verify("Bearer $first"))
        val second = t.generate()
        assertTrue(Regex("[0-9a-f]{64}").matches(second))
        assertNotEquals(first, second)
        assertFalse(t.verify("Bearer $first"))
        assertTrue(t.verify("Bearer $second"))
        t.clear()
        assertFalse(t.verify("Bearer $second"))
    }

    @Test(expected = IllegalArgumentException::class)
    fun shortTokenRefused() {
        TokenStore().set("short")
    }

    @Test fun adbPeerAcceptsOnlyRootOrShellWithPositivePid() {
        listOf(0, 2000).forEach { uid ->
            listOf(1, 1234, Int.MAX_VALUE).forEach { pid ->
                val peer = AdbPeer(uid, pid)
                assertEquals(uid, peer.uid)
                assertEquals(pid, peer.pid)
            }
        }
    }

    @Test fun adbPeerRejectsAppSystemAndNegativeUidConfiguration() {
        listOf(Int.MIN_VALUE, -1, 1, 1000, 1001, 1999, 2001, 9999, 10000, 10001, Int.MAX_VALUE).forEach { uid ->
            assertThrows(IllegalArgumentException::class.java) { AdbPeer(uid, 1234) }
            assertThrows(IllegalArgumentException::class.java) { expectedPeer.copy(uid = uid) }
        }
    }

    @Test fun adbPeerRejectsNonpositivePidConfiguration() {
        listOf(0, 2000).forEach { uid ->
            listOf(0, -1, Int.MIN_VALUE).forEach { pid ->
                assertThrows(IllegalArgumentException::class.java) { AdbPeer(uid, pid) }
                assertThrows(IllegalArgumentException::class.java) { AdbPeer(uid, 1234).copy(pid = pid) }
            }
        }
    }

    @Test fun peerProvisioningParsesBothUidProfilesAndOptionalSingleLf() {
        assertEquals(expectedPeer, PeerPolicy.parse("2000:1234"))
        listOf(0, 2000).forEach { uid ->
            listOf(1, 1234, Int.MAX_VALUE).forEach { pid ->
                listOf("", "\n").forEach { suffix ->
                    assertEquals(AdbPeer(uid, pid), PeerPolicy.parse("$uid:$pid$suffix"))
                }
            }
        }
    }

    @Test fun peerProvisioningRejectsMissingExtraFieldsAndInvalidUids() {
        listOf(
            "", "2000", "2000:", ":1234", "uid:pid", "2000::1234", "2000:1234:1234",
            "2000;1234", "2000/1234", "00:1234", "+2000:1234", "02000:1234", "-1:1234",
            "1000:1234", "10000:1234", "2000:1234\n0:1234", "0:1234\n2000:1234",
        ).forEach { value -> assertThrows(IllegalArgumentException::class.java) { PeerPolicy.parse(value) } }
    }

    @Test fun peerProvisioningRejectsCrWhitespaceAndMultipleNewlines() {
        listOf(
            "2000:1234\r", "2000:1234\r\n", "\n2000:1234", "2000:1234\n\n",
            " 2000:1234", "2000:1234 ", "2000 :1234", "2000: 1234", "2000:1234 \n",
            "\t2000:1234", "2000:\t1234", "2000:1234\t", "2000:12\n34", "2000:1234\u000b",
        ).forEach { value -> assertThrows(IllegalArgumentException::class.java) { PeerPolicy.parse(value) } }
    }

    @Test fun peerProvisioningRejectsInvalidNonpositiveAndOverflowPids() {
        listOf(0, 2000).forEach { uid ->
            listOf(
                "0", "-1", "-2147483648", "+1234", "01234", "0000", "pid", "1.0", "1e3",
                "NaN", "0x4d2", "2147483648", "9999999999", "10000000000", Long.MAX_VALUE.toString(),
            ).forEach { pid ->
                assertThrows(IllegalArgumentException::class.java) { PeerPolicy.parse("$uid:$pid") }
            }
        }
    }

    @Test fun peerProvisioningRejectsNonAsciiDigitsSeparatorsAndControls() {
        listOf(
            "2000:\uff11\uff12\uff13\uff14", "2000:\u0661\u0662\u0663\u0664", "\uff12\uff10\uff10\uff10:1234",
            "2000\uff1a1234", "2000:1234\u00a0", "2000:1234\u2028", "2000:1234\u0000", "2000:1234\u007f",
        ).forEach { value -> assertThrows(IllegalArgumentException::class.java) { PeerPolicy.parse(value) } }
    }

    @Test fun peerProvisioningRejectsMalformedAtSizeCeilingAndOversizedValues() {
        val atCeiling = "2000:" + "1".repeat(27)
        val overCeiling = "2000:" + "1".repeat(28)
        assertEquals(32, atCeiling.length)
        assertEquals(33, overCeiling.length)
        listOf(atCeiling, overCeiling, "2000:1234\n" + "x".repeat(32), "0:" + "1".repeat(4096)).forEach { value ->
            assertThrows(IllegalArgumentException::class.java) { PeerPolicy.parse(value) }
        }
    }

    @Test fun peerPolicyRequiresExactUidAndPidAgainstEachExplicitPin() {
        listOf(expectedPeer, AdbPeer(0, 1234)).forEach { peer ->
            listOf(-1, 0, 1000, 2000, 10000).forEach { uid ->
                listOf(Int.MIN_VALUE, -1, 0, 1, 1233, 1234, 1235, Int.MAX_VALUE).forEach { pid ->
                    assertEquals("uid=$uid pid=$pid expected=$peer", uid == peer.uid && pid == peer.pid, PeerPolicy.allowed(uid, pid, peer))
                }
            }
        }
    }

    @Test fun peerIngressApiHasNoUidOnlyPolicyOrAuthorizationOverload() {
        val allowedMethods = PeerPolicy::class.java.declaredMethods.filter { it.name == "allowed" }
        assertEquals(1, allowedMethods.size)
        assertArrayEquals(arrayOf(Integer.TYPE, Integer.TYPE, AdbPeer::class.java), allowedMethods.single().parameterTypes)
        val authorizeMethods = Router::class.java.declaredMethods.filter { it.name == "authorize" }
        assertEquals(1, authorizeMethods.size)
        assertArrayEquals(arrayOf(HttpRequest::class.java, Integer.TYPE, Integer.TYPE), authorizeMethods.single().parameterTypes)
        val verifyMethods = Router::class.java.declaredMethods.filter { it.name == "verifyPeer" }
        assertEquals(1, verifyMethods.size)
        assertArrayEquals(arrayOf(Integer.TYPE, Integer.TYPE), verifyMethods.single().parameterTypes)
        assertFalse(Router::class.java.constructors.any {
            it.parameterTypes.contentEquals(arrayOf(TokenStore::class.java, Integer.TYPE, String::class.java))
        })
    }

    private fun paddedHeaders(size: Int): String {
        val prefix = "GET /v1/health HTTP/1.1\r\n" +
            (0 until 3).joinToString("") { "X-Pad-$it: ${"p".repeat(2030)}\r\n" } + "X-Pad-3: "
        return prefix + "p".repeat(size - prefix.length - 4) + "\r\n\r\n"
    }

    private fun deadlineExceeded(): Nothing = throw ProtocolException(408, AgentError(ErrorCode.TIMEOUT, Phase.TRANSPORT))

    private class DripInput(bytes: ByteArray) : ByteArrayInputStream(bytes) {
        // Each successful byte advances a fake monotonic clock; no sleeps or real sockets.
        var elapsedTicks = 0
            private set

        override fun read(): Int = super.read().also { if (it >= 0) elapsedTicks++ }

        override fun read(bytes: ByteArray, offset: Int, length: Int): Int =
            super.read(bytes, offset, minOf(length, 1)).also { if (it > 0) elapsedTicks++ }
    }
}
