package com.pulse.agent.debug

import android.net.LocalSocket
import android.net.LocalSocketAddress
import android.os.Process
import com.pulse.agent.transport.Json
import java.io.File
import java.io.IOException
import java.net.SocketTimeoutException

/** Debug APK only; invoked by ADB run-as, not an exported Android component. */
object LocalPeerProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        var outcome = "setup_failed"
        var tokenProvided = false
        val uid = Process.myUid()
        try {
            val token = if (args.contentEquals(arrayOf("--token-stdin"))) {
                val bytes = ByteArray(64)
                var count = 0
                while (count < bytes.size) {
                    val size = System.`in`.read(bytes, count, bytes.size - count)
                    check(size > 0)
                    count += size
                }
                String(bytes, Charsets.US_ASCII)
            } else {
                check(args.isEmpty() && uid != 0 && uid != 2000)
                File("files/agent-token").readText(Charsets.US_ASCII)
            }
            check(token.matches(Regex("[0-9a-f]{64}")))
            tokenProvided = true
            LocalSocket().use { socket ->
                try {
                    socket.connect(LocalSocketAddress("pulse-default", LocalSocketAddress.Namespace.ABSTRACT))
                    socket.soTimeout = 1000
                    socket.outputStream.write(("GET /v1/health HTTP/1.1\r\nAuthorization: Bearer $token\r\nContent-Length: 0\r\n\r\n").toByteArray(Charsets.US_ASCII))
                    socket.outputStream.flush()
                    val response = ByteArray(1024)
                    val size = socket.inputStream.read(response)
                    outcome = if (size < 0 || String(response, 0, size, Charsets.US_ASCII).startsWith("HTTP/1.1 403 ")) "rejected" else "accepted"
                } catch (_: SocketTimeoutException) { outcome = "timeout" }
                catch (_: IOException) { outcome = "rejected" }
            }
        } catch (_: Exception) { outcome = "setup_failed" }
        System.out.println(Json.encode(mapOf("peer_policy" to outcome, "peer_uid" to uid, "peer_pid" to Process.myPid(), "token_provided" to tokenProvided)))
    }
}
