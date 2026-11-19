package com.pulse.agent.transport

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicReference

/** Private process-lifetime token. No unauthenticated provisioning endpoint. */
class TokenStore {
    private val token = AtomicReference<ByteArray?>(null)

    fun set(value: String) {
        require(value.length in 32..128 && value.all { it.code in 33..126 }) { "invalid token" }
        token.set(value.toByteArray(StandardCharsets.UTF_8))
    }

    fun clear() = token.set(null)

    fun generate(): String = ByteArray(32).also(SecureRandom()::nextBytes)
        .joinToString("") { "%02x".format(it.toInt() and 0xff) }.also(::set)

    /** 校验 `Authorization: Bearer <token>`，常量时间比较。 */
    fun verify(authorization: String?): Boolean {
        val expected = token.get() ?: return false
        if (authorization == null || !authorization.startsWith(PREFIX)) return false
        val actual = authorization.substring(PREFIX.length).toByteArray(StandardCharsets.UTF_8)
        return MessageDigest.isEqual(expected, actual)
    }

    private companion object {
        const val PREFIX = "Bearer "
    }
}
