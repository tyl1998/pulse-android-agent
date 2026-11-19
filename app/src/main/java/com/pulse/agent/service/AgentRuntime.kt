package com.pulse.agent.service

import com.pulse.agent.transport.TokenStore
import com.pulse.agent.execution.DeviceCoordinator
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/** Process-local state only. Page content is never cached across connections. */
object AgentRuntime {
    val tokens = TokenStore()
    val revision = AtomicLong(0)
    // Opaque lease only; no observation/result cache survives a session or FGS restart.
    val platformOperation = AtomicReference<Any?>(null)
    val recoveryOperation = AtomicReference<Any?>(null)

    @Volatile
    var coordinator: DeviceCoordinator? = null
        internal set

    @Volatile
    var accessibility: AgentAccessibilityService? = null
        internal set
}
