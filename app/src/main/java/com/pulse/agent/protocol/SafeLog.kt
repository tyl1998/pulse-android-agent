package com.pulse.agent.protocol

/** Only validated opaque IDs and closed enums cross the logging boundary. */
object SafeLog {
    fun error(requestId: String?, error: AgentError): String =
        "request_id=${requestId?.takeIf(AgentProtocol::validId) ?: "unassigned"} phase=${error.phase.wire} category=${error.code.wire}"
}
