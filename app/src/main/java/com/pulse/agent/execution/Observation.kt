package com.pulse.agent.execution

import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase

enum class CapabilityStatus(val wire: String) { AVAILABLE("available"), UNAVAILABLE("unavailable"), FAILED("failed") }

data class SourceResult(
    val source: String,
    val status: CapabilityStatus,
    val data: Map<String, Any?>? = null,
    val error: AgentError? = null,
    val reasons: List<String> = emptyList(),
    val requested: Boolean = true,
    val startedAtMs: Long? = null,
    val finishedAtMs: Long? = null,
) {
    init {
        require(if (status == CapabilityStatus.AVAILABLE) data != null && error == null else data == null && error != null)
    }

    val completeness: String get() = when {
        status != CapabilityStatus.AVAILABLE -> "unavailable"
        reasons.isNotEmpty() -> "partial"
        else -> "complete"
    }

    fun toWire(): Map<String, Any?> = mapOf(
        "source" to source, "status" to status.wire, "requested" to requested,
        "started_at_uptime_ms" to startedAtMs, "finished_at_uptime_ms" to finishedAtMs,
        "completeness" to mapOf("status" to completeness, "reasons" to reasons),
        "data" to data, "error" to error?.toWire(),
    )

    companion object {
        fun notRequested(source: String) = SourceResult(
            source, CapabilityStatus.UNAVAILABLE, error = AgentError(ErrorCode.NOT_REQUESTED, Phase.OBSERVE), requested = false,
        )

        fun unavailable(source: String, code: ErrorCode) = SourceResult(
            source, CapabilityStatus.UNAVAILABLE, error = AgentError(code, Phase.OBSERVE),
        )

        fun failed(source: String, code: ErrorCode) = SourceResult(
            source, CapabilityStatus.FAILED, error = AgentError(code, Phase.OBSERVE),
        )
    }
}

data class Observation(
    val sources: Map<String, SourceResult>,
    val capabilities: Map<String, Any?>,
    val revisionStart: Long = 0,
    val revisionEnd: Long = revisionStart,
) {
    val hasEvidence: Boolean get() = sources.values.any { it.requested && it.status == CapabilityStatus.AVAILABLE }

    fun toWire(): Map<String, Any?> {
        val requested = sources.values.filter { it.requested }
        val reasons = requested.flatMap { source ->
            source.reasons + if (source.status != CapabilityStatus.AVAILABLE) listOf("${source.source}_${source.status.wire}") else emptyList()
        }.toMutableList()
        if (revisionStart != revisionEnd) reasons.add("page_changed_during_capture")
        val completeness = when {
            !hasEvidence -> "unavailable"
            reasons.isNotEmpty() -> "partial"
            else -> "complete"
        }
        return mapOf(
            "scope" to "active_window",
            "sources" to sources.mapValues { it.value.toWire() },
            "capabilities" to capabilities,
            "completeness" to mapOf("status" to completeness, "reasons" to reasons.distinct()),
            "revision_start" to revisionStart, "revision_end" to revisionEnd,
            "atomic_snapshot" to false,
        )
    }
}
