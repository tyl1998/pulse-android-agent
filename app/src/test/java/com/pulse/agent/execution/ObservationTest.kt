package com.pulse.agent.execution

import com.pulse.agent.protocol.ActionParams
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentRequest
import com.pulse.agent.protocol.Envelope
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.ObserveParams
import com.pulse.agent.protocol.Phase
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class ObservationTest {
    @Test fun availableSourcePreservesDataAndCaptureTimesWithoutAnError() {
        val data = mapOf("nodes" to listOf(mapOf("text" to "Submit")))
        val source = SourceResult("tree", CapabilityStatus.AVAILABLE, data, startedAtMs = 11, finishedAtMs = 17)
        val wire = source.toWire()

        assertEquals("available", wire["status"])
        assertEquals("tree", wire["source"])
        assertEquals(true, wire["requested"])
        assertEquals(11L, wire["started_at_uptime_ms"])
        assertEquals(17L, wire["finished_at_uptime_ms"])
        assertEquals(data, wire["data"])
        assertNull(wire["error"])
        assertCompleteness(wire, "complete")
    }

    @Test(expected = IllegalArgumentException::class)
    fun availableSourceCannotClaimSuccessWithNullData() {
        SourceResult("tree", CapabilityStatus.AVAILABLE)
    }

    @Test(expected = IllegalArgumentException::class)
    fun availableSourceCannotContainBothDataAndAnError() {
        SourceResult(
            "ocr", CapabilityStatus.AVAILABLE, data = mapOf("blocks" to emptyList<Any>()),
            error = AgentError(ErrorCode.OCR_FAILED, Phase.OBSERVE),
        )
    }

    @Test(expected = IllegalArgumentException::class)
    fun unavailableSourceMustRetainAnError() {
        SourceResult("ocr", CapabilityStatus.UNAVAILABLE)
    }

    @Test(expected = IllegalArgumentException::class)
    fun failedSourceMustRetainAnError() {
        SourceResult("ocr", CapabilityStatus.FAILED)
    }

    @Test fun unavailableAndFailedSourcesCannotContainFakeEmptyOrNonemptyData() {
        for (status in listOf(CapabilityStatus.UNAVAILABLE, CapabilityStatus.FAILED)) {
            for (data in listOf(emptyMap(), mapOf("blocks" to emptyList<Any>()), mapOf("blocks" to listOf(mapOf("text" to "Invented"))))) {
                assertThrows(IllegalArgumentException::class.java) {
                    SourceResult("ocr", status, data, AgentError(ErrorCode.OCR_FAILED, Phase.OBSERVE))
                }
            }
        }
    }

    @Test fun successfulNonemptyOcrPreservesTextBoundsAndDoesNotSynthesizeNativeStates() {
        val blocks = listOf(
            mapOf(
                "text" to "Submit",
                "bounds" to mapOf("left" to 10, "top" to 20, "right" to 80, "bottom" to 40),
                "confidence" to null,
            ),
        )
        val source = availableOcr(blocks)
        val observation = observation(source)
        val data = objectMap(source.toWire()["data"])

        assertTrue(observation.hasEvidence)
        assertEquals("available", source.toWire()["status"])
        assertEquals(blocks, data["blocks"])
        assertNull(source.toWire()["error"])
        for (block in objectList(data["blocks"])) {
            assertFalse(block.containsKey("checked"))
            assertFalse(block.containsKey("enabled"))
            assertFalse(block.containsKey("states"))
            assertFalse(block.containsKey("checkable"))
        }
        assertCompleteness(source.toWire(), "partial", "ocr_text_only_no_native_states")
        assertCompleteness(observation.toWire(), "partial", "ocr_text_only_no_native_states")
    }

    @Test fun successfulEmptyOcrIsLegitimateAvailableEvidenceRatherThanUnavailable() {
        val source = availableOcr(emptyList())
        val observation = observation(source)
        val data = objectMap(source.toWire()["data"])

        assertEquals("available", source.toWire()["status"])
        assertEquals(emptyList<Any>(), data["blocks"])
        assertNull(source.toWire()["error"])
        assertTrue(observation.hasEvidence)
        assertCompleteness(observation.toWire(), "partial", "ocr_text_only_no_native_states")
    }

    @Test fun unavailableAndFailedOcrKeepDistinctErrorsAndNullDataNotEmptyBlocks() {
        val sources = listOf(
            SourceResult.unavailable("ocr", ErrorCode.OCR_UNAVAILABLE),
            SourceResult.failed("ocr", ErrorCode.OCR_FAILED),
        )
        for (source in sources) {
            val wire = source.toWire()
            val error = checkNotNull(source.error)
            assertTrue(wire.containsKey("data"))
            assertNull(wire["data"])
            assertEquals(source.status.wire, wire["status"])
            assertEquals(error.toWire(), wire["error"])
            assertEquals(Phase.OBSERVE, error.phase)
            assertFalse(observation(source).hasEvidence)
            assertCompleteness(wire, "unavailable")
            assertCompleteness(observation(source).toWire(), "unavailable", "ocr_${source.status.wire}")
        }
    }

    @Test fun ocrNeverInheritsCheckedOrEnabledFromNeighboringNativeTreeEvidence() {
        val states = mapOf("enabled" to false, "checked" to true, "checkable" to true)
        val tree = SourceResult(
            "tree", CapabilityStatus.AVAILABLE,
            data = mapOf("nodes" to listOf(mapOf("text" to "Agree", "states" to states))),
        )
        val ocr = availableOcr(listOf(mapOf("text" to "Agree", "bounds" to null, "confidence" to null)))
        val wire = observation(tree, ocr).toWire()
        val nativeNodes = objectList(objectMap(sourceWire(wire, "tree")["data"])["nodes"])
        val ocrBlocks = objectList(objectMap(sourceWire(wire, "ocr")["data"])["blocks"])

        assertEquals(states, nativeNodes.single()["states"])
        assertEquals(setOf("text", "bounds", "confidence"), ocrBlocks.single().keys)
        assertFalse(ocrBlocks.single().containsKey("checked"))
        assertFalse(ocrBlocks.single().containsKey("enabled"))
        assertFalse(ocrBlocks.single().containsKey("states"))
    }

    @Test fun completeSuppliedTreeEvidenceIsNotPenalizedForUnrequestedScreenshotOrOcr() {
        val tree = availableTree()
        val observation = Observation(
            mapOf("tree" to tree, "screenshot" to SourceResult.notRequested("screenshot"), "ocr" to SourceResult.notRequested("ocr")),
            capabilities = mapOf("tree" to true, "screenshot" to true, "ocr" to true),
            revisionStart = 7, revisionEnd = 7,
        )
        val wire = observation.toWire()

        assertTrue(observation.hasEvidence)
        assertCompleteness(wire, "complete")
        assertEquals("active_window", wire["scope"])
        assertEquals(false, wire["atomic_snapshot"])
        assertEquals(7L, wire["revision_start"])
        assertEquals(7L, wire["revision_end"])
        for (name in listOf("screenshot", "ocr")) {
            val source = sourceWire(wire, name)
            assertEquals(false, source["requested"])
            assertEquals("unavailable", source["status"])
            assertNull(source["data"])
            assertEquals(AgentError(ErrorCode.NOT_REQUESTED, Phase.OBSERVE).toWire(), source["error"])
        }
    }

    @Test fun webViewAccessibilityOnlyTreeRemainsPartial() {
        assertPartialTree("webview_semantics_limited")
    }

    @Test fun flutterOrSurfaceSemanticsTreeRemainsPartial() {
        assertPartialTree("flutter_or_surface_semantics_limited")
    }

    @Test fun nativeNodeDepthAndTextLimitsRemainPartialWithoutDroppingCapturedData() {
        val reasons = listOf("node_limit", "depth_limit", "text_truncated")
        val tree = availableTree(reasons)
        val wire = observation(tree).toWire()

        assertCompleteness(tree.toWire(), "partial", *reasons.toTypedArray())
        assertCompleteness(wire, "partial", *reasons.toTypedArray())
        assertEquals(tree.data, sourceWire(wire, "tree")["data"])
    }

    @Test fun nativeSemanticsWithoutVerifiedVisualCoverageRemainPartial() {
        assertPartialTree("native_semantics_only_visual_coverage_unverified")
    }

    @Test fun missingRequestedScreenshotAndFailedOcrMakeNativeEvidencePartial() {
        val observation = observation(
            availableTree(),
            SourceResult.unavailable("screenshot", ErrorCode.SCREENSHOT_UNAVAILABLE),
            SourceResult.failed("ocr", ErrorCode.OCR_FAILED),
        )
        val wire = observation.toWire()

        assertTrue(observation.hasEvidence)
        assertCompleteness(wire, "partial", "screenshot_unavailable", "ocr_failed")
        assertNull(sourceWire(wire, "screenshot")["data"])
        assertNull(sourceWire(wire, "ocr")["data"])
        assertEquals(AgentError(ErrorCode.SCREENSHOT_UNAVAILABLE, Phase.OBSERVE).toWire(), sourceWire(wire, "screenshot")["error"])
        assertEquals(AgentError(ErrorCode.OCR_FAILED, Phase.OBSERVE).toWire(), sourceWire(wire, "ocr")["error"])
    }

    @Test fun missingRequestedTreeMakesAvailableOcrPartialRatherThanNativeTreeEvidence() {
        val observation = observation(
            SourceResult.unavailable("tree", ErrorCode.ACCESSIBILITY_UNAVAILABLE),
            availableOcr(listOf(mapOf("text" to "Settings"))),
        )
        val wire = observation.toWire()

        assertTrue(observation.hasEvidence)
        assertCompleteness(wire, "partial", "tree_unavailable", "ocr_text_only_no_native_states")
        assertNull(sourceWire(wire, "tree")["data"])
        assertEquals(AgentError(ErrorCode.ACCESSIBILITY_UNAVAILABLE, Phase.OBSERVE).toWire(), sourceWire(wire, "tree")["error"])
    }

    @Test fun failedRequestedTreeDoesNotHideSuccessfulScreenshotEvidence() {
        val screenshot = SourceResult("screenshot", CapabilityStatus.AVAILABLE, data = mapOf("width" to 100, "height" to 200))
        val observation = observation(SourceResult.failed("tree", ErrorCode.PLATFORM_FAILED), screenshot)
        val wire = observation.toWire()

        assertTrue(observation.hasEvidence)
        assertCompleteness(wire, "partial", "tree_failed")
        assertCompleteness(sourceWire(wire, "tree"), "unavailable")
        assertEquals(screenshot.data, sourceWire(wire, "screenshot")["data"])
    }

    @Test fun pageRevisionChangeMakesOtherwiseCompleteCapturePartialAndNonAtomic() {
        val observation = Observation(mapOf("tree" to availableTree()), emptyMap(), revisionStart = 41, revisionEnd = 42)
        val wire = observation.toWire()

        assertTrue(observation.hasEvidence)
        assertCompleteness(wire, "partial", "page_changed_during_capture")
        assertEquals(41L, wire["revision_start"])
        assertEquals(42L, wire["revision_end"])
        assertEquals(false, wire["atomic_snapshot"])
        assertCompleteness(sourceWire(wire, "tree"), "complete")
    }

    @Test fun completenessReasonsAreDeduplicatedAcrossSourcesAndRevisionChange() {
        val tree = availableTree(listOf("node_limit", "node_limit", "page_changed_during_capture"))
        val ocr = SourceResult(
            "ocr", CapabilityStatus.AVAILABLE, data = mapOf("blocks" to emptyList<Any>()),
            reasons = listOf("node_limit", "ocr_text_only_no_native_states"),
        )
        val observation = Observation(mapOf("tree" to tree, "ocr" to ocr), emptyMap(), revisionStart = 1, revisionEnd = 2)

        assertCompleteness(observation.toWire(), "partial", "node_limit", "page_changed_during_capture", "ocr_text_only_no_native_states")
    }

    @Test fun noRequestedSourcesCannotClaimAvailableEvidence() {
        val unrequestedAvailable = SourceResult(
            "tree", CapabilityStatus.AVAILABLE, data = mapOf("nodes" to emptyList<Any>()), requested = false,
        )
        val observation = observation(unrequestedAvailable, SourceResult.notRequested("ocr"))

        assertFalse(observation.hasEvidence)
        assertCompleteness(observation.toWire(), "unavailable")
    }

    @Test fun emptySourceMapIsUnavailableWithoutFabricatingSources() {
        val capabilities = mapOf("tree" to false, "screenshot" to false, "ocr" to false)
        val observation = Observation(emptyMap(), capabilities)
        val wire = observation.toWire()

        assertFalse(observation.hasEvidence)
        assertCompleteness(wire, "unavailable")
        assertEquals(emptyMap<String, Any?>(), wire["sources"])
        assertEquals(capabilities, wire["capabilities"])
    }

    @Test fun allUnavailableCaptureRemainsUnavailableEvenWhenThePageChanges() {
        val observation = Observation(
            mapOf("tree" to SourceResult.unavailable("tree", ErrorCode.WINDOW_UNAVAILABLE)),
            emptyMap(), revisionStart = 1, revisionEnd = 2,
        )

        assertFalse(observation.hasEvidence)
        assertCompleteness(observation.toWire(), "unavailable", "tree_unavailable", "page_changed_during_capture")
    }

    @Test(timeout = 10_000) fun allUnavailableEnvelopeFailsWhilePreservingCapabilitiesAndEverySourceError() {
        val capabilities = mapOf(
            "tree" to mapOf("available" to false, "error" to AgentError(ErrorCode.ACCESSIBILITY_UNAVAILABLE, Phase.OBSERVE).toWire()),
            "screenshot" to mapOf("available" to false, "error" to AgentError(ErrorCode.SCREEN_PROTECTED, Phase.OBSERVE).toWire()),
            "ocr" to mapOf("available" to false, "error" to AgentError(ErrorCode.OCR_FAILED, Phase.OBSERVE).toWire()),
        )
        val observation = Observation(
            linkedMapOf(
                "tree" to SourceResult.unavailable("tree", ErrorCode.ACCESSIBILITY_UNAVAILABLE),
                "screenshot" to SourceResult.unavailable("screenshot", ErrorCode.SCREEN_PROTECTED),
                "ocr" to SourceResult.failed("ocr", ErrorCode.OCR_FAILED),
            ),
            capabilities,
        )
        val envelope = observeEnvelope(observation, ObserveParams(tree = true, screenshot = true, ocr = true))

        assertTrue(envelope.terminal)
        assertFalse(envelope.success)
        assertEquals(AgentError(ErrorCode.WINDOW_UNAVAILABLE, Phase.OBSERVE), envelope.error)
        assertNotNull(envelope.result)
        val result = checkNotNull(envelope.result)
        assertEquals(observation.toWire(), result)
        assertEquals(capabilities, result["capabilities"])
        assertCompleteness(result, "unavailable", "tree_unavailable", "screenshot_unavailable", "ocr_failed")
        for ((name, source) in observation.sources) {
            val wire = sourceWire(result, name)
            assertNull(wire["data"])
            assertEquals(source.error!!.toWire(), wire["error"])
            assertEquals(source.status.wire, wire["status"])
        }
        assertFalse(envelope.encode().contains("\"blocks\":[]"))
        assertTrue(envelope.encode().contains("\"success\":false"))
    }

    @Test(timeout = 10_000) fun legitimateEmptyOcrAsTheOnlyEvidenceProducesASuccessfulEnvelope() {
        val observation = Observation(
            mapOf(
                "tree" to SourceResult.notRequested("tree"),
                "screenshot" to SourceResult.notRequested("screenshot"),
                "ocr" to availableOcr(emptyList()),
            ),
            capabilities = mapOf("ocr" to true),
        )
        val envelope = observeEnvelope(observation, ObserveParams(tree = false, ocr = true))

        assertTrue(envelope.terminal)
        assertTrue(envelope.success)
        assertNull(envelope.error)
        assertNotNull(envelope.result)
        val result = checkNotNull(envelope.result)
        assertEquals(emptyList<Any>(), objectMap(sourceWire(result, "ocr")["data"])["blocks"])
        assertCompleteness(result, "partial", "ocr_text_only_no_native_states")
    }

    private fun observeEnvelope(observation: Observation, params: ObserveParams): Envelope {
        val events = LinkedBlockingQueue<Envelope>()
        val observeCalls = AtomicInteger()
        val seenParams = CompletableFuture<ObserveParams>()
        val backend = object : DeviceBackend {
            override fun observe(scope: RequestScope, params: ObserveParams): Observation {
                observeCalls.incrementAndGet()
                seenParams.complete(params)
                return observation
            }

            override fun dispatch(scope: RequestScope, params: ActionParams): CompletableFuture<ActionCompletion>? = null
            override fun close() = Unit
        }
        val scope = RequestScope(
            UUID.randomUUID().toString(),
            AgentRequest(UUID.randomUUID().toString(), "observe", 10_000, observe = params),
            events = { events.offer(it) },
        )
        DeviceCoordinator(backend).use { coordinator ->
            coordinator.submit(scope)
            val envelope = checkNotNull(events.poll(2_000, TimeUnit.MILLISECONDS)) { "No observation envelope within bounded wait" }
            assertEquals(params, seenParams.get(2_000, TimeUnit.MILLISECONDS))
            assertEquals(1, observeCalls.get())
            assertEquals(scope.sessionId, envelope.sessionId)
            assertEquals(scope.request.requestId, envelope.requestId)
            assertEquals("observe", envelope.operation)
            assertTrue(scope.isEnded)
            assertTrue(events.isEmpty())
            return envelope
        }
    }

    private fun availableTree(reasons: List<String> = emptyList()) = SourceResult(
        "tree", CapabilityStatus.AVAILABLE,
        data = mapOf("nodes" to listOf(mapOf("text" to "Submit", "node_path" to listOf(0)))), reasons = reasons,
    )

    private fun availableOcr(blocks: List<Map<String, Any?>>) = SourceResult(
        "ocr", CapabilityStatus.AVAILABLE, data = mapOf("blocks" to blocks), reasons = listOf("ocr_text_only_no_native_states"),
    )

    private fun observation(vararg sources: SourceResult) = Observation(sources.associateBy { it.source }, emptyMap())

    private fun assertPartialTree(reason: String) {
        val tree = availableTree(listOf(reason))
        val observation = observation(tree)
        assertTrue(observation.hasEvidence)
        assertCompleteness(tree.toWire(), "partial", reason)
        assertCompleteness(observation.toWire(), "partial", reason)
        assertEquals(tree.data, sourceWire(observation.toWire(), "tree")["data"])
    }

    private fun assertCompleteness(wire: Map<*, *>, status: String, vararg reasons: String) {
        assertEquals(mapOf("status" to status, "reasons" to reasons.toList()), wire["completeness"])
    }

    private fun sourceWire(wire: Map<*, *>, source: String): Map<*, *> = objectMap(objectMap(wire["sources"])[source])

    private fun objectMap(value: Any?): Map<*, *> {
        assertTrue("Expected a wire object", value is Map<*, *>)
        return value as Map<*, *>
    }

    private fun objectList(value: Any?): List<Map<*, *>> {
        assertTrue("Expected a wire array", value is List<*>)
        return (value as List<*>).map(::objectMap)
    }
}
