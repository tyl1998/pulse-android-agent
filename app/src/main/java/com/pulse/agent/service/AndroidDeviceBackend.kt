package com.pulse.agent.service

import android.accessibilityservice.AccessibilityService
import android.accessibilityservice.GestureDescription
import android.graphics.Bitmap
import android.graphics.Path
import android.graphics.Rect
import android.os.Bundle
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import android.util.Base64
import android.view.Display
import android.view.accessibility.AccessibilityNodeInfo
import android.view.accessibility.AccessibilityWindowInfo
import com.google.mlkit.vision.common.InputImage
import com.google.mlkit.vision.text.TextRecognition
import com.google.mlkit.vision.text.TextRecognizer
import com.google.mlkit.vision.text.chinese.ChineseTextRecognizerOptions
import com.pulse.agent.execution.ActionCompletion
import com.pulse.agent.execution.CapabilityStatus
import com.pulse.agent.execution.DeviceBackend
import com.pulse.agent.execution.Observation
import com.pulse.agent.execution.PlatformOperation
import com.pulse.agent.execution.RequestScope
import com.pulse.agent.execution.SourceResult
import com.pulse.agent.protocol.ActionParams
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.ObserveParams
import com.pulse.agent.protocol.Phase
import java.io.ByteArrayOutputStream
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.sqrt

/** Platform callbacks only release their own lease/result, never dispatch another action. */
@Suppress("DEPRECATION")
class AndroidDeviceBackend : DeviceBackend {
    private val main = Handler(Looper.getMainLooper())
    private val closed = AtomicBoolean(false)
    private val pending = AgentRuntime.platformOperation
    private val ownedPending = AtomicReference<PlatformOperation<*>?>(null)
    private val callbacks = ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, ArrayBlockingQueue<Runnable>(4)) { r ->
        Thread(r, "pulse-callback").apply { isDaemon = true }
    }
    private val callbackExecutor = Executor { task ->
        try { callbacks.execute(task) } catch (_: RejectedExecutionException) { task.run() }
    }
    private val recognizer: TextRecognizer? = runCatching {
        TextRecognition.getClient(ChineseTextRecognizerOptions.Builder().build())
    }.getOrNull()
    private var lastScreenshotAt = -1000L

    fun capabilities(): Map<String, Any?> {
        val available = !closed.get() && AgentRuntime.accessibility != null
        return mapOf(
            "accessibility" to if (available) "available" else "unavailable",
            "screenshot" to if (available) "available" else "unavailable",
            "ocr" to if (available && recognizer != null) "available" else "unavailable",
            "ocr_engine" to "mlkit_chinese_bundled", "ocr_languages" to listOf("zh", "en"),
            "actions" to listOf("tap", "swipe", "set_text", "node_click"),
            "platform_operation_pending" to (pending.get() != null),
            "recovery_required" to (AgentRuntime.recoveryOperation.get() != null),
            "minimum_android_api" to 30,
            "display_scope" to "default_only",
        )
    }

    override fun observe(scope: RequestScope, params: ObserveParams): Observation {
        scope.checkActive()
        ensureIdle(Phase.OBSERVE)
        val revision = AgentRuntime.revision.get()
        val service = AgentRuntime.accessibility
        if (service == null || closed.get()) {
            return Observation(mapOf(
                "tree" to if (params.tree) SourceResult.unavailable(TREE, ErrorCode.ACCESSIBILITY_UNAVAILABLE) else SourceResult.notRequested(TREE),
                "screenshot" to if (params.screenshot) SourceResult.unavailable(SCREEN, ErrorCode.ACCESSIBILITY_UNAVAILABLE) else SourceResult.notRequested(SCREEN),
                "ocr" to if (params.ocr) SourceResult.unavailable(OCR, ErrorCode.ACCESSIBILITY_UNAVAILABLE) else SourceResult.notRequested(OCR),
            ), capabilities(), revision, AgentRuntime.revision.get())
        }
        val tree = if (params.tree) captureTree(scope, service) else SourceResult.notRequested(TREE)
        val imageSources = if (params.screenshot || params.ocr) scope.await(captureImage(scope, service, params))
            else Pair(SourceResult.notRequested(SCREEN), SourceResult.notRequested(OCR))
        scope.checkActive()
        return Observation(mapOf("tree" to tree, "screenshot" to imageSources.first, "ocr" to imageSources.second),
            capabilities(), revision, AgentRuntime.revision.get())
    }

    private fun captureTree(scope: RequestScope, service: AgentAccessibilityService): SourceResult {
        val treeLease = Any()
        if (!pending.compareAndSet(null, treeLease)) throw AgentException(AgentError(ErrorCode.BUSY, Phase.OBSERVE, true))
        try { return readTree(scope, service) }
        finally { pending.compareAndSet(treeLease, null) }
    }

    private fun readTree(scope: RequestScope, service: AgentAccessibilityService): SourceResult {
        val started = SystemClock.uptimeMillis()
        val reasons = linkedSetOf("native_semantics_only_visual_coverage_unverified")
        val nodes = mutableListOf<Map<String, Any?>>()
        var textBudget = MAX_TREE_TEXT_CHARS
        fun text(value: CharSequence?): String? {
            if (value == null) return null
            val length = minOf(value.length, MAX_NODE_CHARS, textBudget)
            if (length < value.length) reasons.add("text_truncated")
            textBudget -= length
            return value.subSequence(0, length).toString()
        }
        fun visit(node: AccessibilityNodeInfo, path: List<Int>) {
            try {
                scope.checkActive()
                val bounds = Rect().also(node::getBoundsInScreen)
                val cls = node.className?.toString().orEmpty()
                if (cls.contains("WebView", true)) reasons.add("webview_semantics_limited")
                if (cls.contains("flutter", true) || cls.endsWith("SurfaceView") || cls.endsWith("TextureView")) {
                    reasons.add("flutter_or_surface_semantics_limited")
                }
                nodes.add(mapOf(
                    "node_path" to path, "source" to TREE, "view_id" to text(node.viewIdResourceName),
                    "class_name" to text(node.className), "package_name" to text(node.packageName),
                    "text" to if (node.isPassword) null else text(node.text),
                    "content_description" to if (node.isPassword) null else text(node.contentDescription),
                    "text_redacted" to node.isPassword, "bounds" to bounds(bounds),
                    "states" to mapOf(
                        "enabled" to node.isEnabled, "checked" to if (node.isCheckable) node.isChecked else null,
                        "checkable" to node.isCheckable, "clickable" to node.isClickable,
                        "editable" to node.isEditable, "focused" to node.isFocused,
                        "showing_hint_text" to node.isShowingHintText,
                        "selected" to node.isSelected, "visible" to node.isVisibleToUser,
                    ),
                ))
                if (path.size >= MAX_TREE_DEPTH && node.childCount > 0) { reasons.add("depth_limit"); return }
                for (index in 0 until minOf(node.childCount, MAX_TREE_NODES)) {
                    scope.checkActive()
                    if (nodes.size >= MAX_TREE_NODES) { reasons.add("node_limit"); break }
                    val child = node.getChild(index)
                    if (child == null) reasons.add("missing_child") else visit(child, path + index)
                }
                if (node.childCount > MAX_TREE_NODES) reasons.add("node_limit")
            } finally { node.recycle() }
        }
        return try {
            scope.checkActive()
            if (AgentRuntime.accessibility !== service) throw AgentException(AgentError(ErrorCode.ACCESSIBILITY_UNAVAILABLE, Phase.OBSERVE))
            val active = service.rootInActiveWindow
            // Some launchers (vivo com.bbk.launcher2) make rootInActiveWindow null while still
            // exposing their windows through getWindows(); prefer the active window when the
            // single-root call cannot resolve one, instead of reporting the page as unavailable.
            val diagnostics = linkedSetOf<String>()
            val root = if (active != null) active else activeWindowRoot(service, scope, diagnostics)
            reasons.addAll(diagnostics)
            if (root == null) {
                return SourceResult(TREE, CapabilityStatus.UNAVAILABLE,
                    error = AgentError(ErrorCode.WINDOW_UNAVAILABLE, Phase.OBSERVE),
                    reasons = diagnostics.toList())
            }
            try { scope.checkActive(); requireDefaultDisplay(root, Phase.OBSERVE) }
            catch (e: Exception) { root.recycle(); throw e }
            val windowId = root.windowId
            if (active == null) reasons.add("active_window_resolved_from_windows")
            visit(root, emptyList())
            SourceResult(TREE, CapabilityStatus.AVAILABLE,
                data = mapOf("window_id" to windowId, "display_id" to Display.DEFAULT_DISPLAY, "coordinate_space" to "display_pixels", "coverage" to "exposed_accessibility_nodes_only", "nodes" to nodes),
                reasons = reasons.toList(), startedAtMs = started, finishedAtMs = SystemClock.uptimeMillis())
        } catch (e: AgentException) {
            if (e.error.code == ErrorCode.UNSUPPORTED_DISPLAY) return SourceResult.unavailable(TREE, ErrorCode.UNSUPPORTED_DISPLAY)
            throw e
        }
        catch (_: Throwable) { SourceResult.failed(TREE, ErrorCode.PLATFORM_FAILED) }
    }

    private fun captureImage(scope: RequestScope, service: AgentAccessibilityService, params: ObserveParams): CompletableFuture<Pair<SourceResult, SourceResult>> {
        val started = SystemClock.uptimeMillis()
        val call = post<Pair<SourceResult, SourceResult>>(scope) { operation ->
            requireService(service, Phase.OBSERVE)
            val root = service.rootInActiveWindow
            if (root == null) {
                operation.complete(imageFailure(params, ErrorCode.WINDOW_UNAVAILABLE, unavailable = true))
                return@post
            }
            try { scope.checkActive(); requireDefaultDisplay(root, Phase.OBSERVE) }
            catch (e: AgentException) {
                if (e.error.code == ErrorCode.UNSUPPORTED_DISPLAY) {
                    operation.complete(imageFailure(params, ErrorCode.UNSUPPORTED_DISPLAY, unavailable = true))
                    return@post
                }
                throw e
            } finally { root.recycle() }
            scope.checkActive()
            val now = SystemClock.uptimeMillis()
            if (now - lastScreenshotAt < 1000) {
                operation.complete(imageFailure(params, ErrorCode.SCREENSHOT_RATE_LIMITED, unavailable = true))
            } else {
                scope.dispatch {
                    lastScreenshotAt = now
                    service.takeScreenshot(Display.DEFAULT_DISPLAY, callbackExecutor, object : AccessibilityService.TakeScreenshotCallback {
                    override fun onSuccess(screenshot: AccessibilityService.ScreenshotResult) {
                        processImage(scope, operation, screenshot, params, started)
                    }

                    override fun onFailure(errorCode: Int) {
                        val code = if (Build.VERSION.SDK_INT >= 34 && errorCode == AccessibilityService.ERROR_TAKE_SCREENSHOT_SECURE_WINDOW) ErrorCode.SCREEN_PROTECTED else when (errorCode) {
                            AccessibilityService.ERROR_TAKE_SCREENSHOT_INTERVAL_TIME_SHORT -> ErrorCode.SCREENSHOT_RATE_LIMITED
                            else -> ErrorCode.SCREENSHOT_UNAVAILABLE
                        }
                        operation.complete(imageFailure(params, code))
                    }
                    })
                }
            }
        }
        return call.result
    }

    private fun processImage(
        scope: RequestScope, call: PlatformOperation<Pair<SourceResult, SourceResult>>,
        screenshot: AccessibilityService.ScreenshotResult, params: ObserveParams, started: Long,
    ) {
        var image: Bitmap? = null
        var ownedByOcr = false
        var result: Pair<SourceResult, SourceResult>? = null
        var failure: AgentException? = null
        try {
            scope.checkActive()
            val originalWidth = screenshot.hardwareBuffer.width
            val originalHeight = screenshot.hardwareBuffer.height
            image = softwareImage(screenshot)
            scope.checkActive()
            val metadata = mapOf(
                "width" to image.width, "height" to image.height, "original_width" to originalWidth, "original_height" to originalHeight,
                "coordinate_space" to "screenshot_pixels", "scale_x" to image.width.toDouble() / originalWidth,
                "scale_y" to image.height.toDouble() / originalHeight,
                "display_id" to Display.DEFAULT_DISPLAY,
                "protected_content" to "not_verified",
                "captured_at_uptime_ms" to screenshot.timestamp,
            )
            val screen = if (params.screenshot) {
                try {
                    val encoded = LimitedImageOutput()
                    if (!image.compress(Bitmap.CompressFormat.PNG, 100, encoded)) throw AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.OBSERVE))
                    SourceResult(SCREEN, CapabilityStatus.AVAILABLE, metadata + mapOf(
                        "format" to "png", "base64" to Base64.encodeToString(encoded.toByteArray(), Base64.NO_WRAP),
                    ), reasons = listOf("protected_content_not_verified"), startedAtMs = started, finishedAtMs = SystemClock.uptimeMillis())
                } catch (e: AgentException) { SourceResult.failed(SCREEN, e.error.code) }
                catch (_: Exception) { SourceResult.failed(SCREEN, ErrorCode.PLATFORM_FAILED) }
            } else SourceResult.notRequested(SCREEN)
            scope.checkActive()
            val engine = recognizer
            if (!params.ocr) result = Pair(screen, SourceResult.notRequested(OCR))
            else if (engine == null) result = Pair(screen, SourceResult.unavailable(OCR, ErrorCode.OCR_UNAVAILABLE))
            else {
                val ocrImage = image
                val ocrStarted = SystemClock.uptimeMillis()
                val task = scope.dispatch { engine.process(InputImage.fromBitmap(ocrImage, 0)) }
                ownedByOcr = true
                task.addOnCompleteListener(callbackExecutor) { completed ->
                    var ocr: SourceResult? = null
                    var stopped: AgentException? = null
                    try {
                        scope.checkActive()
                        if (!completed.isSuccessful) ocr = SourceResult.failed(OCR, ErrorCode.OCR_FAILED)
                        else {
                            val blocks = completed.result.textBlocks
                            val reasons = mutableListOf("ocr_text_only_no_native_states")
                            if (blocks.size > MAX_OCR_BLOCKS) reasons.add("ocr_block_limit")
                            val data = blocks.take(MAX_OCR_BLOCKS).map { block ->
                                if (block.text.length > MAX_NODE_CHARS) reasons.add("ocr_text_truncated")
                                mapOf("text" to block.text.take(MAX_NODE_CHARS), "bounds" to block.boundingBox?.let(::bounds), "confidence" to null)
                            }
                            ocr = SourceResult(OCR, CapabilityStatus.AVAILABLE, metadata + mapOf("blocks" to data),
                                reasons = reasons.distinct(), startedAtMs = ocrStarted, finishedAtMs = SystemClock.uptimeMillis())
                        }
                    } catch (e: AgentException) { stopped = e }
                    catch (_: Exception) { ocr = SourceResult.failed(OCR, ErrorCode.OCR_FAILED) }
                    finally { ocrImage.recycle() }
                    if (stopped != null) call.fail(stopped!!) else call.complete(Pair(screen, checkNotNull(ocr)))
                }
            }
        } catch (e: AgentException) {
            if (e.error.code == ErrorCode.IMAGE_TOO_LARGE || e.error.code == ErrorCode.PLATFORM_FAILED) result = imageFailure(params, e.error.code)
            else failure = e
        } catch (_: Exception) { result = imageFailure(params, ErrorCode.SCREENSHOT_UNAVAILABLE) }
        finally {
            screenshot.hardwareBuffer.close()
            if (!ownedByOcr) image?.recycle()
        }
        if (failure != null) call.fail(failure!!) else result?.let(call::complete)
    }

    private fun softwareImage(screenshot: AccessibilityService.ScreenshotResult): Bitmap {
        val buffer = screenshot.hardwareBuffer
        if (buffer.width.toLong() * buffer.height > MAX_SOURCE_PIXELS) throw AgentException(AgentError(ErrorCode.IMAGE_TOO_LARGE, Phase.OBSERVE))
        val hardware = Bitmap.wrapHardwareBuffer(buffer, screenshot.colorSpace)
            ?: throw AgentException(AgentError(ErrorCode.SCREENSHOT_UNAVAILABLE, Phase.OBSERVE))
        var software: Bitmap? = null
        try {
            software = hardware.copy(Bitmap.Config.ARGB_8888, false)
                ?: throw AgentException(AgentError(ErrorCode.SCREENSHOT_UNAVAILABLE, Phase.OBSERVE))
            val scale = minOf(1.0, sqrt(MAX_IMAGE_PIXELS.toDouble() / (software.width.toLong() * software.height)))
            val resized = Bitmap.createScaledBitmap(software, (software.width * scale).toInt().coerceAtLeast(1), (software.height * scale).toInt().coerceAtLeast(1), true)
            if (resized !== software) software.recycle()
            software = null
            return resized
        } finally { hardware.recycle(); software?.recycle() }
    }

    private fun imageFailure(params: ObserveParams, code: ErrorCode, unavailable: Boolean = false): Pair<SourceResult, SourceResult> = Pair(
        if (!params.screenshot) SourceResult.notRequested(SCREEN) else if (unavailable) SourceResult.unavailable(SCREEN, code) else SourceResult.failed(SCREEN, code),
        if (params.ocr) SourceResult.unavailable(OCR, ErrorCode.SCREENSHOT_UNAVAILABLE) else SourceResult.notRequested(OCR),
    )

    override fun dispatch(scope: RequestScope, params: ActionParams): CompletableFuture<ActionCompletion>? {
        scope.checkActive()
        ensureIdle(Phase.DISPATCH)
        val service = AgentRuntime.accessibility ?: throw AgentException(AgentError(ErrorCode.ACCESSIBILITY_UNAVAILABLE, Phase.DISPATCH))
        val committed = CompletableFuture<Unit>()
        val call = post<ActionCompletion>(scope) { operation ->
            try {
                requireService(service, Phase.DISPATCH)
                val root = service.rootInActiveWindow ?: throw AgentException(AgentError(ErrorCode.WINDOW_UNAVAILABLE, Phase.DISPATCH))
                val isGesture = params.kind == "tap" || params.kind == "swipe"
                val dispatched = try {
                    scope.checkActive()
                    requireDefaultDisplay(root, Phase.DISPATCH)
                    if (root.packageName?.toString() != params.expectedPackage) throw AgentException(AgentError(ErrorCode.TARGET_CHANGED, Phase.DISPATCH))
                    if (isGesture) {
                        val rect = Rect().also(root::getBoundsInScreen)
                        if (!rect.contains(params.x, params.y) || !rect.contains(params.endX, params.endY)) {
                            throw AgentException(AgentError(ErrorCode.TARGET_UNAVAILABLE, Phase.DISPATCH))
                        }
                        if (scope.remainingMs < params.durationMs + 250) throw AgentException(AgentError(ErrorCode.TIMEOUT, Phase.DISPATCH))
                        val path = Path().apply {
                            moveTo(params.x.toFloat(), params.y.toFloat())
                            if (params.kind == "swipe") lineTo(params.endX.toFloat(), params.endY.toFloat())
                        }
                        val gesture = GestureDescription.Builder().addStroke(GestureDescription.StrokeDescription(path, 0, params.durationMs)).build()
                        scope.commitAction {
                            if (scope.remainingMs < params.durationMs + 250) throw AgentException(AgentError(ErrorCode.TIMEOUT, Phase.DISPATCH))
                            operation.actionInvoked = true
                            committed.complete(Unit)
                            service.dispatchGesture(gesture, object : AccessibilityService.GestureResultCallback() {
                                override fun onCompleted(gestureDescription: GestureDescription?) { operation.complete(ActionCompletion("gesture_callback")) }
                                override fun onCancelled(gestureDescription: GestureDescription?) {
                                    operation.complete(ActionCompletion("gesture_callback", AgentError(ErrorCode.GESTURE_CANCELLED, Phase.COMPLETION)))
                                }
                            }, main)
                        }
                    } else nodeAction(scope, root, params) {
                        operation.actionInvoked = true
                        committed.complete(Unit)
                    }
                } finally { root.recycle() }
                if (!isGesture) {
                    if (dispatched) operation.complete(ActionCompletion("accessibility_action_return"))
                    else operation.quarantine(ActionCompletion("accessibility_action_return", AgentError(ErrorCode.ACTION_UNCERTAIN, Phase.COMPLETION)))
                } else if (!dispatched) operation.complete(ActionCompletion("gesture_dispatch_return", AgentError(ErrorCode.DISPATCH_REJECTED, Phase.DISPATCH)))
            } catch (e: AgentException) { committed.completeExceptionally(e); operation.fail(e) }
            catch (_: Exception) {
                val failure = AgentException(AgentError(ErrorCode.PLATFORM_FAILED, Phase.DISPATCH))
                committed.completeExceptionally(failure); operation.fail(failure)
            }
        }
        call.result.whenComplete { _, error -> if (error != null) committed.completeExceptionally(error) }
        scope.await(committed)
        return call.result
    }

    private fun nodeAction(scope: RequestScope, root: AccessibilityNodeInfo, params: ActionParams, onCommit: () -> Unit): Boolean {
        var target = root
        try {
            for (index in params.nodePath) {
                scope.checkActive()
                val child = target.getChild(index) ?: throw AgentException(AgentError(ErrorCode.TARGET_UNAVAILABLE, Phase.DISPATCH))
                if (target !== root) target.recycle()
                target = child
            }
            if (!target.isEnabled || !target.isVisibleToUser) throw AgentException(AgentError(ErrorCode.TARGET_UNAVAILABLE, Phase.DISPATCH))
            return if (params.kind == "set_text") {
                if (!target.isEditable) throw AgentException(AgentError(ErrorCode.TARGET_UNAVAILABLE, Phase.DISPATCH))
                val arguments = Bundle().apply {
                    putCharSequence(AccessibilityNodeInfo.ACTION_ARGUMENT_SET_TEXT_CHARSEQUENCE, params.text)
                }
                scope.commitAction {
                    onCommit()
                    target.performAction(AccessibilityNodeInfo.ACTION_SET_TEXT, arguments)
                }
            } else {
                if (!target.isClickable) throw AgentException(AgentError(ErrorCode.TARGET_UNAVAILABLE, Phase.DISPATCH))
                scope.commitAction {
                    onCommit()
                    target.performAction(AccessibilityNodeInfo.ACTION_CLICK)
                }
            }
        } finally { if (target !== root) target.recycle() }
    }

    private fun requireService(service: AgentAccessibilityService, phase: Phase) {
        if (closed.get() || AgentRuntime.accessibility !== service) throw AgentException(AgentError(ErrorCode.ACCESSIBILITY_UNAVAILABLE, phase))
    }

    /**
     * Fallback root for pages that [AccessibilityService.getRootInActiveWindow] cannot resolve.
     * Only the default display is considered, only a window the platform marks active (or, when
     * none is marked, the topmost application window) is chosen, and every non-chosen window is
     * recycled. Returns null when nothing on the default display can supply a root.
     */
    private fun activeWindowRoot(
        service: AgentAccessibilityService,
        scope: RequestScope,
        diagnostics: MutableSet<String>,
    ): AccessibilityNodeInfo? {
        val windows = try { service.windows } catch (e: Exception) {
            diagnostics.add("windows_exception_" + (e.javaClass.simpleName ?: "unknown"))
            return null
        }
        if (windows.isNullOrEmpty()) {
            diagnostics.add("windows_empty")
            return null
        }
        diagnostics.add("windows_count_" + minOf(windows.size, 9))
        var chosen: AccessibilityWindowInfo? = null
        var chosenRoot: AccessibilityNodeInfo? = null
        try {
            for (window in windows) {
                scope.checkActive()
                val usable = window.displayId == Display.DEFAULT_DISPLAY &&
                    (window.type == AccessibilityWindowInfo.TYPE_APPLICATION || window.isActive)
                if (!usable) {
                    diagnostics.add("window_skipped_type_" + window.type + "_active_" + window.isActive)
                    continue
                }
                // Prefer an explicitly active window; otherwise the first application window,
                // which the platform orders front-to-back.
                if (chosen == null || (window.isActive && !chosen.isActive)) {
                    val candidate: AccessibilityNodeInfo? = try {
                        window.root
                    } catch (e: Exception) {
                        diagnostics.add("root_exception_" + (e.javaClass.simpleName ?: "unknown"))
                        null
                    }
                    if (candidate == null) {
                        diagnostics.add("root_null")
                        continue
                    }
                    chosenRoot?.recycle()
                    chosen = window
                    chosenRoot = candidate
                    if (window.isActive) break
                }
            }
        } finally {
            windows.forEach { it.recycle() }
        }
        return chosenRoot
    }

    private fun requireDefaultDisplay(root: AccessibilityNodeInfo, phase: Phase) {
        val window = root.window ?: throw AgentException(AgentError(ErrorCode.UNSUPPORTED_DISPLAY, phase))
        val displayId = try { window.displayId } finally { window.recycle() }
        if (displayId != Display.DEFAULT_DISPLAY) throw AgentException(AgentError(ErrorCode.UNSUPPORTED_DISPLAY, phase))
    }

    private fun ensureIdle(phase: Phase) {
        // A cancelled OS call remains quarantined until its callback releases this exact lease.
        if (pending.get() != null) throw AgentException(AgentError(ErrorCode.BUSY, phase, true))
    }

    private fun <T> post(scope: RequestScope, start: (PlatformOperation<T>) -> Unit): PlatformOperation<T> {
        val call = PlatformOperation<T>(pending, AgentRuntime.recoveryOperation, ownedPending, main::post, main::removeCallbacks)
        if (!call.acquire()) throw AgentException(AgentError(ErrorCode.BUSY, scope.phase, true))
        scope.onStopped { call.abortBeforeStart() }
        call.enqueue(scope, start)
        return call
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        ownedPending.get()?.abortBeforeStart()
        recognizer?.close()
        callbacks.shutdown()
    }

    private class LimitedImageOutput : ByteArrayOutputStream() {
        override fun write(value: Int) {
            if (size() >= MAX_PNG_BYTES) throw AgentException(AgentError(ErrorCode.IMAGE_TOO_LARGE, Phase.OBSERVE))
            super.write(value)
        }
        override fun write(bytes: ByteArray, offset: Int, length: Int) {
            if (length > MAX_PNG_BYTES - size()) throw AgentException(AgentError(ErrorCode.IMAGE_TOO_LARGE, Phase.OBSERVE))
            super.write(bytes, offset, length)
        }
    }

    /**
     * Android reports an inverted rectangle for nodes that are laid out but off-screen or
     * fully clipped (right < left, or bottom < top). That is a real "not on screen" state, not
     * malformed data, so it is normalised to an empty rectangle at the origin corner. Passing
     * it through unchanged would make the Runtime reject the whole observation as invalid.
     */
    private fun bounds(rect: Rect): Map<String, Int> {
        val right = if (rect.right < rect.left) rect.left else rect.right
        val bottom = if (rect.bottom < rect.top) rect.top else rect.bottom
        return mapOf("left" to rect.left, "top" to rect.top, "right" to right, "bottom" to bottom)
    }

    companion object {
        private const val TREE = "accessibility"
        private const val SCREEN = "accessibility_screenshot"
        private const val OCR = "mlkit_chinese"
        const val MAX_TREE_NODES = 512
        const val MAX_TREE_DEPTH = 32
        const val MAX_NODE_CHARS = 256
        const val MAX_TREE_TEXT_CHARS = 32 * 1024
        const val MAX_OCR_BLOCKS = 256
        const val MAX_SOURCE_PIXELS = 8_000_000L
        const val MAX_IMAGE_PIXELS = 1_500_000
        const val MAX_PNG_BYTES = 4 * 1024 * 1024
    }
}
