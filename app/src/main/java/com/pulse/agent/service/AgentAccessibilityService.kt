package com.pulse.agent.service

import android.accessibilityservice.AccessibilityService
import android.content.Intent
import android.view.accessibility.AccessibilityEvent
import com.pulse.agent.protocol.ErrorCode

/** Events only mark revisions; capture never runs on the accessibility callback thread. */
class AgentAccessibilityService : AccessibilityService() {

    override fun onServiceConnected() {
        super.onServiceConnected()
        AgentRuntime.accessibility = this
        // 无障碍服务豁免后台启动限制；由它拉起前台服务，前台服务不导出。
        startForegroundService(Intent(this, AgentForegroundService::class.java))
    }

    override fun onAccessibilityEvent(event: AccessibilityEvent?) {
        AgentRuntime.revision.incrementAndGet()
    }

    override fun onInterrupt() { AgentRuntime.coordinator?.cancelAll(ErrorCode.ACCESSIBILITY_INTERRUPTED) }

    override fun onUnbind(intent: Intent?): Boolean {
        disconnect()
        return super.onUnbind(intent)
    }

    override fun onDestroy() {
        disconnect()
        super.onDestroy()
    }

    private fun disconnect() {
        if (AgentRuntime.accessibility !== this) return
        AgentRuntime.accessibility = null
        AgentRuntime.coordinator?.cancelAll(ErrorCode.ACCESSIBILITY_UNAVAILABLE)
        stopService(Intent(this, AgentForegroundService::class.java))
    }
}
