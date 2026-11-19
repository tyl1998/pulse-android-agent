package com.pulse.agent.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.IBinder
import android.os.Build
import android.system.Os
import android.system.OsConstants
import android.util.Log
import com.pulse.agent.BuildConfig
import com.pulse.agent.R
import com.pulse.agent.transport.LocalSocketServer
import com.pulse.agent.transport.Router
import com.pulse.agent.transport.PeerPolicy
import com.pulse.agent.protocol.AgentException
import com.pulse.agent.execution.DeviceCoordinator
import com.pulse.agent.protocol.AgentError
import com.pulse.agent.protocol.ErrorCode
import com.pulse.agent.protocol.Phase
import com.pulse.agent.protocol.SafeLog
import com.pulse.agent.protocol.BoundedErrorLog
import java.io.File
import java.io.IOException

/** Owns the private token, socket and single-device executor lifetimes. Not exported. */
class AgentForegroundService : Service() {

    private var server: LocalSocketServer? = null
    private var errors: BoundedErrorLog? = null

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (Build.VERSION.SDK_INT >= 34) {
            startForeground(NOTIFICATION_ID, buildNotification(), ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE)
        } else startForeground(NOTIFICATION_ID, buildNotification())
        if (server == null) {
            errors?.close()
            val sessionErrors = BoundedErrorLog { bytes, publish ->
                val temporary = File(filesDir, "$ERROR_FILE.new")
                try {
                    openFileOutput(temporary.name, MODE_PRIVATE).use {
                        Os.fchmod(it.fd, OsConstants.S_IRUSR or OsConstants.S_IWUSR)
                        it.write(bytes)
                    }
                    publish {
                        if (!temporary.renameTo(File(filesDir, ERROR_FILE))) throw IOException("error_snapshot_unavailable")
                    }
                } finally { temporary.delete() }
            }
            errors = sessionErrors
            val instance = intent?.getStringExtra(EXTRA_INSTANCE) ?: DEFAULT_INSTANCE
            try {
                val peer = try {
                    openFileInput(PEER_FILE).use { input ->
                        val bytes = ByteArray(33)
                        val size = input.read(bytes)
                        require(size in 1..32 && input.read() == -1)
                        PeerPolicy.parse(String(bytes, 0, size, Charsets.US_ASCII))
                    }
                } catch (_: Exception) { throw AgentException(AgentError(ErrorCode.PROVISIONING_REQUIRED, Phase.LIFECYCLE)) }
                File(filesDir, TOKEN_FILE).delete()
                val token = AgentRuntime.tokens.generate()
                val backend = AndroidDeviceBackend()
                val coordinator = DeviceCoordinator(backend, logError = { Log.e(TAG, it); sessionErrors.append(it) })
                AgentRuntime.coordinator = coordinator
                server = LocalSocketServer(
                    instance = instance,
                    router = Router(AgentRuntime.tokens, BuildConfig.PROTOCOL_VERSION, BuildConfig.VERSION_NAME, peer) {
                        backend.capabilities() + ("error_log_session_id" to sessionErrors.sessionId)
                    },
                    coordinator = coordinator,
                    errorRecord = sessionErrors::append,
                ).also { it.start() }
                // File visibility means the exact socket was bound successfully, not just startup intent.
                openFileOutput(TOKEN_FILE, MODE_PRIVATE).use {
                    Os.fchmod(it.fd, OsConstants.S_IRUSR or OsConstants.S_IWUSR)
                    it.write(token.toByteArray(Charsets.US_ASCII))
                }
            } catch (e: Exception) {
                val line = SafeLog.error(null, (e as? AgentException)?.error ?: AgentError(ErrorCode.PLATFORM_FAILED, Phase.LIFECYCLE))
                Log.e(TAG, line)
                sessionErrors.append(line)
                AgentRuntime.tokens.clear()
                File(filesDir, TOKEN_FILE).delete()
                server?.stop()
                server = null
                AgentRuntime.coordinator?.close()
                AgentRuntime.coordinator = null
                stopSelf()
            }
        }
        return START_NOT_STICKY
    }

    override fun onDestroy() {
        AgentRuntime.tokens.clear()
        File(filesDir, TOKEN_FILE).delete()
        server?.stop()
        server = null
        AgentRuntime.coordinator?.close()
        AgentRuntime.coordinator = null
        errors?.close()
        errors = null
        super.onDestroy()
    }

    private fun buildNotification(): Notification {
        val nm = getSystemService(NotificationManager::class.java)
        nm.createNotificationChannel(
            NotificationChannel(CHANNEL_ID, getString(R.string.notification_channel_name), NotificationManager.IMPORTANCE_LOW),
        )
        return Notification.Builder(this, CHANNEL_ID)
            .setContentTitle(getString(R.string.notification_running))
            .setSmallIcon(android.R.drawable.stat_notify_sync)
            .setOngoing(true)
            .build()
    }

    companion object {
        const val EXTRA_INSTANCE = "com.pulse.agent.extra.INSTANCE"
        const val DEFAULT_INSTANCE = "default"
        const val TOKEN_FILE = "agent-token"
        const val PEER_FILE = "adb-peer"
        const val ERROR_FILE = "agent-errors"
        private const val CHANNEL_ID = "pulse_agent"
        private const val NOTIFICATION_ID = 1
        private const val TAG = "PulseAgentFgs"
    }
}
