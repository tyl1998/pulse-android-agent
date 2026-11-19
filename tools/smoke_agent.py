#!/usr/bin/env python3
"""P1.0 Agent smoke, using only Python stdlib plus adb and host lsof.

Run explicitly with --serial and --evidence. --check-only validates the debug APK without
ADB, sockets, installation, settings changes, or writing evidence. This is not
fixture P0.4/P0.7 acceptance, a Runtime driver, or a device fault-injection tool.
"""

import argparse
import base64
import binascii
from collections import Counter
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import select
import shutil
import socket
import struct
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET
import zipfile


ROOT = Path(__file__).resolve().parents[1]
AGENT = "com.pulse.agent"
FIXTURE = "com.pulse.target"
SERVICE = AGENT + "/com.pulse.agent.service.AgentAccessibilityService"
PROBE = "com.pulse.agent.debug.LocalPeerProbe"
REMOTE = "localabstract:pulse-default"
MAX_MESSAGE = 64 * 1024
MAX_RESPONSE = 8 * 1024 * 1024
MAX_PNG = 4 * 1024 * 1024
UUID_RE = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\Z")
PHASES = set("transport auth protocol queue observe dispatch completion lifecycle".split())
CODES = set((
    "unauthorized forbidden_peer provisioning_required invalid_message message_too_large unsupported_protocol "
    "not_found method_not_allowed busy timeout cancelled disconnected stopped "
    "accessibility_unavailable accessibility_interrupted window_unavailable unsupported_display target_changed "
    "target_unavailable dispatch_rejected gesture_cancelled action_uncertain platform_failed not_requested "
    "screenshot_unavailable screenshot_rate_limited screen_protected image_too_large "
    "ocr_unavailable ocr_failed response_too_large internal_error"
).split())
REQUIRED = (
    "apk_preflight", "device_ready", "install_agent", "private_peer_bootstrap", "accessibility_start",
    "loopback_binding", "authenticated_health", "missing_token", "wrong_token",
    "unknown_path", "unsupported_protocol", "malformed_message", "oversized_message",
    "native_tree", "unrequested_ocr", "exact_unicode_name", "checkbox_callback_tap",
    "screenshot_and_ocr", "queued_cancel", "queued_timeout", "queued_disconnect",
    "fixture_state_restored", "webview_partial", "flutter_partial", "app_uid_peer", "same_uid_wrong_pid_peer",
    "sensitive_logs", "owned_cleanup",
)
SENSITIVE_KEYS = {
    "text", "content_description", "base64", "authorization", "token", "password",
    "screenshot_bytes", "encoded_content",
}


class SmokeError(Exception):
    def __init__(self, code, environmental=False):
        super().__init__(code)
        self.code = code
        self.environmental = environmental


def require(condition, code, environmental=False):
    if not condition:
        raise SmokeError(code, environmental)


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def valid_id(value):
    return isinstance(value, str) and UUID_RE.fullmatch(value) is not None


def sensitive_key(key):
    key = str(key).lower().replace("-", "_")
    return key in SENSITIVE_KEYS or key.endswith("_token") or "authorization" in key


def redact(value, secrets=()):
    """Defense in depth; evidence producers also use explicit field allowlists."""
    if isinstance(value, dict):
        return {str(k): redact(v, secrets) for k, v in value.items() if not sensitive_key(k)}
    if isinstance(value, (list, tuple)):
        return [redact(v, secrets) for v in value]
    if isinstance(value, str):
        for secret in secrets:
            if secret:
                value = value.replace(secret, "[redacted]")
        return value
    if value is None or type(value) in (bool, int, float):
        return value
    return "[redacted]"


class Secrets:
    def __init__(self):
        self.values = set()
        self.credentials = set()

    def credential(self, value):
        self.values.add(value)
        self.credentials.add(value)

    def remember(self, value):
        if isinstance(value, dict):
            for key, child in value.items():
                if sensitive_key(key) and isinstance(child, str) and child:
                    self.values.add(child)
                else:
                    self.remember(child)
        elif isinstance(value, list):
            for child in value:
                self.remember(child)


class Evidence:
    def __init__(self, serial):
        self.secrets = Secrets()
        self.data = {
            "scope": "user-authorized-P1.0-only", "serial": serial,
            "started_at": timestamp(), "cases": [], "commands": [], "responses": [],
            "limitations": [
                "No P0.4 acceptance/reset/fault injection, P0.7, Runtime/DSL, or P1.2 driver.",
                "An already dispatched OS gesture is irreversible; cancellation is not rollback.",
                "LAN probe is same-host only, not cross-machine LAN isolation proof.",
                "Real dispatch-failure and gesture-cancel callbacks are not deterministically injected.",
                "APK installation is retained; only owned settings/forward/private peer configuration are restored.",
                "PID/token changes or native uncertainty require explicit new provisioning/recovery, not automatic replay.",
            ],
        }

    def case(self, name, function, required=True):
        item = {"name": name, "required": required, "started_at": timestamp()}
        self.data["cases"].append(item)
        try:
            result = function()
            item.update(status="passed", details=redact(result or {}, self.secrets.credentials))
            return result
        except SmokeError as exc:
            item.update(status="blocked" if exc.environmental else "failed", reason=exc.code)
        except Exception as exc:
            # Exception text can contain response bodies, UI text, or subprocess output.
            item.update(status="failed", reason="host_" + type(exc).__name__)
        finally:
            item["finished_at"] = timestamp()
        return None

    def must(self, name, function):
        result = self.case(name, function)
        if self.data["cases"][-1]["status"] != "passed":
            raise SmokeError("prerequisite_unmet", True)
        return result

    def skip(self, name, reason):
        self.data["cases"].append({
            "name": name, "required": False, "status": "not_exercised", "reason": reason,
            "finished_at": timestamp(),
        })

    def finish(self):
        existing = {item["name"] for item in self.data["cases"]}
        for name in REQUIRED:
            if name not in existing:
                self.data["cases"].append({
                    "name": name, "required": True, "status": "blocked",
                    "reason": "prerequisite_unmet", "finished_at": timestamp(),
                })
        passed = all(item["status"] == "passed" for item in self.data["cases"] if item["required"])
        self.data.update(status="passed" if passed else "failed", finished_at=timestamp())
        return redact(self.data, self.secrets.credentials)


def command(args, evidence, label, *, input_bytes=None, timeout=15, check=True):
    """Capture every stream in memory. Never persist stdout/stderr or input."""
    record = {"label": label, "argv": list(args), "started_at": timestamp()}
    evidence.data["commands"].append(record)
    if input_bytes is not None:
        record["stdin"] = "redacted"
    started = time.monotonic()
    try:
        result = subprocess.run(
            args, input=input_bytes, stdin=subprocess.DEVNULL if input_bytes is None else None,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False,
        )
        record["exit_code"] = result.returncode
        if check:
            require(result.returncode == 0, "command_failed_" + label, True)
        return result
    except subprocess.TimeoutExpired:
        record.update(exit_code=None, timed_out=True)
        raise SmokeError("command_timeout_" + label, True) from None
    except OSError:
        record["exit_code"] = None
        raise SmokeError("command_unavailable_" + label, True) from None
    finally:
        record.update(finished_at=timestamp(), elapsed_ms=round((time.monotonic() - started) * 1000))


class Adb:
    def __init__(self, executable, serial, evidence):
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", serial) is not None, "invalid_serial")
        self.executable, self.serial, self.evidence = executable, serial, evidence

    def run(self, *args, label, **kwargs):
        return command([self.executable, "-s", self.serial, *map(str, args)], self.evidence, label, **kwargs)

    def setting(self, key):
        return self.run("shell", "settings", "get", "secure", key, label="read_" + key).stdout.decode("utf-8").strip()

    def put_setting(self, key, value):
        if value == "null":
            self.run("shell", "settings", "delete", "secure", key, label="restore_" + key)
        else:
            # Android's shell command joins arguments; quote even an empty value.
            quoted = "'" + value.replace("'", "'\\''") + "'"
            self.run("shell", "settings", "put", "secure", key, quoted, label="write_" + key)


def components(value):
    return [] if value in ("", "null") else value.split(":")


def is_agent_component(value):
    return value in (SERVICE, AGENT + "/.service.AgentAccessibilityService")


class AccessibilityLease:
    def __init__(self, adb):
        self.adb = adb
        self.original_services = adb.setting("enabled_accessibility_services")
        self.original_enabled = adb.setting("accessibility_enabled")
        self.added = False
        self.enabled_changed = False

    def enable(self):
        current = components(self.adb.setting("enabled_accessibility_services"))
        if not any(map(is_agent_component, current)):
            self.added = not any(map(is_agent_component, components(self.original_services)))
            component = next((part for part in components(self.original_services) if is_agent_component(part)), SERVICE)
            self.adb.put_setting("enabled_accessibility_services", ":".join([*current, component]))
        flag = self.adb.setting("accessibility_enabled")
        if flag != "1":
            self.enabled_changed = True
            self.adb.put_setting("accessibility_enabled", "1")
        return {"component_preexisting": not self.added, "other_services_preserved": True}

    def restore(self):
        current_raw = self.adb.setting("enabled_accessibility_services")
        current = components(current_raw)
        if self.added and any(map(is_agent_component, current)):
            current = [part for part in current if not is_agent_component(part)]
            value = ":".join(current)
            if not current and self.original_services in ("", "null"):
                value = self.original_services
            self.adb.put_setting("enabled_accessibility_services", value)
            current = components(self.adb.setting("enabled_accessibility_services"))
            require(not any(map(is_agent_component, current)), "own_accessibility_entry_not_removed", True)
        flag_restored = not self.enabled_changed
        if self.enabled_changed:
            flag = self.adb.setting("accessibility_enabled")
            # Do not overwrite another actor's new services or changed enable flag.
            unchanged_services = {SERVICE if is_agent_component(part) else part for part in current} == {
                SERVICE if is_agent_component(part) else part for part in components(self.original_services)
            }
            if unchanged_services and flag == "1":
                self.adb.put_setting("accessibility_enabled", self.original_enabled)
                flag_restored = True
            elif flag != "1":
                flag_restored = True
        return {"own_component_removed": self.added, "enable_flag_restored": flag_restored,
                "own_activation_stopped": self.enabled_changed and flag_restored and self.original_enabled != "1",
                "external_service_changes_preserved": True}


def adbd_identity(pid_output, ps_output):
    pid = pid_output.decode("ascii").strip()
    require(re.fullmatch(r"[1-9][0-9]{0,9}", pid) is not None and int(pid) <= 2147483647,
            "adbd_pid_not_single", True)
    lines = [line.split() for line in ps_output.decode("ascii").splitlines() if line.strip()]
    require(len(lines) == 2 and lines[0] == ["UID", "PID", "NAME"] and len(lines[1]) == 3,
            "adbd_ps_not_single_row", True)
    uid, actual_pid, name = lines[1]
    require(uid in ("0", "2000") and actual_pid == pid and name.rsplit("/", 1)[-1] == "adbd",
            "adbd_kernel_identity_invalid", True)
    return {"uid": int(uid), "pid": int(pid)}


def installed_base_apk(raw):
    paths = []
    for line in raw.decode("ascii").splitlines():
        require(line.startswith("package:"), "installed_apk_path_invalid", True)
        path = line[len("package:"):]
        require(re.fullmatch(r"/[A-Za-z0-9_./+=~@-]+", path) is not None and ".." not in path.split("/"),
                "installed_apk_path_unsafe", True)
        if path.endswith("/base.apk"):
            paths.append(path)
    require(len(paths) == 1, "installed_base_apk_not_single", True)
    return paths[0]


class PeerConfigLease:
    """The pin is private stdin data; restoration is conditional on file ownership."""
    def __init__(self, adb):
        self.adb = adb
        self.previous = None
        self.written = None
        self.modified = False
        self.directory_created = False

    def read(self):
        names = self.adb.run("shell", "-T", "run-as", AGENT, "ls", "files", label="peer_private_files").stdout.splitlines()
        if b"adb-peer" not in names:
            return None
        kind = self.adb.run("shell", "-T", "run-as", AGENT, "ls", "-l", "files/adb-peer",
                            label="peer_private_file_type").stdout
        require(kind.startswith(b"-"), "peer_configuration_not_regular_file", True)
        value = self.adb.run("shell", "-T", "run-as", AGENT, "cat", "files/adb-peer",
                             label="read_private_peer_configuration").stdout
        require(len(value) <= MAX_MESSAGE, "existing_peer_configuration_too_large", True)
        return value

    def provision(self, peer):
        parent = self.adb.run("shell", "-T", "run-as", AGENT, "ls", ".", label="agent_private_parent").stdout.splitlines()
        if b"files" not in parent:
            self.directory_created = True
            self.adb.run("shell", "-T", "run-as", AGENT, "mkdir", "files", label="create_agent_files")
        kind = self.adb.run("shell", "-T", "run-as", AGENT, "ls", "-ld", "files", label="agent_files_directory").stdout
        require(kind.startswith(b"d"), "agent_files_not_private_directory", True)
        self.previous = self.read()
        self.written = (str(peer["uid"]) + ":" + str(peer["pid"]) + "\n").encode("ascii")
        if self.previous != self.written:
            self.modified = True  # A timed-out tee may already have changed the file.
            result = self.adb.run("shell", "-T", "run-as", AGENT, "tee", "files/adb-peer",
                                  label="provision_private_peer", input_bytes=self.written)
            require(result.stdout == self.written and self.read() == self.written, "private_peer_write_failed", True)
        return {"configuration_preexisting": self.previous is not None, "own_modification": self.modified,
                "private_stdin_provisioning": True}

    def restore(self):
        result = {"modified": self.modified, "external_changes_preserved": True}
        if self.modified:
            current = self.read()
            if current == self.previous:
                self.modified = False
                result["already_original"] = True
            else:
                require(current == self.written, "peer_configuration_ownership_changed", True)
                if self.previous is None:
                    self.adb.run("shell", "-T", "run-as", AGENT, "rm", "-f", "files/adb-peer", label="remove_own_peer_configuration")
                else:
                    restored = self.adb.run("shell", "-T", "run-as", AGENT, "tee", "files/adb-peer",
                                           label="restore_private_peer_configuration", input_bytes=self.previous)
                    require(restored.stdout == self.previous, "private_peer_restore_echo_failed", True)
                require(self.read() == self.previous, "private_peer_restore_failed", True)
                self.modified = False
                result["original_configuration_restored"] = True
        if self.directory_created:
            removed = self.adb.run("shell", "-T", "run-as", AGENT, "rmdir", "files",
                                   label="remove_empty_owned_agent_directory", check=False)
            result["empty_owned_directory_removed"] = removed.returncode == 0
            result["nonempty_app_directory_not_deleted"] = True
        return result


def forwards(raw):
    return {tuple(line.split()) for line in raw.decode("utf-8").splitlines() if len(line.split()) == 3}


class ForwardLease:
    def __init__(self, adb):
        self.adb, self.port = adb, None
        self.creation_unknown = False

    def create(self):
        before = forwards(self.adb.run("forward", "--list", label="forward_baseline").stdout)
        self.creation_unknown = True
        result = self.adb.run("forward", "tcp:0", REMOTE, label="create_forward")
        raw = result.stdout.decode("ascii").strip()
        require(raw.isdecimal() and 0 < int(raw) < 65536, "invalid_forward_port", True)
        port = int(raw)
        require(not any(row[1] == "tcp:" + raw for row in before), "forward_not_new", True)
        self.port = port
        self.creation_unknown = False
        expected = (self.adb.serial, "tcp:" + raw, REMOTE)
        require(expected in forwards(self.adb.run("forward", "--list", label="verify_forward").stdout),
                "forward_mapping_missing", True)
        return port

    def restore(self):
        require(not self.creation_unknown, "forward_creation_outcome_unknown_no_blanket_remove", True)
        if self.port is None:
            return {"created": False}
        rows = forwards(self.adb.run("forward", "--list", label="cleanup_forward_list").stdout)
        local = "tcp:" + str(self.port)
        expected = (self.adb.serial, local, REMOTE)
        if expected in rows:
            self.adb.run("forward", "--remove", local, label="remove_own_forward")
            require(expected not in forwards(self.adb.run("forward", "--list", label="forward_removed").stdout),
                    "own_forward_not_removed")
            return {"removed": True, "port": self.port}
        require(not any(row[:2] == expected[:2] for row in rows), "forward_ownership_changed", True)
        return {"already_absent": True, "port": self.port}


def manifest_attributes(raw):
    """Read packaged Android XML (binary AXML), not a possibly stale source manifest."""
    if raw.lstrip().startswith(b"<"):
        root = ET.fromstring(raw)
        return [(element.tag, {key.rsplit("}", 1)[-1]: val for key, val in element.attrib.items()})
                for element in root.iter()]
    require(len(raw) >= 8, "manifest_truncated")
    kind, header, size = struct.unpack_from("<HHI", raw)
    require(kind == 3 and header == 8 and size == len(raw), "manifest_not_axml")
    strings, elements, pos = [], [], header
    while pos < size:
        require(pos + 8 <= size, "manifest_chunk_truncated")
        kind, head, length = struct.unpack_from("<HHI", raw, pos)
        require(head >= 8 and length >= head and pos + length <= size, "manifest_chunk_bounds")
        if kind == 1:
            require(head >= 28, "manifest_string_pool_header")
            count, _, flags, start, _ = struct.unpack_from("<IIIII", raw, pos + 8)
            require(count <= 65536 and head + count * 4 <= length and head <= start <= length,
                    "manifest_string_pool_bounds")
            strings = []
            for index in range(count):
                offset = struct.unpack_from("<I", raw, pos + head + index * 4)[0]
                cursor = pos + start + offset
                end = pos + length

                def read_length(at, utf8):
                    unit = 1 if utf8 else 2
                    require(at + unit <= end, "manifest_string_truncated")
                    first = raw[at] if utf8 else struct.unpack_from("<H", raw, at)[0]
                    high = 0x80 if utf8 else 0x8000
                    if not first & high:
                        return first, at + unit
                    require(at + 2 * unit <= end, "manifest_string_truncated")
                    second = raw[at + 1] if utf8 else struct.unpack_from("<H", raw, at + 2)[0]
                    return ((first & (high - 1)) << (8 if utf8 else 16)) | second, at + 2 * unit

                utf8 = bool(flags & 0x100)
                units, cursor = read_length(cursor, utf8)
                if utf8:
                    units, cursor = read_length(cursor, True)
                byte_length = units * (1 if utf8 else 2)
                require(cursor + byte_length + (1 if utf8 else 2) <= end, "manifest_string_bounds")
                strings.append(raw[cursor:cursor + byte_length].decode("utf-8" if utf8 else "utf-16-le"))
        elif kind == 0x102:
            require(head >= 16 and head + 20 <= length, "manifest_element_bounds")
            ext = pos + head
            name = struct.unpack_from("<I", raw, ext + 4)[0]
            attr_start, attr_size, count = struct.unpack_from("<HHH", raw, ext + 8)
            require(name < len(strings) and attr_size >= 20 and attr_start >= 20 and
                    head + attr_start + attr_size * count <= length, "manifest_attribute_bounds")
            attrs = {}
            for index in range(count):
                at = ext + attr_start + index * attr_size
                _, key, untyped = struct.unpack_from("<III", raw, at)
                value_size, _, value_type, typed = struct.unpack_from("<HBBI", raw, at + 12)
                require(key < len(strings) and value_size == 8, "manifest_attribute_invalid")
                if untyped != 0xFFFFFFFF:
                    require(untyped < len(strings), "manifest_value_bounds")
                    value = strings[untyped]
                elif value_type == 3:
                    require(typed < len(strings), "manifest_value_bounds")
                    value = strings[typed]
                elif value_type == 0x12:
                    require(typed in (0, 0xFFFFFFFF), "manifest_boolean_invalid")
                    value = typed != 0
                else:
                    value = typed
                attrs[strings[key]] = value
            elements.append((strings[name], attrs))
        pos += length
    require(elements and pos == size, "manifest_empty")
    return elements


def agent_manifest(elements):
    roots = [attrs for tag, attrs in elements if tag == "manifest"]
    require(len(roots) == 1 and roots[0].get("package") == AGENT, "apk_package_mismatch")
    require(not any(tag == "instrumentation" for tag, _ in elements), "agent_apk_contains_instrumentation")
    apps = [attrs for tag, attrs in elements if tag == "application"]
    require(len(apps) == 1, "agent_application_count")

    def boolean(attrs, key):
        value = attrs.get(key)
        require(type(value) is bool or value in ("true", "false"), "manifest_boolean_missing_" + key)
        return value is True or value == "true"

    app = apps[0]
    require(boolean(app, "debuggable"), "debug_apk_required_for_private_probe")
    for key in ("allowBackup", "fullBackupContent", "usesCleartextTraffic"):
        require(not boolean(app, key), "unsafe_manifest_" + key)
    allowed = {"android.permission.FOREGROUND_SERVICE", "android.permission.FOREGROUND_SERVICE_SPECIAL_USE",
               "android.permission.POST_NOTIFICATIONS"}
    own_permission = AGENT + ".DYNAMIC_RECEIVER_NOT_EXPORTED_PERMISSION"
    requested = {attrs.get("name") for tag, attrs in elements if tag in ("uses-permission", "uses-permission-sdk-23")}
    require(allowed <= requested and requested <= allowed | {own_permission}, "unsafe_agent_permissions")
    for tag, attrs in elements:
        if tag == "permission":
            require(attrs.get("name") == own_permission and attrs.get("protectionLevel") in (2, "signature"),
                    "unsafe_declared_permission")
    if own_permission in requested:
        require(any(tag == "permission" and attrs.get("name") == own_permission for tag, attrs in elements),
                "missing_private_permission_declaration")
    found, exported = {}, []
    for tag, attrs in elements:
        if tag not in ("activity", "activity-alias", "service", "receiver", "provider"):
            continue
        name = attrs.get("name")
        require(isinstance(name, str) and name and len(name) <= 256, "manifest_component_name")
        name = AGENT + name if name.startswith(".") else AGENT + "." + name if "." not in name else name
        require(name != PROBE and name not in found, "invalid_debug_probe_or_duplicate_component")
        visible = boolean(attrs, "exported")
        found[name] = (tag, attrs, visible)
        if visible:
            require(tag == "service" and name == SERVICE.split("/", 1)[1] and
                    attrs.get("permission") == "android.permission.BIND_ACCESSIBILITY_SERVICE", "unsafe_exported_component")
            exported.append(name)
    a11y = SERVICE.split("/", 1)[1]
    fgs = AGENT + ".service.AgentForegroundService"
    require(a11y in found and found[a11y][2] and fgs in found and found[fgs][0] == "service" and not found[fgs][2],
            "agent_service_manifest_invalid")
    require(found[fgs][1].get("foregroundServiceType") in ("specialUse", 0x40000000), "private_fgs_type_invalid")
    sdks = [attrs for tag, attrs in elements if tag == "uses-sdk"]
    require(len(sdks) == 1, "manifest_sdk_missing")
    minimum, target = sdks[0].get("minSdkVersion"), sdks[0].get("targetSdkVersion")
    require(type(minimum) is int or isinstance(minimum, str) and minimum.isdecimal(), "manifest_min_sdk_invalid")
    require(type(target) is int or isinstance(target, str) and target.isdecimal(), "manifest_target_sdk_invalid")
    require(30 <= int(minimum) <= int(target), "manifest_sdk_unsupported")
    return {"debuggable": True, "backup_disabled": True, "cleartext_disabled": True,
            "permissions": sorted(requested), "exported_components": exported,
            "accessibility_permission_guarded": True, "foreground_service_private": True,
            "debug_probe_manifest_component": False, "min_sdk": int(minimum), "target_sdk": int(target)}


def apk_info(path):
    require(path.is_file(), "apk_missing", True)
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    with zipfile.ZipFile(path) as archive:
        entry = archive.getinfo("AndroidManifest.xml")
        require(entry.file_size <= 1024 * 1024, "manifest_too_large")
        elements = manifest_attributes(archive.read(entry))
        safety = agent_manifest(elements)
        descriptor = ("L" + PROBE.replace(".", "/") + ";\0").encode("ascii")
        probe_present = False
        for dex in archive.infolist():
            if re.fullmatch(r"classes[0-9]*\.dex", dex.filename):
                require(dex.file_size <= 64 * 1024 * 1024, "dex_host_bound")
                with archive.open(dex) as stream:
                    tail = b""
                    for chunk in iter(lambda: stream.read(512 * 1024), b""):
                        if descriptor in tail + chunk:
                            probe_present = True
                            break
                        tail = chunk[-len(descriptor):]
            if probe_present:
                break
        require(probe_present, "debug_peer_probe_descriptor_missing")
    return {"path": str(path), "sha256": digest.hexdigest(), "bytes": path.stat().st_size, "package": AGENT,
            "manifest": safety, "debug_probe_descriptor_present": True}


def strict_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result

    def constant(_):
        raise SmokeError("nonfinite_json")

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=constant)
    except (UnicodeError, ValueError, RecursionError):
        raise SmokeError("invalid_response_json") from None
    require(isinstance(value, dict), "response_not_object")
    return value


def assert_error(error):
    require(isinstance(error, dict) and set(error) == {"code", "phase", "retryable"}, "invalid_error_shape")
    require(isinstance(error["code"], str) and error["code"] in CODES and
            isinstance(error["phase"], str) and error["phase"] in PHASES and type(error["retryable"]) is bool,
            "invalid_error_fields")


def assert_envelope(event, request_id=None, operation=None, session_id=None):
    require(set(event) == {"protocol_version", "request_id", "session_id", "operation", "success",
                           "terminal", "result", "error"}, "invalid_envelope_shape")
    require(type(event["protocol_version"]) is int and event["protocol_version"] == 1, "response_protocol")
    for field in ("request_id", "session_id"):
        require(event[field] is None or valid_id(event[field]), "invalid_response_id")
    require(event["operation"] in (None, "health", "observe", "action", "cancel"), "invalid_response_operation")
    for expected, field in ((request_id, "request_id"), (operation, "operation"), (session_id, "session_id")):
        if expected is not None:
            require(event[field] == expected, "response_" + field + "_mismatch")
    require(type(event["success"]) is bool and type(event["terminal"]) is bool, "invalid_response_flags")
    require(event["result"] is None or isinstance(event["result"], dict), "invalid_result_shape")
    if event["success"]:
        require(event["error"] is None, "successful_error")
    else:
        assert_error(event["error"])
        require(event["terminal"], "nonterminal_error")
    if event["operation"] == "action":
        result = event["result"] or {}
        expected_status = "dispatched" if not event["terminal"] else "completed" if event["success"] else None
        require(result.get("status") == expected_status if expected_status else result.get("status") in ("failed", "rejected"),
                "invalid_action_status")
        if event["terminal"] and event["success"]:
            require(result.get("completion_source") in ("gesture_callback", "accessibility_action_return"),
                    "missing_completion_source")
    elif not event["terminal"]:
        raise SmokeError("nonterminal_nonaction")
    return event


class Reader:
    """A bounded incremental parser; coalesced headers/events stay in the buffer."""
    def __init__(self, sock, timeout=13):
        self.sock, self.buffer, self.eof = sock, bytearray(), False
        self.deadline = time.monotonic() + timeout
        self.received = 0

    def receive(self):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "response_read_timeout", True)
        self.sock.settimeout(remaining)
        try:
            chunk = self.sock.recv(64 * 1024)
        except socket.timeout:
            raise SmokeError("response_read_timeout", True) from None
        except OSError:
            raise SmokeError("response_disconnected", True) from None
        self.received += len(chunk)
        require(self.received <= MAX_RESPONSE + 8192, "response_too_large")
        self.eof = not chunk
        self.buffer.extend(chunk)

    def until(self, delimiter, limit):
        while True:
            index = self.buffer.find(delimiter)
            if index >= 0:
                require(index + len(delimiter) <= limit, "response_segment_too_large")
                result = bytes(self.buffer[:index])
                del self.buffer[:index + len(delimiter)]
                return result
            require(len(self.buffer) <= limit, "response_segment_too_large")
            require(not self.eof, "truncated_response")
            self.receive()

    def exact(self, length):
        require(0 <= length <= MAX_RESPONSE, "response_length_bound")
        while len(self.buffer) < length:
            require(not self.eof, "truncated_response")
            self.receive()
        result = bytes(self.buffer[:length])
        del self.buffer[:length]
        return result

    def assert_eof(self):
        require(not self.buffer, "extra_response_data")
        if not self.eof:
            self.receive()
        require(self.eof and not self.buffer, "extra_response_data")


def parse_headers(raw):
    try:
        lines = raw.decode("ascii").split("\r\n")
    except UnicodeError:
        raise SmokeError("nonascii_response_headers") from None
    require(len(lines) <= 33 and all(len(line) <= 2048 and all(32 <= ord(char) <= 126 for char in line) for line in lines),
            "invalid_response_header_bounds")
    require(re.fullmatch(r"HTTP/1\.1 [1-5][0-9]{2} [\x20-\x7e]+", lines[0]) is not None,
            "invalid_http_status")
    headers = {}
    for line in lines[1:]:
        require(":" in line, "invalid_response_header")
        key, value = line.split(":", 1)
        require(re.fullmatch(r"[A-Za-z0-9-]+", key) is not None and key.lower() not in headers,
                "duplicate_or_invalid_header")
        headers[key.lower()] = value.strip()
    require("transfer-encoding" not in headers, "unsupported_response_framing")
    require(headers.get("connection", "").lower() == "close", "response_not_close_delimited")
    require(headers.get("cache-control") == "no-store", "response_not_no_store")
    return int(lines[0].split()[1]), headers


def source_summary(source):
    if not isinstance(source, dict):
        return {"status": "invalid"}
    result = {key: source.get(key) for key in ("source", "status", "requested", "completeness", "error",
                                               "started_at_uptime_ms", "finished_at_uptime_ms")}
    data = source.get("data")
    if isinstance(data, dict):
        result["data_summary"] = {key: data[key] for key in (
            "width", "height", "original_width", "original_height", "scale_x", "scale_y",
            "coordinate_space", "format", "coverage", "display_id",
        ) if key in data}
        for key in ("nodes", "blocks"):
            if isinstance(data.get(key), list):
                result["data_summary"][key + "_count"] = len(data[key])
    return result


def event_summary(event):
    result = {key: event[key] for key in ("protocol_version", "request_id", "session_id", "operation",
                                         "success", "terminal", "error")}
    value = event.get("result") or {}
    result["result"] = {key: value[key] for key in ("status", "completion_source", "cancelled", "capabilities",
                                                   "completeness", "atomic_snapshot") if key in value}
    if isinstance(value.get("sources"), dict):
        result["result"]["sources"] = {key: source_summary(source) for key, source in value["sources"].items()}
    return result


class Connection:
    def __init__(self, client, sock, request_id, operation, label, timeout):
        self.client, self.sock = client, sock
        self.request_id, self.operation = request_id, operation
        self.reader = Reader(sock, timeout)
        self.status, self.headers, self.events = None, None, []
        self.record = {"label": label, "request_id": request_id, "operation": operation, "started_at": timestamp(),
                       "events": []}
        client.evidence.data["responses"].append(self.record)

    def head(self):
        if self.headers is None:
            self.status, self.headers = parse_headers(self.reader.until(b"\r\n\r\n", 8192))
            self.record["http_status"] = self.status
            if self.status == 200:
                require(self.is_stream == (self.operation in ("observe", "action")), "wrong_success_framing")
            if self.is_stream:
                require(self.status == 200 and "content-length" not in self.headers, "invalid_stream_framing")
                require(valid_id(self.headers.get("x-pulse-session")), "missing_stream_session")
                self.record["session_id"] = self.headers["x-pulse-session"]
        return self.headers

    @property
    def is_stream(self):
        return self.headers is not None and self.headers.get("content-type", "").split(";")[0] == "application/x-ndjson"

    def event(self):
        self.head()
        require(self.is_stream and len(self.events) < 2, "invalid_stream_event_count")
        require(not self.events or not self.events[-1]["terminal"], "event_after_terminal")
        event = strict_json(self.reader.until(b"\n", MAX_RESPONSE))
        assert_envelope(event, self.request_id, self.operation, self.headers["x-pulse-session"])
        self.client.evidence.secrets.remember(event)
        self.events.append(event)
        self.record["events"].append(event_summary(event))
        return event

    def finish(self):
        try:
            self.head()
            if self.is_stream:
                while not self.events or not self.events[-1]["terminal"]:
                    self.event()
                if self.operation == "action" and self.events[-1]["success"]:
                    require(len(self.events) == 2 and not self.events[0]["terminal"], "completion_without_dispatch")
                if self.operation == "action" and len(self.events) == 2:
                    require(self.events[-1]["result"].get("status") != "rejected", "rejected_after_dispatch")
                if self.operation == "observe":
                    require(len(self.events) == 1, "observe_event_count")
            else:
                require(self.headers.get("content-type", "").split(";")[0] == "application/json", "not_json_response")
                length = self.headers.get("content-length", "")
                require(len(length) <= 8 and re.fullmatch(r"0|[1-9][0-9]*", length) is not None, "invalid_response_length")
                event = strict_json(self.reader.exact(int(length)))
                assert_envelope(event, self.request_id, self.operation if self.status == 200 else None)
                require(event["terminal"] and event["success"] == (self.status == 200), "json_response_status_mismatch")
                if self.status != 200:
                    require(event["session_id"] is None, "error_exposes_connection_session")
                self.client.evidence.secrets.remember(event)
                self.events.append(event)
                self.record["events"].append(event_summary(event))
                if event["session_id"] is not None:
                    self.record["session_id"] = event["session_id"]
            self.reader.assert_eof()
            self.record["finished_at"] = timestamp()
            return self.events[-1]
        finally:
            self.close()

    def readable(self):
        return bool(self.reader.buffer) or bool(select.select([self.sock], [], [], 0)[0])

    def close(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None
            self.client.connections.discard(self)
            self.record.setdefault("closed_at", timestamp())

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Client:
    def __init__(self, port, credential, evidence):
        self.port, self.credential, self.evidence = port, credential, evidence
        self.connections = set()
        self.maximum_live = 0

    def open(self, operation, params=None, *, timeout_ms=5000, label=None, credential=None,
             path=None, raw_body=None, version=1, declared_length=None):
        require(len(self.connections) < 4, "host_connection_budget")
        request_id = None if operation == "health" or raw_body is not None else str(uuid.uuid4())
        body = b"" if operation == "health" else json.dumps({
            "protocol_version": version, "request_id": request_id, "operation": operation,
            "timeout_ms": timeout_ms, "params": params or {},
        }, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if raw_body is not None:
            body = raw_body
        require(len(body) <= MAX_MESSAGE, "host_message_bound")
        chosen = self.credential if credential is None else credential
        lines = [
            ("GET" if operation == "health" else "POST") + " " + (path or "/v1/" + operation) + " HTTP/1.1",
            "Host: 127.0.0.1", "Content-Length: " + str(len(body) if declared_length is None else declared_length),
            "Connection: close",
        ]
        if chosen:
            require(re.fullmatch(r"[0-9a-f]{64}", chosen) is not None, "invalid_host_credential")
            lines.append("Authorization: Bearer " + chosen)
        if operation != "health":
            lines.append("Content-Type: application/json")
        try:
            sock = socket.create_connection(("127.0.0.1", self.port), timeout=2)
        except OSError:
            raise SmokeError("loopback_connect_failed", True) from None
        connection = Connection(self, sock, request_id, operation, label or operation, timeout_ms / 1000 + 3)
        self.connections.add(connection)
        self.maximum_live = max(self.maximum_live, len(self.connections))
        try:
            # Never shutdown(SHUT_WR): EOF/extra input cancels a live Agent scope.
            # sendall is one request submission, not an application-level action retry.
            sock.sendall(("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body)
        except OSError:
            connection.close()
            raise SmokeError("request_write_failed_no_retry", True) from None
        return connection

    def request(self, operation, params=None, **kwargs):
        return self.open(operation, params, **kwargs).finish()

    def close(self):
        for connection in list(self.connections):
            connection.close()
        self.evidence.data["maximum_live_sockets"] = self.maximum_live


def assert_success(event):
    require(event["terminal"], "operation_not_terminal")
    if not event["success"]:
        error = event["error"]
        assert_error(error)
        environmental = error["code"] in ("busy", "window_unavailable", "accessibility_unavailable", "accessibility_interrupted")
        raise SmokeError("agent_" + error["code"] + "_" + error["phase"], environmental)
    require(event["error"] is None, "successful_error")
    return event["result"]


def assert_action(event, source):
    result = assert_success(event)
    require(result.get("status") == "completed" and result.get("completion_source") == source,
            "wrong_action_completion_source")
    return {"status": "completed", "completion_source": source}


def assert_rejected(connection, http_status, code, phase):
    event = connection.finish()
    require(connection.status == http_status and not event["success"] and event["terminal"], "rejection_status")
    require(event["error"]["code"] == code and event["error"]["phase"] == phase, "rejection_error")
    require(event["result"] is None, "rejection_leaked_result")
    return {"http_status": connection.status, "error": event["error"]}


def assert_source(source, expected):
    require(isinstance(source, dict) and source.get("source") == expected, "source_identity")
    require(source.get("status") in ("available", "unavailable", "failed"), "source_status")
    completeness = source.get("completeness")
    require(isinstance(completeness, dict) and completeness.get("status") in ("complete", "partial", "unavailable")
            and isinstance(completeness.get("reasons"), list), "source_completeness")
    if source["status"] == "available":
        require(isinstance(source.get("data"), dict) and source.get("error") is None, "available_source_invalid")
    else:
        require(source.get("data") is None and completeness["status"] == "unavailable", "unavailable_source_data")
        assert_error(source.get("error"))
    return source


def tree_nodes(result):
    require(isinstance(result, dict) and isinstance(result.get("sources"), dict), "missing_observation_sources")
    source = assert_source(result["sources"].get("tree"), "accessibility")
    require(source["status"] == "available", "native_tree_unavailable", True)
    require(source["completeness"]["status"] == "partial" and
            "native_semantics_only_visual_coverage_unverified" in source["completeness"]["reasons"],
            "native_tree_overclaims_completeness")
    require(result.get("completeness", {}).get("status") == "partial", "observation_overclaims_completeness")
    if "display_id" in source["data"]:
        require(type(source["data"]["display_id"]) is int and source["data"]["display_id"] == 0,
                "native_tree_not_default_display", True)
    nodes = source["data"].get("nodes")
    require(isinstance(nodes, list) and 0 < len(nodes) <= 512, "invalid_tree_nodes", True)
    for node in nodes:
        path = node.get("node_path")
        require(isinstance(path, list) and len(path) <= 32 and all(type(index) is int and 0 <= index <= 511 for index in path),
                "invalid_native_node_path")
        require(isinstance(node.get("states"), dict) and isinstance(node.get("bounds"), dict), "invalid_native_node")
    require(nodes[0].get("package_name") == FIXTURE, "fixture_not_foreground", True)
    return nodes


def find_node(nodes, name):
    matches = [node for node in nodes if node.get("view_id") == FIXTURE + ":id/" + name]
    require(len(matches) == 1, "fixture_node_unavailable_" + name, True)
    return matches[0]


def actual_name(node):
    require(node.get("text_redacted") is False and node["states"].get("editable") is True, "unsafe_name_node", True)
    flag = node["states"].get("showing_hint_text")
    require(type(flag) is bool, "name_hint_state_missing", True)
    text = node.get("text")
    require(text is None or isinstance(text, str), "name_not_string")
    return "" if flag else text or ""


def checked(node):
    value = node["states"].get("checked")
    require(node["states"].get("checkable") is True and type(value) is bool, "checkbox_state_missing", True)
    return value


def center(node):
    bounds = node["bounds"]
    require(all(type(bounds.get(key)) is int for key in ("left", "top", "right", "bottom")), "invalid_node_bounds")
    require(bounds["right"] > bounds["left"] and bounds["bottom"] > bounds["top"] and
            node["states"].get("visible") is True and node["states"].get("enabled") is True,
            "node_not_visible", True)
    x = (bounds["left"] + bounds["right"]) // 2
    y = (bounds["top"] + bounds["bottom"]) // 2
    require(0 <= x <= 8191 and 0 <= y <= 8191, "coordinate_out_of_range", True)
    return x, y


def png_summary(data):
    require(isinstance(data.get("base64"), str) and len(data["base64"]) <= ((MAX_PNG + 2) // 3) * 4,
            "png_encoded_bound")
    try:
        image = base64.b64decode(data["base64"], validate=True)
    except (ValueError, binascii.Error):
        raise SmokeError("png_invalid_base64") from None
    require(len(image) <= MAX_PNG and image.startswith(b"\x89PNG\r\n\x1a\n"), "png_signature_or_bound")
    pos, dimensions, image_chunks, ended = 8, None, 0, False
    while pos < len(image):
        require(pos + 12 <= len(image), "png_truncated_chunk")
        length = struct.unpack_from(">I", image, pos)[0]
        kind = image[pos + 4:pos + 8]
        end = pos + 12 + length
        require(end <= len(image), "png_chunk_bound")
        require(binascii.crc32(image[pos + 4:end - 4]) & 0xFFFFFFFF == struct.unpack_from(">I", image, end - 4)[0],
                "png_crc")
        if dimensions is None:
            require(kind == b"IHDR" and length == 13, "png_missing_ihdr")
            dimensions = struct.unpack_from(">II", image, pos + 8)
        if kind == b"IDAT":
            image_chunks += 1
        if kind == b"IEND":
            require(length == 0 and end == len(image), "png_extra_data")
            ended = True
        pos = end
    require(ended and image_chunks and dimensions is not None, "png_incomplete")
    width, height = dimensions
    require(data.get("format") == "png" and (data.get("width"), data.get("height")) == dimensions and
            type(data.get("width")) is int and type(data.get("height")) is int and
            width > 0 and height > 0 and width * height <= 1500000, "png_metadata_dimensions")
    original_w, original_h = data.get("original_width"), data.get("original_height")
    require(type(original_w) is int and type(original_h) is int and original_w >= width and original_h >= height,
            "png_original_dimensions")
    require(type(data.get("scale_x")) in (int, float) and type(data.get("scale_y")) in (int, float) and
            abs(data.get("scale_x", 0) - width / original_w) < 0.000001 and
            abs(data.get("scale_y", 0) - height / original_h) < 0.000001, "png_scale_metadata")
    require(data.get("coordinate_space") == "screenshot_pixels", "png_coordinate_space")
    if "display_id" in data:
        require(type(data["display_id"]) is int and data["display_id"] == 0, "png_not_default_display", True)
    return {"sha256": hashlib.sha256(image).hexdigest(), "bytes": len(image), "width": width, "height": height,
            "original_width": original_w, "original_height": original_h, "scale_x": data["scale_x"], "scale_y": data["scale_y"]}


def assert_text_only_ocr(source):
    assert_source(source, "mlkit_chinese")
    require(source["status"] == "available", "ocr_unavailable", True)
    require(source["completeness"]["status"] == "partial" and
            "ocr_text_only_no_native_states" in source["completeness"]["reasons"], "ocr_overclaims_completeness")

    def no_native_states(value):
        if isinstance(value, dict):
            require(not set(value).intersection({"states", "checked", "enabled", "checkable", "clickable", "editable",
                                                  "selected", "focused"}), "ocr_fabricates_native_state")
            for child in value.values():
                no_native_states(child)
        elif isinstance(value, list):
            for child in value:
                no_native_states(child)

    no_native_states(source["data"])
    blocks = source["data"].get("blocks")
    require(isinstance(blocks, list) and len(blocks) <= 256, "ocr_blocks_invalid")
    for block in blocks:
        require(isinstance(block, dict) and set(block) == {"text", "bounds", "confidence"} and
                isinstance(block["text"], str), "ocr_fabricates_native_state")
        require(block["bounds"] is None or isinstance(block["bounds"], dict), "ocr_bounds_invalid")
        require(block["confidence"] is None or type(block["confidence"]) in (int, float), "ocr_confidence_invalid")
    return {"blocks_count": len(blocks), "empty_after_success_is_valid": not blocks,
            "completeness": source["completeness"], "native_states_provided": False}


CUSTOM_LINE = re.compile(r"^[VDIWEFA]/(PulseAgentFgs|PulseLocalSocket)\(\s*([0-9]+)\):\s?(.*)$")
SAFE_LINE = re.compile(r"request_id=(unassigned|[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}) phase=([a-z_]+) category=([a-z_]+)\Z")


def structured_error(payload, secrets):
    safe = SAFE_LINE.fullmatch(payload)
    require(safe is not None, "custom_log_not_structured")
    request_id, phase, category = safe.groups()
    require(phase in PHASES and category in CODES, "custom_log_unknown_category")
    fixed_vocabulary = "request_id phase category unassigned " + " ".join(sorted(PHASES | CODES))
    require(not any(value in payload for value in secrets.values
                    if len(value) >= 4 and value not in fixed_vocabulary), "custom_log_sensitive_value")
    return {"request_id": request_id, "phase": phase, "category": category}


def custom_logs(raw, pid, secrets):
    rows = []
    for line in raw.decode("utf-8", errors="replace").splitlines():
        match = CUSTOM_LINE.fullmatch(line)
        if not match:
            require("PulseAgentFgs" not in line and "PulseLocalSocket" not in line, "custom_log_unparseable")
            continue
        tag, logged_pid, payload = match.groups()
        require(int(logged_pid) == pid, "log_pid_scope_mismatch")
        rows.append({"tag": tag, **structured_error(payload, secrets)})
    return rows


def private_error_logs(raw, secrets, expected_session):
    require(len(raw) <= 16 * 1024, "private_error_log_size_bound")
    try:
        lines = raw.decode("ascii", errors="strict").splitlines()
    except UnicodeError:
        raise SmokeError("private_error_log_encoding") from None
    require(lines and lines[0].startswith("log_session="), "private_error_log_session_missing")
    session_id = lines[0].removeprefix("log_session=")
    require(valid_id(session_id) and valid_id(expected_session), "private_error_log_session_invalid")
    require(session_id == expected_session, "private_error_log_wrong_session", True)
    require(len(lines) - 1 <= 64, "private_error_log_entry_bound")
    return [{"source": "app_private_bounded_file", "log_session": session_id,
             **structured_error(line, secrets)} for line in lines[1:]]


def probe_result(raw, expected_peer, wrong_pid=False):
    require(len(raw) <= 4096, "peer_probe_output_bound")
    fields = strict_json(raw)
    require(set(fields) == {"peer_policy", "peer_uid", "peer_pid", "token_provided"}, "peer_probe_output_shape")
    require(fields["peer_policy"] in ("rejected", "accepted", "timeout", "setup_failed"), "peer_probe_policy_invalid")
    require(type(fields["token_provided"]) is bool and fields["token_provided"], "peer_probe_credential_not_provided")
    uid, pid = fields["peer_uid"], fields["peer_pid"]
    require(type(uid) is int and uid >= 0 and type(pid) is int and 0 < pid <= 2147483647, "peer_probe_kernel_identity_invalid")
    if wrong_pid:
        require(uid == expected_peer["uid"] and pid != expected_peer["pid"], "peer_probe_not_same_uid_wrong_pid")
    else:
        require(uid >= 10000 and uid not in (0, 2000), "peer_probe_not_app_uid")
    require(fields["peer_policy"] == "rejected", "peer_probe_" + fields["peer_policy"], fields["peer_policy"] == "setup_failed")
    return fields


class Smoke:
    def __init__(self, args, evidence):
        self.args, self.evidence = args, evidence
        self.adb = Adb(args.adb, args.serial, evidence)
        self.a11y, self.forward, self.client = None, ForwardLease(self.adb), None
        self.peer_config = PeerConfigLease(self.adb)
        self.expected_peer, self.installed_apk = None, None
        self.recovery_required = False
        self.pid, self.previous_name, self.previous_checked = None, None, None
        self.name_dirty = False
        self.checkbox_touched = False
        self.last_image = 0.0
        self.error_log_session = None

    def preflight(self):
        result = {"agent": apk_info(self.args.apk)}
        self.evidence.data["apks"] = result
        return result

    def device(self):
        require(self.adb.run("get-state", label="device_state").stdout.strip() == b"device", "device_unavailable", True)
        properties = {}
        for key in ("ro.product.manufacturer", "ro.product.model", "ro.build.version.release", "ro.build.version.sdk"):
            value = self.adb.run("shell", "getprop", key, label="device_property").stdout.decode("utf-8").strip()
            require(len(value) <= 128, "device_property_bound")
            properties[key] = value
        require(properties["ro.build.version.sdk"].isdecimal() and int(properties["ro.build.version.sdk"]) >= 30,
                "android_api_unsupported", True)
        installed = self.adb.run("shell", "pm", "path", FIXTURE, label="fixture_installed").stdout
        require(installed.startswith(b"package:"), "fixture_not_installed", True)
        self.a11y = AccessibilityLease(self.adb)
        self.evidence.data["device"] = properties
        return {"device": properties, "fixture_installed": True}

    def install(self):
        result = self.adb.run("install", "-r", str(self.args.apk), label="install_agent", timeout=90)
        require(b"Success" in result.stdout, "apk_install_not_successful", True)
        self.installed_apk = installed_base_apk(self.adb.run("shell", "pm", "path", AGENT, label="installed_agent_apk").stdout)
        return {"installed": [AGENT], "installed_base_apk": self.installed_apk, "fixture_not_reinstalled": True}

    def fresh_peer(self):
        pid_output = self.adb.run("shell", "pidof", "adbd", label="fresh_adbd_pid").stdout
        pid = pid_output.decode("ascii").strip()
        require(re.fullmatch(r"[1-9][0-9]{0,9}", pid) is not None and int(pid) <= 2147483647,
                "adbd_pid_not_single", True)
        ps = self.adb.run("shell", "ps", "-p", pid, "-o", "UID,PID,NAME", label="fresh_adbd_kernel_identity").stdout
        return adbd_identity(pid_output, ps)

    def bootstrap(self):
        self.expected_peer = self.fresh_peer()
        lease = self.peer_config.provision(self.expected_peer)
        require(self.fresh_peer() == self.expected_peer, "adbd_changed_during_provisioning", True)
        self.evidence.data["adbd"] = {**self.expected_peer, "source": "fresh_pidof_and_ps", "checked_at": timestamp()}
        return {**lease, "expected_adbd": self.expected_peer, "before_accessibility_enable": True}

    def start(self):
        require(self.expected_peer is not None, "private_peer_not_provisioned")
        require(self.fresh_peer() == self.expected_peer, "adbd_changed_before_accessibility_enable", True)
        accessibility = self.a11y.enable()
        token = None
        for _ in range(30):
            result = self.adb.run("shell", "-T", "run-as", AGENT, "cat", "files/agent-token",
                                  label="read_private_credential", check=False)
            if result.returncode == 0:
                require(re.fullmatch(rb"[0-9a-f]{64}", result.stdout) is not None, "invalid_private_credential")
                token = result.stdout.decode("ascii")
                break
            time.sleep(0.25)
        require(token is not None, "accessibility_fgs_not_ready", True)
        self.evidence.secrets.credential(token)
        self.pid = self.agent_pid()
        self.client = Client(self.forward.create(), token, self.evidence)
        return {"private_fgs_started_by_accessibility": True, "private_credential_valid": True,
                "forward_port": self.forward.port, "agent_pid": self.pid, "accessibility": accessibility}

    def ensure_instance(self):
        require(not self.recovery_required, "agent_recovery_required_no_automatic_retry", True)
        same_pid = self.agent_pid() == self.pid
        current = self.adb.run("shell", "-T", "run-as", AGENT, "cat", "files/agent-token",
                               label="check_private_credential_instance", check=False)
        if current.returncode == 0 and re.fullmatch(rb"[0-9a-f]{64}", current.stdout):
            self.evidence.secrets.credential(current.stdout.decode("ascii"))
        if not same_pid or current.returncode != 0 or current.stdout != self.client.credential.encode("ascii"):
            self.recovery_required = True
            self.evidence.data["new_provisioning_required"] = True
            raise SmokeError("agent_instance_changed_requires_new_provisioning", True)

    def check_recovery(self, result):
        caps = result.get("capabilities", {}) if isinstance(result, dict) else {}
        require(isinstance(caps, dict), "capabilities_invalid")
        if "recovery_required" in caps:
            require(type(caps["recovery_required"]) is bool, "recovery_capability_invalid")
            if caps["recovery_required"]:
                self.recovery_required = True
                self.evidence.data["recovery_required"] = True
        if "display_scope" in caps:
            require(caps["display_scope"] == "default_only", "unsupported_agent_display_scope", True)
        if "error_log_session_id" in caps:
            session_id = caps["error_log_session_id"]
            require(valid_id(session_id), "error_log_session_invalid")
            require(self.error_log_session is None or self.error_log_session == session_id,
                    "error_log_instance_changed_requires_new_provisioning", True)
            self.error_log_session = session_id
        require(not self.recovery_required, "agent_recovery_required_no_automatic_retry", True)

    def binding(self):
        executable = self.args.lsof or shutil.which("lsof") or "/usr/sbin/lsof"
        result = command([executable, "-nP", "-a", "-iTCP:" + str(self.forward.port), "-sTCP:LISTEN", "-Fn"],
                         self.evidence, "host_lsof")
        addresses = [line[1:] for line in result.stdout.decode("ascii").splitlines() if line.startswith("n")]
        allowed = {"127.0.0.1:" + str(self.forward.port), "[::1]:" + str(self.forward.port)}
        require(addresses and set(addresses) <= allowed and "127.0.0.1:" + str(self.forward.port) in addresses,
                "forward_not_loopback_only")
        return {"bindings": addresses, "scope": "host_forward_bind_only", "cross_machine_lan_proof": False}

    def lan_probe(self):
        # Route lookup on a UDP client sends no datagrams and creates no listener.
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as route:
                route.connect(("192.0.2.1", 9))
                address = route.getsockname()[0]
        except OSError:
            raise SmokeError("host_nonloopback_route_unavailable", True) from None
        ip = ipaddress.ip_address(address)
        require(not (ip.is_loopback or ip.is_unspecified or ip.is_multicast), "no_nonloopback_host_address", True)
        try:
            sock = socket.create_connection((address, self.forward.port), timeout=1)
        except (ConnectionRefusedError, socket.timeout, OSError):
            return {"address": address, "connection_refused_or_unreachable": True,
                    "scope": "same_host_interface_only", "cross_machine_lan_proof": False}
        else:
            sock.close()
            raise SmokeError("forward_reachable_on_host_nonloopback")

    def health(self):
        for attempt in range(8):
            try:
                if self.pid is not None:
                    self.ensure_instance()
                event = self.client.request("health", label="health_read")
                result = assert_success(event)
                self.check_recovery(result)
                require(isinstance(result, dict) and isinstance(result.get("capabilities"), dict), "health_capabilities_invalid")
                caps = result["capabilities"]
                if result.get("status") == "ok" and caps.get("accessibility") == "available":
                    require(valid_id(event["request_id"]) and valid_id(event["session_id"]), "health_missing_ids")
                    return {"status": "ready", "capabilities": caps}
            except SmokeError as exc:
                if not exc.environmental or self.recovery_required or attempt == 7:
                    raise
            if attempt < 7:
                time.sleep(0.25)
        raise SmokeError("health_not_ready", True)

    def launch(self, page):
        require(page in ("FormActivity", "WebViewActivity", "FlutterPageActivity"), "fixture_page_out_of_scope")
        result = self.adb.run("shell", "am", "start", "-W", "-n", FIXTURE + "/." + page,
                              label="launch_" + page, timeout=20)
        require(b"Error:" not in result.stdout + result.stderr, "fixture_launch_failed", True)

    def observe(self, label, image=False):
        if self.pid is not None:
            self.ensure_instance()
        if image:
            time.sleep(max(0, self.last_image + 1.15 - time.monotonic()))
            self.last_image = time.monotonic()
        result = assert_success(self.client.request("observe", {"tree": True, "screenshot": image, "ocr": image},
                                                   timeout_ms=10000 if image else 5000, label=label))
        self.check_recovery(result)
        return result

    def read_tree(self, label, required_node=None, predicate=None):
        for attempt in range(8):
            try:
                result = self.observe(label + "_read_" + str(attempt + 1))
                nodes = tree_nodes(result)
                if required_node:
                    find_node(nodes, required_node)
                if predicate:
                    require(predicate(nodes), "observation_not_settled", True)
                return result, nodes
            except SmokeError as exc:
                if not exc.environmental or self.recovery_required or attempt == 7:
                    raise
                time.sleep(0.2)
        raise SmokeError("tree_read_exhausted", True)

    def action(self, params, source, label):
        require(not self.recovery_required, "agent_recovery_required_no_automatic_retry", True)
        self.health()
        try:
            event = self.client.request("action", {"expected_package": FIXTURE, **params}, label=label)
        except Exception:
            if params["kind"] in ("set_text", "node_click"):
                self.recovery_required = True
                self.evidence.data["native_submission_receipt_uncertain_no_replay"] = True
            raise
        if not event["success"] and (event.get("result") or {}).get("status") == "failed" and params["kind"] in ("set_text", "node_click"):
            self.recovery_required = True
            self.evidence.data["native_completion_uncertain_no_replay"] = True
        return assert_action(event, source)

    def native(self):
        self.launch("FormActivity")
        result, nodes = self.read_tree("form", "et_name")
        require("text_truncated" not in result["sources"]["tree"]["completeness"]["reasons"],
                "name_baseline_may_be_truncated", True)
        self.previous_name = actual_name(find_node(nodes, "et_name"))
        self.previous_checked = checked(find_node(nodes, "cb_agree"))
        if self.previous_name:
            self.evidence.secrets.values.add(self.previous_name)
        return {"tree": source_summary(result["sources"]["tree"]), "initial_checked": self.previous_checked}

    def unrequested(self):
        # Omit OCR/screenshot flags: explicit false would not test the defaults.
        if self.pid is not None:
            self.ensure_instance()
        result = assert_success(self.client.request("observe", label="unrequested_ocr_defaults"))
        self.check_recovery(result)
        source = assert_source(result["sources"].get("ocr"), "mlkit_chinese")
        require(source["status"] == "unavailable" and source.get("requested") is False and
                source["error"]["code"] == "not_requested" and source["data"] is None, "ocr_default_not_off")
        return source_summary(source)

    def name(self):
        _, nodes = self.read_tree("name_before", "et_name")
        node = find_node(nodes, "et_name")
        sentinel = "pulse-smoke-" + uuid.uuid4().hex[:12] + "-\u6d4b\u8bd5"
        self.evidence.secrets.values.add(sentinel)
        self.name_dirty = True
        result = self.action({"kind": "set_text", "node_path": node["node_path"], "text": sentinel},
                             "accessibility_action_return", "set_unicode_name")
        _, after = self.read_tree("name_after", "et_name",
                                  lambda nodes: actual_name(find_node(nodes, "et_name")) == sentinel)
        require(actual_name(find_node(after, "et_name")) == sentinel, "exact_unicode_name_mismatch")
        return {**result, "exact_unicode_equal": True, "previous_value_in_memory_only": True}

    def tap_checkbox(self):
        _, nodes = self.read_tree("checkbox_before", "cb_agree")
        node = find_node(nodes, "cb_agree")
        baseline = checked(node)
        x, y = center(node)
        self.checkbox_touched = True
        result = self.action({"kind": "tap", "x": x, "y": y, "duration_ms": 80}, "gesture_callback", "checkbox_tap")
        _, nodes = self.read_tree("checkbox_after", "cb_agree",
                                  lambda nodes: checked(find_node(nodes, "cb_agree")) != baseline)
        require(checked(find_node(nodes, "cb_agree")) != baseline, "checkbox_did_not_toggle")
        return {**result, "checked_toggled": True}

    def image(self):
        result = self.observe("screenshot_ocr", True)
        screen = assert_source(result["sources"].get("screenshot"), "accessibility_screenshot")
        # A screenshot retry is a read only, and only an explicit rate limit permits it.
        if screen["status"] != "available" and screen["error"]["code"] == "screenshot_rate_limited":
            result = self.observe("screenshot_ocr_after_rate_limit", True)
            screen = assert_source(result["sources"].get("screenshot"), "accessibility_screenshot")
        require(screen["status"] == "available", "screenshot_unavailable", True)
        return {"png": png_summary(screen["data"]), "ocr": assert_text_only_ocr(result["sources"].get("ocr")),
                "completeness": result.get("completeness")}

    def stable_checkbox(self, baseline, label):
        for index, delay in enumerate((0.1, 0.25, 0.4)):
            time.sleep(delay)
            _, nodes = self.read_tree(label + "_drained_" + str(index), "cb_agree")
            require(checked(find_node(nodes, "cb_agree")) == baseline, "late_checkbox_action")
        return {"checked_unchanged_after_drain": True, "samples": 3}

    def queued(self, mode):
        require(not self.recovery_required, "agent_recovery_required_no_automatic_retry", True)
        self.health()
        _, nodes = self.read_tree(mode + "_baseline", "cb_agree")
        checkbox = find_node(nodes, "cb_agree")
        baseline = checked(checkbox)
        center(checkbox)
        require(checkbox["states"].get("clickable") is True, "queued_checkbox_not_actionable", True)
        neutral = find_node(nodes, "tv_form_title")
        require(all(neutral["states"].get(key) is False for key in ("clickable", "checkable", "editable")),
                "neutral_gesture_target_unsafe", True)
        x, y = center(neutral)
        params = {"kind": "tap", "x": x, "y": y, "duration_ms": 1000, "expected_package": FIXTURE}
        with self.client.open("action", params, label=mode + "_neutral_blocker") as blocker:
            try:
                first = blocker.event()
                require(first["success"] and not first["terminal"] and first["result"]["status"] == "dispatched",
                        "neutral_blocker_not_dispatched", True)
                admitted_at = time.monotonic()
                # Do not submit a checkbox action if a completed blocker is already buffered.
                require(not blocker.readable(), "blocker_already_completed", True)
                self.checkbox_touched = True
                with self.client.open("action", {"kind": "node_click", "node_path": checkbox["node_path"],
                                                 "expected_package": FIXTURE},
                                      timeout_ms=100 if mode == "timeout" else 5000,
                                      label=mode + "_queued_checkbox") as target:
                    target.head()
                    require(target.is_stream, "queued_action_not_stream")
                    target_session = target.headers["x-pulse-session"]
                    require(not blocker.readable() and time.monotonic() - admitted_at < 0.4,
                            "queue_admission_not_deterministic", True)
                    if mode == "cancel":
                        receipt = self.client.request("cancel", {"target_session_id": target_session,
                                                                  "target_request_id": target.request_id}, label="cancel_pair")
                        require(assert_success(receipt).get("cancelled") is True, "target_cancel_not_accepted")
                    elif mode == "disconnect":
                        target.record["intentional_disconnect"] = True
                        target.close()
                    if mode != "disconnect":
                        final = target.finish()
                        if not final["success"] and (final.get("result") or {}).get("status") == "failed":
                            self.recovery_required = True
                            self.evidence.data["queued_native_completion_uncertain_no_replay"] = True
                        require(len(target.events) == 1 and not final["success"] and
                                final["result"].get("status") == "rejected" and final["error"]["phase"] == "queue" and
                                final["error"]["code"] == ("cancelled" if mode == "cancel" else "timeout"),
                                "queued_action_was_dispatched_or_wrong_terminal")
                    require(time.monotonic() - admitted_at < 0.8, "queue_window_not_deterministic", True)
            finally:
                # Even a failed target/cancel check must await the submitted neutral OS gesture.
                if blocker.sock is not None:
                    assert_action(blocker.finish(), "gesture_callback")
        result = self.stable_checkbox(baseline, mode)
        return {**result, "target_session_id": target_session, "target_request_id": target.request_id,
                "target_terminal_observed": mode != "disconnect", "blocker_completion_source": "gesture_callback",
                "os_gesture_rollback_claimed": False}

    def restore_fixture(self):
        require(self.previous_name is not None and self.previous_checked is not None, "fixture_baseline_missing", True)
        # Observe before each compensation: never blindly retry or double-toggle an action.
        try:
            if self.name_dirty:
                _, nodes = self.read_tree("restore_name_before", "et_name")
                node = find_node(nodes, "et_name")
                if actual_name(node) != self.previous_name:
                    self.action({"kind": "set_text", "node_path": node["node_path"], "text": self.previous_name},
                                "accessibility_action_return", "restore_previous_name")
        finally:
            if self.checkbox_touched:
                _, nodes = self.read_tree("restore_checkbox_before", "cb_agree")
                node = find_node(nodes, "cb_agree")
                if checked(node) != self.previous_checked:
                    self.action({"kind": "node_click", "node_path": node["node_path"]},
                                "accessibility_action_return", "restore_previous_checkbox")
        _, nodes = self.read_tree("fixture_restore_verify", "et_name", lambda nodes:
                                  actual_name(find_node(nodes, "et_name")) == self.previous_name and
                                  checked(find_node(nodes, "cb_agree")) == self.previous_checked)
        require(actual_name(find_node(nodes, "et_name")) == self.previous_name and
                checked(find_node(nodes, "cb_agree")) == self.previous_checked, "fixture_restore_mismatch")
        self.name_dirty = self.checkbox_touched = False
        return {"previous_name_equal": True, "previous_checked_equal": True, "no_fixture_faults_changed": True}

    def partial_page(self, page):
        self.launch(page)
        result, nodes = self.read_tree(page)
        reasons = result["sources"]["tree"]["completeness"]["reasons"]
        classes = [node.get("class_name") or "" for node in nodes]
        detectable = any("webview" in name.lower() for name in classes) if page == "WebViewActivity" else any(
            "flutter" in name.lower() or name.endswith(("SurfaceView", "TextureView")) for name in classes)
        reason = "webview_semantics_limited" if page == "WebViewActivity" else "flutter_or_surface_semantics_limited"
        require(not detectable or reason in reasons, "missing_detectable_visual_boundary")
        return {"native_completeness": result["sources"]["tree"]["completeness"], "boundary_detectable": detectable,
                "full_dom_or_flutter_coverage_claimed": False, "nodes_count": len(nodes)}

    def agent_pid(self):
        value = self.adb.run("shell", "pidof", AGENT, label="agent_pid").stdout.decode("ascii").strip()
        require(value.isdecimal() and int(value) > 0, "agent_pid_unavailable", True)
        return int(value)

    def logs(self):
        require(self.pid is not None, "agent_pid_missing", True)
        self.evidence.data["last_error_log_source"] = "unavailable"
        raw = self.adb.run("logcat", "-d", "-b", "all", "--pid=" + str(self.pid), "-v", "brief",
                           "PulseAgentFgs:V", "PulseLocalSocket:V", "*:S", label="pid_scoped_logcat",
                           timeout=20).stdout
        rows = custom_logs(raw, self.pid, self.evidence.secrets)
        if rows:
            self.evidence.data["error_log_source"] = "pid_scoped_logcat"
            self.evidence.data["last_error_log_source"] = "pid_scoped_logcat"
            return rows
        # Only trusted ADB run-as reads this bounded private file; no public log endpoint.
        require(valid_id(self.error_log_session), "private_error_log_session_unavailable", True)
        self.ensure_instance()
        current = self.adb.run("shell", "-T", "run-as", AGENT, "cat", "files/agent-errors",
                               label="private_bounded_error_records", check=False)
        if current.returncode != 0:
            return []
        mode = self.adb.run("shell", "-T", "run-as", AGENT, "ls", "-l", "files/agent-errors",
                            label="private_error_record_permissions").stdout
        require(mode.startswith(b"-rw-------"), "private_error_log_permissions")
        try:
            rows = private_error_logs(current.stdout, self.evidence.secrets, self.error_log_session)
        except SmokeError as exc:
            if exc.code == "private_error_log_wrong_session":
                return []
            raise
        self.ensure_instance()
        self.evidence.data["last_error_log_source"] = "app_private_bounded_file"
        if rows:
            self.evidence.data["error_log_source"] = "app_private_bounded_file"
        return rows

    def peer(self, wrong_pid=False):
        require(self.expected_peer is not None and self.installed_apk is not None, "peer_probe_prerequisite_missing")
        if wrong_pid:
            require(self.expected_peer["uid"] == 2000, "same_uid_probe_requires_shell_adbd_profile", True)
        self.health()
        before = Counter(row["category"] for row in self.logs())
        before_source = self.evidence.data.get("last_error_log_source")
        args = ["shell", "-T"] + ([] if wrong_pid else ["run-as", AGENT])
        args += ["env", "CLASSPATH=" + self.installed_apk, "app_process", "/system/bin", PROBE]
        if wrong_pid:
            args.append("--token-stdin")
        try:
            result = self.adb.run(*args, label="same_uid_wrong_pid_probe" if wrong_pid else "app_uid_probe",
                                  input_bytes=self.client.credential.encode("ascii") if wrong_pid else None, timeout=10)
            receipt = probe_result(result.stdout, self.expected_peer, wrong_pid)
            require(receipt["peer_pid"] != self.pid, "probe_not_separate_from_agent")
        finally:
            self.health()
            require(self.agent_pid() == self.pid, "peer_probe_restarted_agent")
        after = Counter(row["category"] for row in self.logs())
        after_source = self.evidence.data.get("last_error_log_source")
        # Coalesced file publication can include an older probe between reads; not causal evidence.
        policy_seen = (before_source == after_source == "pid_scoped_logcat" and
                       after["forbidden_peer"] > before["forbidden_peer"])
        return {**receipt, "agent_still_ready": True, "agent_pid_unchanged": True,
                "agent_exact_peer_policy_log_observed": policy_seen,
                "mechanism": "agent_exact_uid_pid_policy" if policy_seen else "io_denial_possible_selinux_not_distinguishable"}

    def check_logs(self):
        require(self.agent_pid() == self.pid, "agent_pid_changed_log_scope", True)
        rows = self.logs()
        require(rows, "custom_log_evidence_absent", True)
        request_ids = {record.get("request_id") for record in self.evidence.data["responses"]}
        require(any(row["request_id"] in request_ids for row in rows), "current_run_error_record_absent", True)
        self.evidence.data["agent_custom_logs"] = rows
        source = self.evidence.data["last_error_log_source"]
        return {"source": source, "pid_scoped": source == "pid_scoped_logcat",
                "current_run_request_correlated": True, "private_session_scoped": source == "app_private_bounded_file",
                "global_logcat_cleared": False, "structured_lines": len(rows),
                "credentials_inputs_labels_encoded_images_absent": True, "third_party_stream_not_saved": True}

    def cleanup(self):
        results, errors = {}, []
        if self.client:
            self.client.close()
        accessibility_ok = False
        for name, function in (("forward", self.forward.restore),
                               ("accessibility", self.a11y.restore if self.a11y else lambda: {"changed": False})):
            try:
                results[name] = function() or {"removed_if_owned": True}
                if name == "accessibility":
                    accessibility_ok = True
            except (SmokeError, OSError, ValueError) as exc:
                errors.append(exc.code if isinstance(exc, SmokeError) else type(exc).__name__)
        if accessibility_ok:
            try:
                if self.a11y and (self.a11y.added or results["accessibility"].get("own_activation_stopped")):
                    stopped = False
                    for _ in range(20):
                        names = self.adb.run("shell", "-T", "run-as", AGENT, "ls", "files",
                                             label="await_owned_fgs_stop", check=False)
                        if names.returncode == 0 and b"agent-token" not in names.stdout.splitlines():
                            stopped = True
                            break
                        time.sleep(0.25)
                    require(stopped, "owned_fgs_not_stopped_peer_configuration_retained", True)
                    results["owned_fgs_stopped"] = True
                results["peer_configuration"] = self.peer_config.restore()
            except (SmokeError, OSError, ValueError) as exc:
                errors.append(exc.code if isinstance(exc, SmokeError) else type(exc).__name__)
        else:
            results["peer_configuration"] = {"restore_skipped": "owned_accessibility_shutdown_not_confirmed"}
            if self.peer_config.modified:
                errors.append("peer_restore_blocked_by_accessibility_cleanup")
        self.evidence.data["cleanup"] = results
        require(not errors, "owned_cleanup_failed", True)
        return results

    def run(self):
        evidence = self.evidence
        try:
            evidence.must("apk_preflight", self.preflight)
            evidence.must("device_ready", self.device)
            evidence.must("install_agent", self.install)
            evidence.must("private_peer_bootstrap", self.bootstrap)
            evidence.must("accessibility_start", self.start)
            evidence.must("loopback_binding", self.binding)
            evidence.case("same_host_lan_probe", self.lan_probe, required=False)
            # A successful non-loopback connection is a real security failure even if optional probing is blocked.
            require(evidence.data["cases"][-1]["status"] != "failed", "unsafe_host_forward")
            evidence.must("authenticated_health", self.health)
            wrong = "0" * 64 if self.client.credential != "0" * 64 else "1" * 64
            for name, kwargs, status, code, phase in (
                ("missing_token", {"credential": ""}, 401, "unauthorized", "auth"),
                ("wrong_token", {"credential": wrong}, 401, "unauthorized", "auth"),
                ("unknown_path", {"path": "/v1/unknown"}, 404, "not_found", "protocol"),
                ("unsupported_protocol", {"version": 2}, 426, "unsupported_protocol", "protocol"),
                ("malformed_message", {"raw_body": b"{"}, 400, "invalid_message", "protocol"),
                ("oversized_message", {"raw_body": b"", "declared_length": MAX_MESSAGE + 1}, 413, "message_too_large", "transport"),
            ):
                operation = "health" if name in ("missing_token", "wrong_token", "unknown_path") else "observe"
                evidence.case(name, lambda op=operation, kw=kwargs, st=status, co=code, ph=phase, label=name:
                              assert_rejected(self.client.open(op, label=label, **kw), st, co, ph))
            evidence.case("app_uid_peer", self.peer)
            evidence.case("same_uid_wrong_pid_peer", lambda: self.peer(True))
            try:
                evidence.must("native_tree", self.native)
                evidence.case("unrequested_ocr", self.unrequested)
                evidence.case("exact_unicode_name", self.name)
                evidence.case("checkbox_callback_tap", self.tap_checkbox)
                evidence.case("screenshot_and_ocr", self.image)
                for mode in ("cancel", "timeout", "disconnect"):
                    evidence.case("queued_" + mode, lambda mode=mode: self.queued(mode))
            finally:
                if self.previous_name is not None:
                    evidence.must("fixture_state_restored", self.restore_fixture)
            evidence.case("webview_partial", lambda: self.partial_page("WebViewActivity"))
            evidence.case("flutter_partial", lambda: self.partial_page("FlutterPageActivity"))
        except SmokeError:
            pass
        finally:
            if self.client and self.pid:
                evidence.case("sensitive_logs", self.check_logs)
            evidence.case("owned_cleanup", self.cleanup)
            evidence.skip("real_failed_or_cancelled_gesture_callback", "no_deterministic_public_api_injection; unit coverage is separate")
            evidence.skip("connection_bound_and_transport_read_timeout", "not_exercised; do not exceed four live sockets or infer a timeout pass")


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serial", required=True, help="Explicit authorized Android serial (e.g. 10AE5U23J9000P3)")
    parser.add_argument("--evidence", type=Path, required=True,
                        help="Evidence root; every run gets a fresh subdirectory (pulse/build/p1-agent-v1-20261103)")
    parser.add_argument("--run-name", help="Optional unique run subdirectory name; an existing name is never overwritten")
    parser.add_argument("--apk", type=Path, default=ROOT / "app/build/outputs/apk/debug/app-debug.apk")
    parser.add_argument("--adb", default="adb")
    parser.add_argument("--lsof", default=None)
    parser.add_argument("--check-only", action="store_true", help="Host APK/parser preflight only; no device/network/evidence changes")
    return parser.parse_args(argv)


def main(argv=None):
    args = arguments(argv)
    evidence = Evidence(args.serial)
    try:
        smoke = Smoke(args, evidence)
        if args.check_only:
            result = evidence.case("apk_preflight", smoke.preflight)
            case = evidence.data["cases"][-1]
            status = case["status"]
            print(json.dumps({"check_only": True, "status": status, "apks": result,
                              "reason": case.get("reason"), "device_commands_executed": 0,
                              "evidence_written": False}, ensure_ascii=True))
            return 0 if status == "passed" else 2
        run_name = args.run_name or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,95}", run_name) is not None, "invalid_evidence_run_name")
        root = args.evidence.resolve()
        root.mkdir(parents=True, exist_ok=True)
        run_directory = root / run_name
        require(not run_directory.exists(), "evidence_run_already_exists")
        run_directory.mkdir(exist_ok=False)
        target = run_directory / "result.json"
        evidence.data.update(run_name=run_name, evidence_root=str(root))
        smoke.run()
        result = evidence.finish()
        encoded = json.dumps(result, ensure_ascii=True, indent=2, allow_nan=False) + "\n"
        require(not any(secret in encoded for secret in evidence.secrets.credentials), "evidence_credential_leak")
        with target.open("x", encoding="utf-8") as stream:
            stream.write(encoded)
        count = sum(item["required"] and item["status"] == "passed" for item in result["cases"])
        print(json.dumps({"status": result["status"], "required_passed": count, "required_total": len(REQUIRED),
                          "evidence": str(target)}, ensure_ascii=True))
        return 0 if result["status"] == "passed" else 1
    except (SmokeError, OSError, ValueError) as exc:
        reason = exc.code if isinstance(exc, SmokeError) else "host_" + type(exc).__name__
        print(json.dumps({"status": "failed", "reason": reason}, ensure_ascii=True))
        return 2


if __name__ == "__main__":
    sys.exit(main())
