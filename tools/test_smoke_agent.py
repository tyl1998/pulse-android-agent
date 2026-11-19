"""Offline smoke-runner tests. No ADB, network listener, APK install, or Gradle."""

import argparse
import base64
import binascii
from collections import deque
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import socket
import struct
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, patch
import uuid
import zipfile
import zlib

import smoke_agent as smoke


TEMP = Path("/var/folders/39/lr_9rr2j05q668x5mgzw5bjc0000gp/T/kilo")
if not TEMP.is_dir():
    TEMP = Path(tempfile.gettempdir())
RID, SID = str(uuid.uuid4()), str(uuid.uuid4())
DUMMY_CREDENTIAL = "1" * 64
EXPECTED_PEER = {"uid": 2000, "pid": 5321}
HOST_GUARDS = []


def setUpModule():
    # A forgotten mock must fail the test, never run ADB or create a real socket.
    for target in ("subprocess.run", "socket.create_connection", "socket.socket"):
        guard = patch("smoke_agent." + target, side_effect=AssertionError("unmocked host external operation"))
        guard.start()
        HOST_GUARDS.append(guard)


def tearDownModule():
    for guard in reversed(HOST_GUARDS):
        guard.stop()
    HOST_GUARDS.clear()


def envelope(operation="action", *, terminal=True, success=True, result=None, error=None,
             request_id=RID, session_id=SID):
    return {"protocol_version": 1, "request_id": request_id, "session_id": session_id,
            "operation": operation, "success": success, "terminal": terminal,
            "result": result, "error": error}


def dispatched():
    return envelope(terminal=False, result={"status": "dispatched"})


def completed(source="gesture_callback"):
    return envelope(result={"status": "completed", "completion_source": source})


def failed(code="cancelled", phase="queue", status="rejected"):
    return envelope(success=False, result={"status": status},
                    error={"code": code, "phase": phase, "retryable": False})


def wire(events, *, status=200, stream=True, extra=b"", headers=None):
    if stream:
        body = b"".join(json.dumps(event, ensure_ascii=False).encode("utf-8") + b"\n" for event in events)
        fields = {"Content-Type": "application/x-ndjson; charset=utf-8", "X-Pulse-Session": SID}
    else:
        body = json.dumps(events[0], ensure_ascii=False).encode("utf-8")
        fields = {"Content-Type": "application/json; charset=utf-8", "Content-Length": str(len(body))}
    fields.update({"Connection": "close", "Cache-Control": "no-store"})
    fields.update(headers or {})
    head = "HTTP/1.1 " + str(status) + " Test\r\n" + "\r\n".join(key + ": " + value for key, value in fields.items())
    return head.encode("ascii") + b"\r\n\r\n" + body + extra


class FakeSocket:
    def __init__(self, chunks=(), write_error=False):
        self.chunks = deque(chunks)
        self.closed = False
        self.writes = []
        self.timeouts = []
        self.write_error = write_error

    def settimeout(self, value):
        self.timeouts.append(value)

    def recv(self, size):
        value = self.chunks.popleft() if self.chunks else b""
        if isinstance(value, Exception):
            raise value
        if len(value) > size:
            self.chunks.appendleft(value[size:])
        return value[:size]

    def sendall(self, value):
        self.writes.append(value)
        if self.write_error:
            raise OSError("simulated")

    def shutdown(self, *_):
        raise AssertionError("write side must stay open")

    def close(self):
        self.closed = True


def connection(raw, operation="action", chunks=None):
    evidence = smoke.Evidence("host-test")
    client = smoke.Client(12345, DUMMY_CREDENTIAL, evidence)
    sock = FakeSocket(chunks if chunks is not None else [raw])
    conn = smoke.Connection(client, sock, RID, operation, "host-parser", 2)
    client.connections.add(conn)
    return conn, sock, evidence


def source(name, data=None, reasons=(), status="available", requested=True, code="not_requested"):
    return {"source": name, "status": status, "requested": requested,
            "completeness": {"status": "partial" if reasons else "complete" if status == "available" else "unavailable",
                             "reasons": list(reasons)},
            "data": data if status == "available" else None,
            "error": None if status == "available" else {"code": code, "phase": "observe", "retryable": False}}


def native_node(name, path, *, text=None, editable=False, hint=False, checkable=False, checked=False):
    return {"view_id": smoke.FIXTURE + ":id/" + name, "node_path": path,
            "package_name": smoke.FIXTURE, "class_name": "android.widget.TextView",
            "text": text, "content_description": None, "text_redacted": False,
            "bounds": {"left": 10, "top": 10, "right": 100, "bottom": 50},
            "states": {"enabled": True, "visible": True, "editable": editable,
                       "showing_hint_text": hint, "checkable": checkable,
                       "checked": checked if checkable else None, "clickable": checkable}}


def png_chunk(kind, data):
    return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", binascii.crc32(kind + data) & 0xFFFFFFFF)


def image_data():
    image = b"\x89PNG\r\n\x1a\n" + png_chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 6, 0, 0, 0))
    image += png_chunk(b"IDAT", zlib.compress(b"\0\0\0\0\xff")) + png_chunk(b"IEND", b"")
    return {"base64": base64.b64encode(image).decode("ascii"), "format": "png", "width": 1, "height": 1,
            "original_width": 2, "original_height": 2, "scale_x": 0.5, "scale_y": 0.5,
            "coordinate_space": "screenshot_pixels"}


def manifest_elements():
    return [
        ("manifest", {"package": smoke.AGENT}),
        ("uses-sdk", {"minSdkVersion": 30, "targetSdkVersion": 36}),
        ("uses-permission", {"name": "android.permission.FOREGROUND_SERVICE"}),
        ("uses-permission", {"name": "android.permission.FOREGROUND_SERVICE_SPECIAL_USE"}),
        ("uses-permission", {"name": "android.permission.POST_NOTIFICATIONS"}),
        ("application", {"debuggable": True, "allowBackup": False, "fullBackupContent": False, "usesCleartextTraffic": False}),
        ("service", {"name": smoke.SERVICE.split("/", 1)[1], "exported": True,
                     "permission": "android.permission.BIND_ACCESSIBILITY_SERVICE"}),
        ("service", {"name": smoke.AGENT + ".service.AgentForegroundService", "exported": False,
                     "foregroundServiceType": 0x40000000}),
    ]


def binary_manifest(elements=None, *, utf8=True, typed=False):
    elements = manifest_elements() if elements is None else elements
    strings = []
    for name, attrs in elements:
        for value in [name, *attrs.keys(), *[value for value in attrs.values() if isinstance(value, str)]]:
            if value not in strings:
                strings.append(value)
    offsets, data = [], bytearray()
    for value in strings:
        offsets.append(len(data))
        encoded = value.encode("utf-8" if utf8 else "utf-16-le")
        if utf8:
            data.extend(bytes([len(value), len(encoded)]) + encoded + b"\0")
        else:
            data.extend(struct.pack("<H", len(value)) + encoded + b"\0\0")
    data.extend(b"\0" * (-len(data) % 4))
    start = 28 + 4 * len(strings)
    pool = struct.pack("<HHIIIIII", 1, 28, start + len(data), len(strings), 0, 0x100 if utf8 else 0, start, 0)
    pool += struct.pack("<" + "I" * len(offsets), *offsets) + data

    def element(name, attrs):
        ext = struct.pack("<IIHHHHHH", 0xFFFFFFFF, strings.index(name), 20, 20, len(attrs), 0, 0, 0)
        values = b""
        for key, value in attrs.items():
            if type(value) is bool:
                untyped, kind, data_value = 0xFFFFFFFF, 0x12, 0xFFFFFFFF if value else 0
            elif type(value) is int:
                untyped, kind, data_value = 0xFFFFFFFF, 0x10, value
            else:
                untyped, kind, data_value = 0xFFFFFFFF if typed else strings.index(value), 3, strings.index(value)
            values += struct.pack("<IIIHBBI", 0xFFFFFFFF, strings.index(key), untyped, 8, 0, kind, data_value)
        return struct.pack("<HHIII", 0x102, 16, 16 + len(ext) + len(values), 1, 0xFFFFFFFF) + ext + values

    chunks = pool + b"".join(element(name, attrs) for name, attrs in elements)
    return struct.pack("<HHI", 3, 8, 8 + len(chunks)) + chunks


def write_host_apk(path, elements=None, probe=True):
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("AndroidManifest.xml", binary_manifest(elements))
        archive.writestr("classes.dex", b"host-fixture\0" +
                         (("L" + smoke.PROBE.replace(".", "/") + ";\0").encode("ascii") if probe else b"no-probe"))


class ParserTests(unittest.TestCase):
    def test_incremental_ndjson_split_utf8_and_coalesced_events(self):
        result = envelope("observe", result={"sources": {"tree": source("accessibility", {"nodes": [
            {"text": "\u6d4b\u8bd5", "content_description": "description_sensitive_marker", "node_path": []}
        ]})}})
        raw = wire([result])
        conn, sock, evidence = connection(raw, "observe", [raw[index:index + 1] for index in range(len(raw))])
        self.assertEqual(conn.finish(), result)
        self.assertTrue(sock.closed)
        encoded = json.dumps(evidence.data, ensure_ascii=False)
        self.assertNotIn("\u6d4b\u8bd5", encoded)
        self.assertNotIn("description_sensitive_marker", encoded)
        self.assertNotIn("content_description", encoded)

    def test_action_dispatched_then_completed_and_open_write_side(self):
        conn, sock, _ = connection(wire([dispatched(), completed()]))
        self.assertFalse(conn.event()["terminal"])
        self.assertFalse(sock.closed)
        self.assertEqual(conn.finish()["result"]["completion_source"], "gesture_callback")
        self.assertTrue(sock.closed)

    def test_rejected_queued_action_without_dispatch_is_valid(self):
        conn, _, _ = connection(wire([failed()]))
        self.assertEqual(conn.finish()["error"]["phase"], "queue")

    def test_failed_callback_after_dispatch_is_not_a_success(self):
        conn, _, _ = connection(wire([dispatched(), failed("gesture_cancelled", "completion", "failed")]))
        event = conn.finish()
        with self.assertRaises(smoke.SmokeError):
            smoke.assert_action(event, "gesture_callback")

    def test_current_uncertain_action_and_unsupported_display_errors_parse(self):
        conn, _, _ = connection(wire([dispatched(), failed("action_uncertain", "completion", "failed")]))
        event = conn.finish()
        self.assertEqual(event["error"]["code"], "action_uncertain")
        with self.assertRaisesRegex(smoke.SmokeError, "agent_action_uncertain_completion"):
            smoke.assert_action(event, "accessibility_action_return")
        smoke.assert_error({"code": "unsupported_display", "phase": "observe", "retryable": False})

    def test_completion_without_dispatch_rejected(self):
        conn, _, _ = connection(wire([completed()]))
        with self.assertRaisesRegex(smoke.SmokeError, "completion_without_dispatch"):
            conn.finish()

    def test_rejected_terminal_after_dispatch_is_inconsistent(self):
        conn, _, _ = connection(wire([dispatched(), failed()]))
        with self.assertRaisesRegex(smoke.SmokeError, "rejected_after_dispatch"):
            conn.finish()

    def test_terminal_or_extra_event_rejected(self):
        for raw in (wire([dispatched(), completed(), completed()]), wire([failed()], extra=b"\n")):
            with self.subTest(bytes=len(raw)):
                conn, _, _ = connection(raw)
                with self.assertRaisesRegex(smoke.SmokeError, "extra_response_data"):
                    conn.finish()

    def test_truncated_ndjson_not_terminal_success(self):
        raw = wire([completed()])[:-1]
        conn, _, _ = connection(raw)
        with self.assertRaisesRegex(smoke.SmokeError, "truncated_response"):
            conn.finish()

    def test_conventional_error_assertions(self):
        event = envelope(None, success=False, request_id=None, session_id=None,
                         error={"code": "unauthorized", "phase": "auth", "retryable": False})
        conn, _, _ = connection(wire([event], status=401, stream=False), "health")
        conn.request_id = None
        self.assertEqual(smoke.assert_rejected(conn, 401, "unauthorized", "auth")["http_status"], 401)

    def test_response_identity_mismatches_rejected(self):
        for field in ("request_id", "session_id", "operation"):
            event = completed()
            event[field] = "observe" if field == "operation" else str(uuid.uuid4())
            conn, _, _ = connection(wire([event]))
            with self.subTest(field=field), self.assertRaises(smoke.SmokeError):
                conn.finish()

    def test_error_retryable_and_protocol_are_strict(self):
        for mutate in (
            lambda evt: evt.update(protocol_version=True),
            lambda evt: evt["error"].update(retryable=1),
            lambda evt: evt["error"].update(code="invented"),
            lambda evt: evt.update(success=True),
        ):
            event = failed()
            mutate(event)
            with self.subTest(event_type=type(event["protocol_version"])), self.assertRaises(smoke.SmokeError):
                smoke.assert_envelope(event)

    def test_duplicate_nonfinite_and_invalid_utf8_json_rejected(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":"\xff"}', b"[]"):
            with self.subTest(length=len(raw)), self.assertRaises(smoke.SmokeError):
                smoke.strict_json(raw)

    def test_header_framing_and_bounds(self):
        bad = (
            b"HTTP/1.0 200 OK\r\nConnection: close\r\nCache-Control: no-store",
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nconnection: close\r\nCache-Control: no-store",
            b"HTTP/1.1 200 OK\r\nConnection: close\r\nCache-Control: no-store\r\nTransfer-Encoding: chunked",
        )
        for raw in bad:
            with self.subTest(length=len(raw)), self.assertRaises(smoke.SmokeError):
                smoke.parse_headers(raw)
        reader = smoke.Reader(FakeSocket([b"x" * 8193 + b"\r\n\r\n"]))
        with self.assertRaises(smoke.SmokeError):
            reader.until(b"\r\n\r\n", 8192)

    def test_json_success_is_not_valid_observe_stream(self):
        conn, _, _ = connection(wire([envelope("observe", result={})], stream=False), "observe")
        with self.assertRaisesRegex(smoke.SmokeError, "wrong_success_framing"):
            conn.finish()

    def test_stream_session_required(self):
        conn, _, _ = connection(wire([failed()], headers={"X-Pulse-Session": "INVALID"}))
        with self.assertRaisesRegex(smoke.SmokeError, "missing_stream_session"):
            conn.finish()

    def test_read_deadline_is_absolute_and_timeout_never_success(self):
        reader = smoke.Reader(FakeSocket([socket.timeout()]))
        with self.assertRaisesRegex(smoke.SmokeError, "response_read_timeout"):
            reader.receive()
        reader = smoke.Reader(FakeSocket())
        reader.deadline = 0
        with self.assertRaisesRegex(smoke.SmokeError, "response_read_timeout"):
            reader.receive()


class PrivacyAndObservationTests(unittest.TestCase):
    def test_recursive_redaction(self):
        value = {"Authorization": DUMMY_CREDENTIAL, "safe": [{"Text": "input", "content-description": "label",
                  "base64": "encoded", "access_token": "credential", "checked": False}],
                 "nested": {"token": "credential", "note": "prefix " + DUMMY_CREDENTIAL}}
        result = smoke.redact(value, [DUMMY_CREDENTIAL])
        self.assertEqual(result, {"safe": [{"checked": False}], "nested": {"note": "prefix [redacted]"}})

    def test_source_summary_does_not_store_tree_ocr_or_encoded_image(self):
        value = source("accessibility", {"nodes": [{"text": "private", "content_description": "secret"}],
                                         "blocks": [{"text": "recognized"}], "base64": "screenshot", "width": 10})
        result = smoke.source_summary(value)
        self.assertEqual(result["data_summary"], {"width": 10, "nodes_count": 1, "blocks_count": 1})
        for text in ("private", "secret", "recognized", "screenshot"):
            self.assertNotIn(text, json.dumps(result))

    def test_hint_is_not_previous_input_and_unicode_is_preserved(self):
        node = native_node("et_name", [0], text="\u59d3\u540d", editable=True, hint=True)
        self.assertEqual(smoke.actual_name(node), "")
        node["states"]["showing_hint_text"] = False
        self.assertEqual(smoke.actual_name(node), "\u59d3\u540d")
        del node["states"]["showing_hint_text"]
        with self.assertRaisesRegex(smoke.SmokeError, "name_hint_state_missing"):
            smoke.actual_name(node)

    def test_png_metadata_digest_and_no_image_in_summary(self):
        data = image_data()
        summary = smoke.png_summary(data)
        self.assertEqual((summary["width"], summary["height"]), (1, 1))
        self.assertEqual(len(summary["sha256"]), 64)
        self.assertNotIn("base64", summary)

    def test_corrupt_png_and_wrong_scale_rejected(self):
        data = image_data()
        data["scale_x"] = 1
        with self.assertRaisesRegex(smoke.SmokeError, "png_scale_metadata"):
            smoke.png_summary(data)
        data = image_data()
        raw = bytearray(base64.b64decode(data["base64"]))
        raw[-1] ^= 1
        data["base64"] = base64.b64encode(raw).decode("ascii")
        with self.assertRaisesRegex(smoke.SmokeError, "png_crc"):
            smoke.png_summary(data)

    def test_available_empty_ocr_valid_only_after_success(self):
        value = source("mlkit_chinese", {"blocks": []}, ["ocr_text_only_no_native_states"])
        self.assertTrue(smoke.assert_text_only_ocr(value)["empty_after_success_is_valid"])
        missing = source("mlkit_chinese", status="unavailable", requested=False)
        with self.assertRaisesRegex(smoke.SmokeError, "ocr_unavailable"):
            smoke.assert_text_only_ocr(missing)

    def test_ocr_cannot_fabricate_checkbox_state(self):
        for field in ("checked", "enabled", "states"):
            value = source("mlkit_chinese", {"blocks": [{"text": "label", "bounds": None, "confidence": None,
                                                         field: True}]}, ["ocr_text_only_no_native_states"])
            with self.subTest(field=field), self.assertRaisesRegex(smoke.SmokeError, "ocr_fabricates_native_state"):
                smoke.assert_text_only_ocr(value)

    def test_ocr_native_state_forbidden_in_metadata_too(self):
        value = source("mlkit_chinese", {"blocks": [], "enabled": True}, ["ocr_text_only_no_native_states"])
        with self.assertRaisesRegex(smoke.SmokeError, "ocr_fabricates_native_state"):
            smoke.assert_text_only_ocr(value)

    def test_unrequested_ocr_omits_flags_to_exercise_defaults(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.client = Mock()
        runner.client.request.return_value = envelope("observe", result={"sources": {
            "ocr": source("mlkit_chinese", status="unavailable", requested=False)
        }})
        self.assertEqual(runner.unrequested()["status"], "unavailable")
        runner.client.request.assert_called_once_with("observe", label="unrequested_ocr_defaults")

    def test_native_completeness_must_be_conservative(self):
        node = native_node("tv_form_title", [])
        value = {"sources": {"tree": source("accessibility", {"nodes": [node]},
                                            ["native_semantics_only_visual_coverage_unverified"])},
                 "completeness": {"status": "partial", "reasons": []}}
        self.assertEqual(smoke.tree_nodes(value), [node])
        value["sources"]["tree"]["completeness"]["status"] = "complete"
        with self.assertRaisesRegex(smoke.SmokeError, "native_tree_overclaims_completeness"):
            smoke.tree_nodes(value)

    def test_nondefault_display_is_never_used_for_fixture_mutation(self):
        value = {"sources": {"tree": source("accessibility", {"nodes": [native_node("tv_form_title", [])], "display_id": 1},
                                            ["native_semantics_only_visual_coverage_unverified"])},
                 "completeness": {"status": "partial"}}
        with self.assertRaisesRegex(smoke.SmokeError, "native_tree_not_default_display"):
            smoke.tree_nodes(value)
        data = image_data()
        data["display_id"] = 1
        with self.assertRaisesRegex(smoke.SmokeError, "png_not_default_display"):
            smoke.png_summary(data)

    def test_only_structured_pid_scoped_custom_logs_survive(self):
        secrets = smoke.Secrets()
        secrets.credential(DUMMY_CREDENTIAL)
        raw = ("W/PulseLocalSocket( 42): request_id=" + RID + " phase=auth category=unauthorized\n"
               "I/ThirdParty( 42): arbitrary private text\n").encode()
        rows = smoke.custom_logs(raw, 42, secrets)
        self.assertEqual(rows[0]["request_id"], RID)
        self.assertEqual(len(rows), 1)
        self.assertNotIn("private", json.dumps(rows))
        with self.assertRaises(smoke.SmokeError):
            smoke.custom_logs(b"W/PulseAgentFgs( 42): " + DUMMY_CREDENTIAL.encode(), 42, secrets)
        with self.assertRaisesRegex(smoke.SmokeError, "log_pid_scope_mismatch"):
            smoke.custom_logs(raw, 43, secrets)

    def test_ui_word_coinciding_with_fixed_phase_is_not_a_log_leak(self):
        secrets = smoke.Secrets()
        secrets.values.update({"auth", "queue", "request"})
        raw = ("W/PulseLocalSocket( 42): request_id=" + RID + " phase=auth category=unauthorized\n").encode()
        self.assertEqual(len(smoke.custom_logs(raw, 42, secrets)), 1)

    def test_private_error_records_are_explicitly_sourced_and_structured(self):
        secrets = smoke.Secrets()
        secrets.credential(DUMMY_CREDENTIAL)
        header = ("log_session=" + SID + "\n").encode("ascii")
        raw = header + ("request_id=" + RID + " phase=auth category=unauthorized\n").encode("ascii")
        rows = smoke.private_error_logs(raw, secrets, SID)
        self.assertEqual(rows, [{"source": "app_private_bounded_file", "log_session": SID, "request_id": RID,
                                 "phase": "auth", "category": "unauthorized"}])
        self.assertEqual(smoke.private_error_logs(header, secrets, SID), [])
        for invalid in (b"", DUMMY_CREDENTIAL.encode(), raw + b"secret-input\n", b"\xff", b"x" * (16 * 1024 + 1),
                        header + ("request_id=" + RID + " phase=auth category=unauthorized\n").encode() * 65):
            with self.subTest(size=len(invalid)), self.assertRaises(smoke.SmokeError):
                smoke.private_error_logs(invalid, secrets, SID)
        with self.assertRaisesRegex(smoke.SmokeError, "private_error_log_wrong_session"):
            smoke.private_error_logs(raw, secrets, str(uuid.uuid4()))

    def test_logs_fallback_reads_only_private_records_when_logcat_empty(self):
        evidence = smoke.Evidence("host-test")
        runner = smoke.Smoke(host_args(), evidence)
        runner.pid = 42
        runner.error_log_session = SID
        runner.ensure_instance = Mock()
        runner.adb = SimpleNamespace(run=Mock(side_effect=[
            SimpleNamespace(stdout=b"", returncode=0),
            SimpleNamespace(stdout=("log_session=" + SID + "\nrequest_id=" + RID + " phase=auth category=unauthorized\n").encode(), returncode=0),
            SimpleNamespace(stdout=b"-rw------- 1 app app 100 agent-errors\n", returncode=0),
        ]))
        rows = runner.logs()
        self.assertEqual(rows[0]["source"], "app_private_bounded_file")
        self.assertEqual(evidence.data["error_log_source"], "app_private_bounded_file")
        self.assertEqual(runner.adb.run.call_args_list[1].args,
                         ("shell", "-T", "run-as", smoke.AGENT, "cat", "files/agent-errors"))

    def test_missing_or_public_error_file_is_not_false_log_success(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.pid = 42
        runner.error_log_session = SID
        runner.ensure_instance = Mock()
        runner.adb = SimpleNamespace(run=Mock(side_effect=[
            SimpleNamespace(stdout=b"", returncode=0), SimpleNamespace(stdout=b"", returncode=1),
        ]))
        self.assertEqual(runner.logs(), [])
        runner.adb.run.side_effect = [SimpleNamespace(stdout=b"", returncode=0),
                                    SimpleNamespace(stdout=b"", returncode=0),
                                    SimpleNamespace(stdout=b"-rw-r--r-- 1 app app 0 agent-errors\n", returncode=0)]
        with self.assertRaisesRegex(smoke.SmokeError, "private_error_log_permissions"):
            runner.logs()


class ManifestTests(unittest.TestCase):
    def test_packaged_binary_manifest_utf8_utf16_and_typed_values(self):
        for utf8 in (True, False):
            for typed in (True, False):
                with self.subTest(utf8=utf8, typed=typed):
                    attrs = smoke.manifest_attributes(binary_manifest(utf8=utf8, typed=typed))
                    self.assertEqual(attrs[0][1]["package"], smoke.AGENT)
                    self.assertTrue(smoke.agent_manifest(attrs)["foreground_service_private"])
                    self.assertIs(next(value for tag, value in attrs if tag == "application")["allowBackup"], False)

    def test_agent_apk_sha_and_safe_manifest(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path = Path(directory) / "agent.apk"
            write_host_apk(path)
            result = smoke.apk_info(path)
            self.assertEqual(result["package"], smoke.AGENT)
            self.assertEqual(len(result["sha256"]), 64)
            self.assertTrue(result["debug_probe_descriptor_present"])
            self.assertEqual(result["manifest"]["exported_components"], [smoke.SERVICE.split("/", 1)[1]])

    def test_wrong_package_network_permission_and_unsafe_exports_rejected(self):
        for mutation in ("package", "internet", "export", "permission", "backup", "probe_component", "instrumentation"):
            elements = manifest_elements()
            if mutation == "package":
                elements[0][1]["package"] = "unrelated.package"
            elif mutation == "internet":
                elements.append(("uses-permission", {"name": "android.permission.INTERNET"}))
            elif mutation == "export":
                elements.append(("receiver", {"name": "unrelated.Receiver", "exported": True}))
            elif mutation == "permission":
                elements[6][1]["permission"] = "unrelated.permission"
            elif mutation == "backup":
                elements[5][1]["allowBackup"] = True
            elif mutation == "probe_component":
                elements.append(("service", {"name": smoke.PROBE, "exported": False}))
            else:
                elements.append(("instrumentation", {"name": "obsolete.Runner"}))
            with self.subTest(mutation=mutation), self.assertRaises(smoke.SmokeError):
                smoke.agent_manifest(smoke.manifest_attributes(binary_manifest(elements)))

    def test_private_library_components_and_signature_permission_allowed(self):
        elements = manifest_elements()
        permission = smoke.AGENT + ".DYNAMIC_RECEIVER_NOT_EXPORTED_PERMISSION"
        elements.extend([("permission", {"name": permission, "protectionLevel": 2}),
                         ("uses-permission", {"name": permission}),
                         ("provider", {"name": "library.InternalProvider", "exported": False})])
        self.assertIn(permission, smoke.agent_manifest(elements)["permissions"])
        elements[-3][1]["protectionLevel"] = 0
        with self.assertRaises(smoke.SmokeError):
            smoke.agent_manifest(elements)

    def test_missing_debug_probe_or_non_debug_apk_rejected(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path = Path(directory) / "agent.apk"
            write_host_apk(path, probe=False)
            with self.assertRaisesRegex(smoke.SmokeError, "debug_peer_probe_descriptor_missing"):
                smoke.apk_info(path)
        elements = manifest_elements()
        elements[5][1]["debuggable"] = False
        with self.assertRaisesRegex(smoke.SmokeError, "debug_apk_required"):
            smoke.agent_manifest(elements)

    def test_malformed_axml_rejected(self):
        raw = binary_manifest()
        for bad in (b"", raw[:-1], raw[:8] + b"\0" * (len(raw) - 8)):
            with self.subTest(length=len(bad)), self.assertRaises(smoke.SmokeError):
                smoke.manifest_attributes(bad)


class FakeSettings:
    def __init__(self, services="", flag="0"):
        self.values = {"enabled_accessibility_services": services, "accessibility_enabled": flag}
        self.writes = []

    def setting(self, key):
        return self.values[key]

    def put_setting(self, key, value):
        self.writes.append((key, value))
        self.values[key] = value


def host_args():
    return argparse.Namespace(serial="host-test", adb="adb", lsof=None, apk=Path("unused.apk"))


class OwnershipAndSubmissionTests(unittest.TestCase):
    def test_every_device_command_is_serial_qualified_and_outputs_not_saved(self):
        evidence = smoke.Evidence("host-test")
        adb = smoke.Adb("adb", "host-test", evidence)
        with patch.object(smoke.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=b"stdout_sensitive_marker", stderr=b"stderr_sensitive_marker")) as run:
            adb.run("shell", "-T", "run-as", smoke.AGENT, "tee", "files/adb-peer", label="test", input_bytes=b"dummy")
        self.assertEqual(run.call_args.args[0][:3], ["adb", "-s", "host-test"])
        self.assertEqual(run.call_args.args[0][3:5], ["shell", "-T"])
        self.assertNotIn("dummy", json.dumps(evidence.data))
        self.assertNotIn("stdout_sensitive_marker", json.dumps(evidence.data))
        self.assertNotIn("stderr_sensitive_marker", json.dumps(evidence.data))
        self.assertEqual(evidence.data["commands"][0]["exit_code"], 0)

    def test_invalid_serial_cannot_be_command_option(self):
        for serial in ("", "-d", "serial with space", "serial;command"):
            with self.subTest(serial=serial), self.assertRaises(smoke.SmokeError):
                smoke.Adb("adb", serial, smoke.Evidence(serial))

    def test_subprocess_timeout_is_not_a_pass_and_stdout_never_saved(self):
        evidence = smoke.Evidence("host-test")
        with patch.object(smoke.subprocess, "run", side_effect=subprocess.TimeoutExpired("safe", 1, output=b"stdout_sensitive_marker")):
            with self.assertRaisesRegex(smoke.SmokeError, "command_timeout"):
                smoke.command(["adb", "-s", "host-test"], evidence, "test")
        self.assertTrue(evidence.data["commands"][0]["timed_out"])
        self.assertNotIn("stdout_sensitive_marker", json.dumps(evidence.data))

    def test_health_transient_connect_failure_retries_reads_only(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        ready = envelope("health", result={"status": "ok", "capabilities": {"accessibility": "available"}})
        runner.client = Mock()
        runner.client.request.side_effect = [smoke.SmokeError("loopback_connect_failed", True), ready]
        with patch.object(smoke.time, "sleep"):
            self.assertEqual(runner.health()["status"], "ready")
        self.assertEqual([call.args[0] for call in runner.client.request.call_args_list], ["health", "health"])

    def test_optional_lan_route_unavailable_is_not_false_pass_or_fatal(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        with patch.object(smoke.socket, "socket", side_effect=OSError("no route")):
            runner.evidence.case("same_host_lan_probe", runner.lan_probe, required=False)
        self.assertEqual(runner.evidence.data["cases"][-1]["status"], "blocked")

    def test_action_write_failure_is_not_retried(self):
        evidence = smoke.Evidence("host-test")
        client = smoke.Client(12345, DUMMY_CREDENTIAL, evidence)
        sock = FakeSocket(write_error=True)
        with patch.object(smoke.socket, "create_connection", return_value=sock) as connect:
            with self.assertRaisesRegex(smoke.SmokeError, "request_write_failed_no_retry"):
                client.open("action", {"kind": "tap", "x": 1, "y": 1, "expected_package": smoke.FIXTURE})
        self.assertEqual(connect.call_count, 1)
        self.assertEqual(len(sock.writes), 1)
        self.assertTrue(sock.closed)
        self.assertFalse(client.connections)
        self.assertNotIn(DUMMY_CREDENTIAL, json.dumps(evidence.data))

    def test_four_live_socket_budget_and_loopback_only_client(self):
        client = smoke.Client(12345, DUMMY_CREDENTIAL, smoke.Evidence("host-test"))
        with patch.object(smoke.socket, "create_connection", side_effect=lambda *_args, **_kwargs: FakeSocket()) as connect:
            for _ in range(4):
                client.open("observe")
            with self.assertRaisesRegex(smoke.SmokeError, "host_connection_budget"):
                client.open("observe")
            self.assertEqual(connect.call_count, 4)
            self.assertTrue(all(call.args[0] == ("127.0.0.1", 12345) for call in connect.call_args_list))
        client.close()
        self.assertEqual(client.maximum_live, 4)
        self.assertFalse(client.connections)

    def test_only_added_accessibility_entry_removed_and_other_services_preserved(self):
        adb = FakeSettings("other/.Service", "1")
        lease = smoke.AccessibilityLease(adb)
        lease.enable()
        adb.values["enabled_accessibility_services"] += ":new/.Service"
        lease.restore()
        self.assertEqual(adb.values["enabled_accessibility_services"], "other/.Service:new/.Service")
        self.assertEqual(adb.values["accessibility_enabled"], "1")

    def test_preexisting_agent_accessibility_never_removed(self):
        adb = FakeSettings("other/.Service:" + smoke.SERVICE, "1")
        lease = smoke.AccessibilityLease(adb)
        lease.enable()
        lease.restore()
        self.assertEqual(adb.writes, [])

    def test_preexisting_short_component_and_disabled_flag_restored(self):
        short = smoke.AGENT + "/.service.AgentAccessibilityService"
        adb = FakeSettings("other/.Service:" + short, "0")
        lease = smoke.AccessibilityLease(adb)
        adb.values["enabled_accessibility_services"] = "other/.Service"  # Simulate own APK replacement.
        lease.enable()
        result = lease.restore()
        self.assertEqual(adb.values["accessibility_enabled"], "0")
        self.assertTrue(result["own_activation_stopped"])
        self.assertFalse(result["own_component_removed"])
        self.assertIn(short, adb.values["enabled_accessibility_services"])

    def test_initial_empty_or_absent_settings_restored(self):
        for services in ("", "null"):
            with self.subTest(services=services):
                adb = FakeSettings(services, "null")
                lease = smoke.AccessibilityLease(adb)
                lease.enable()
                lease.restore()
                self.assertEqual(adb.values["enabled_accessibility_services"], services)
                self.assertEqual(adb.values["accessibility_enabled"], "null")

    def test_concurrent_accessibility_enabling_preserved(self):
        adb = FakeSettings()
        lease = smoke.AccessibilityLease(adb)
        lease.enable()
        adb.values["enabled_accessibility_services"] += ":new/.Service"
        result = lease.restore()
        self.assertFalse(result["enable_flag_restored"])
        self.assertEqual(adb.values["accessibility_enabled"], "1")

    def test_only_own_forward_removed(self):
        rows = {("host-test", "tcp:12345", smoke.REMOTE), ("other", "tcp:54321", "localabstract:other")}
        calls = []

        def run(*args, **_kwargs):
            calls.append(args)
            if args[1] == "--remove":
                rows.remove(("host-test", args[2], smoke.REMOTE))
            return SimpleNamespace(stdout="".join(" ".join(row) + "\n" for row in rows).encode())

        lease = smoke.ForwardLease(SimpleNamespace(serial="host-test", run=run))
        lease.port = 12345
        lease.restore()
        self.assertIn(("other", "tcp:54321", "localabstract:other"), rows)
        self.assertIn(("forward", "--remove", "tcp:12345"), calls)
        self.assertFalse(any("--remove-all" in call for call in calls))

    def test_replaced_forward_or_unknown_creation_not_removed(self):
        run = Mock(return_value=SimpleNamespace(stdout=b"host-test tcp:12345 localabstract:replacement\n"))
        lease = smoke.ForwardLease(SimpleNamespace(serial="host-test", run=run))
        lease.port = 12345
        with self.assertRaisesRegex(smoke.SmokeError, "forward_ownership_changed"):
            lease.restore()
        self.assertEqual(run.call_count, 1)
        lease.creation_unknown = True
        with self.assertRaisesRegex(smoke.SmokeError, "forward_creation_outcome_unknown"):
            lease.restore()
        self.assertEqual(run.call_count, 1)

    def test_cleanup_attempts_other_owned_resources_after_failure(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.client = Mock()
        runner.forward = Mock()
        runner.forward.restore.side_effect = smoke.SmokeError("simulated")
        runner.a11y = Mock()
        runner.a11y.added = False
        runner.a11y.restore.return_value = {"restored": True}
        runner.peer_config = Mock(modified=True)
        runner.peer_config.restore.return_value = {"restored": True}
        with self.assertRaisesRegex(smoke.SmokeError, "owned_cleanup_failed"):
            runner.cleanup()
        runner.client.close.assert_called_once()
        runner.peer_config.restore.assert_called_once()
        runner.a11y.restore.assert_called_once()

    def test_queue_cancel_failure_still_drains_neutral_gesture(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        nodes = [native_node("tv_form_title", [0]), native_node("cb_agree", [1], checkable=True)]
        runner.read_tree = Mock(return_value=({}, nodes))
        runner.health = Mock(return_value={"status": "ready"})
        blocker, target = MagicMock(), MagicMock()
        for conn in (blocker, target):
            conn.__enter__.return_value = conn
        blocker.event.return_value = dispatched()
        blocker.finish.return_value = completed()
        blocker.readable.return_value = False
        target.is_stream = True
        target.headers = {"x-pulse-session": SID}
        target.request_id = RID
        runner.client = Mock()
        runner.client.open.side_effect = [blocker, target]
        runner.client.request.return_value = envelope("cancel", result={"cancelled": False})
        with self.assertRaisesRegex(smoke.SmokeError, "target_cancel_not_accepted"):
            runner.queued("cancel")
        blocker.finish.assert_called_once()
        params = runner.client.request.call_args.args[1]
        self.assertEqual(params, {"target_session_id": SID, "target_request_id": RID})

    def test_unexpected_queued_native_dispatch_latches_uncertainty_and_drains(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        nodes = [native_node("tv_form_title", [0]), native_node("cb_agree", [1], checkable=True)]
        runner.read_tree = Mock(return_value=({}, nodes))
        runner.health = Mock(return_value={"status": "ready"})
        blocker, target = MagicMock(), MagicMock()
        for conn in (blocker, target):
            conn.__enter__.return_value = conn
        blocker.event.return_value = dispatched()
        blocker.finish.return_value = completed()
        blocker.readable.return_value = False
        target.is_stream = True
        target.headers = {"x-pulse-session": SID}
        target.request_id = RID
        terminal = failed("action_uncertain", "completion", "failed")
        target.events = [dispatched(), terminal]
        target.finish.return_value = terminal
        runner.client = Mock()
        runner.client.open.side_effect = [blocker, target]
        with self.assertRaisesRegex(smoke.SmokeError, "queued_action_was_dispatched_or_wrong_terminal"):
            runner.queued("timeout")
        blocker.finish.assert_called_once()
        self.assertTrue(runner.recovery_required)
        with self.assertRaisesRegex(smoke.SmokeError, "recovery_required_no_automatic_retry"):
            runner.action({"kind": "node_click", "node_path": [1]}, "accessibility_action_return", "no_replay")

    def test_fixture_compensation_observes_and_does_not_blindly_double_toggle(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.previous_name, runner.previous_checked = "previous", True
        runner.name_dirty = runner.checkbox_touched = True
        nodes = [native_node("et_name", [0], text="previous", editable=True),
                 native_node("cb_agree", [1], checkable=True, checked=True)]
        runner.read_tree = Mock(return_value=({}, nodes))
        runner.action = Mock()
        result = runner.restore_fixture()
        self.assertTrue(result["previous_name_equal"])
        runner.action.assert_not_called()

    def test_post_action_settling_retries_observations_not_actions(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))

        def observation(text):
            return {"sources": {"tree": source("accessibility", {"nodes": [
                native_node("et_name", [0], text=text, editable=True)
            ]}, ["native_semantics_only_visual_coverage_unverified"])}, "completeness": {"status": "partial"}}

        runner.observe = Mock(side_effect=[observation("old"), observation("new")])
        runner.action = Mock()
        with patch.object(smoke.time, "sleep"):
            _, nodes = runner.read_tree("settle", "et_name", lambda nodes: smoke.actual_name(nodes[0]) == "new")
        self.assertEqual(smoke.actual_name(nodes[0]), "new")
        self.assertEqual(runner.observe.call_count, 2)
        runner.action.assert_not_called()

    def test_checkbox_compensation_attempted_even_if_name_restore_fails(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.previous_name, runner.previous_checked = "previous", True
        runner.name_dirty = runner.checkbox_touched = True
        nodes = [native_node("et_name", [0], text="changed", editable=True),
                 native_node("cb_agree", [1], checkable=True, checked=False)]
        runner.read_tree = Mock(return_value=({}, nodes))
        runner.action = Mock(side_effect=[smoke.SmokeError("name_restore_failed"), {"status": "completed"}])
        with self.assertRaisesRegex(smoke.SmokeError, "name_restore_failed"):
            runner.restore_fixture()
        self.assertEqual([call.args[0]["kind"] for call in runner.action.call_args_list], ["set_text", "node_click"])


class FakePrivateFiles:
    def __init__(self, previous=None, directory=True):
        self.value, self.directory = previous, directory
        self.running_token = False
        self.calls = []
        self.events = []
        self.fail_write = False

    def run(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        if args[:4] == ("shell", "-T", "run-as", smoke.AGENT):
            op = args[4:]
        elif args[:3] == ("exec-out", "run-as", smoke.AGENT):
            if kwargs.get("input_bytes") is not None:
                raise AssertionError("exec-out cannot transport stdin/EOF")
            op = args[3:]
        else:
            raise AssertionError("private operation escaped Agent package")
        self.events.append(" ".join(op))
        output, code = b"", 0
        if op == ("ls", "."):
            output = b"files\n" if self.directory else b""
        elif op == ("ls", "-ld", "files"):
            output = b"drwx------ files\n"
        elif op == ("ls", "files"):
            output = (b"adb-peer\n" if self.value is not None else b"") + (b"agent-token\n" if self.running_token else b"")
        elif op == ("ls", "-l", "files/adb-peer"):
            output = b"-rw------- adb-peer\n"
        elif op == ("cat", "files/adb-peer"):
            output = self.value
        elif op == ("mkdir", "files"):
            self.directory = True
        elif op == ("tee", "files/adb-peer"):
            self.value = kwargs["input_bytes"]
            output = self.value
            if self.fail_write:
                self.fail_write = False
                raise smoke.SmokeError("simulated_write_timeout", True)
        elif op == ("rm", "-f", "files/adb-peer"):
            self.value = None
        elif op == ("rmdir", "files"):
            code = 1 if self.value is not None or self.running_token else 0
            if code == 0:
                self.directory = False
        else:
            raise AssertionError("unexpected fake private command")
        return SimpleNamespace(stdout=output, stderr=b"", returncode=code)


class BootstrapTests(unittest.TestCase):
    def test_fresh_kernel_identity_uid_and_pid_are_both_required(self):
        self.assertEqual(smoke.adbd_identity(b"5321\n", b"UID PID NAME\n2000 5321 adbd\n"), EXPECTED_PEER)
        self.assertEqual(smoke.adbd_identity(b"5321", b"UID PID NAME\n0 5321 /apex/bin/adbd\n")["uid"], 0)
        for pid, ps in ((b"5321 5322", b"UID PID NAME\n2000 5321 adbd\n"),
                        (b"5321", b"UID PID NAME\n2000 5322 adbd\n"),
                        (b"5321", b"UID PID NAME\nshell 5321 adbd\n"),
                        (b"5321", b"UID PID NAME\n2000 5321 not-adbd\n"),
                        (b"5321", b"UID PID NAME\n2000 5321 adbd\n2000 5321 adbd\n")):
            with self.subTest(pid_length=len(pid)), self.assertRaises(smoke.SmokeError):
                smoke.adbd_identity(pid, ps)

    def test_installed_base_path_selection_and_shell_injection_rejection(self):
        self.assertEqual(smoke.installed_base_apk(b"package:/data/app/agent/base.apk\npackage:/data/app/agent/split.apk\n"),
                         "/data/app/agent/base.apk")
        for raw in (b"package:/data/app/base.apk;command\n", b"package:/data/app/../base.apk\n",
                    b"package:/data/app/one/base.apk\npackage:/data/app/two/base.apk\n", b"package:/data/app/split.apk\n"):
            with self.subTest(length=len(raw)), self.assertRaises(smoke.SmokeError):
                smoke.installed_base_apk(raw)

    def test_new_private_pin_is_stdin_only_and_removed_if_owned(self):
        adb = FakePrivateFiles(directory=False)
        lease = smoke.PeerConfigLease(adb)
        lease.provision(EXPECTED_PEER)
        self.assertEqual(adb.value, b"2000:5321\n")
        self.assertTrue(lease.modified)
        self.assertFalse(any("2000:5321" in " ".join(args) for args, _ in adb.calls))
        self.assertEqual(next(kwargs["input_bytes"] for args, kwargs in adb.calls if "tee" in args), b"2000:5321\n")
        self.assertTrue(all(args[:4] == ("shell", "-T", "run-as", smoke.AGENT)
                            for args, _ in adb.calls if "tee" in args))
        lease.restore()
        self.assertIsNone(adb.value)
        self.assertFalse(adb.directory)

    def test_previous_private_configuration_restored_exactly_even_if_invalid(self):
        previous = b"preserved-invalid-configuration\n"
        adb = FakePrivateFiles(previous)
        lease = smoke.PeerConfigLease(adb)
        lease.provision(EXPECTED_PEER)
        result = lease.restore()
        self.assertEqual(adb.value, previous)
        self.assertTrue(result["original_configuration_restored"])
        self.assertNotIn(previous.decode().strip(), json.dumps(result))

    def test_concurrent_peer_configuration_changes_are_not_overwritten(self):
        adb = FakePrivateFiles(b"0:1234\n")
        lease = smoke.PeerConfigLease(adb)
        lease.provision(EXPECTED_PEER)
        adb.value = b"2000:9876\n"
        calls = len(adb.calls)
        with self.assertRaisesRegex(smoke.SmokeError, "peer_configuration_ownership_changed"):
            lease.restore()
        self.assertEqual(adb.value, b"2000:9876\n")
        self.assertFalse(any("tee" in args or "rm" in args for args, _ in adb.calls[calls:]))

    def test_timed_out_peer_write_still_restores_owned_modification(self):
        adb = FakePrivateFiles(b"0:1234\n")
        lease = smoke.PeerConfigLease(adb)
        adb.fail_write = True
        with self.assertRaises(smoke.SmokeError):
            lease.provision(EXPECTED_PEER)
        self.assertTrue(lease.modified)
        lease.restore()
        self.assertEqual(adb.value, b"0:1234\n")

    def test_unapplied_private_write_preserves_already_original_file(self):
        adb = FakePrivateFiles(b"0:1234\n")
        lease = smoke.PeerConfigLease(adb)
        lease.provision(EXPECTED_PEER)
        adb.value = lease.previous
        calls = len(adb.calls)
        self.assertTrue(lease.restore()["already_original"])
        self.assertFalse(any("tee" in args or "rm" in args for args, _ in adb.calls[calls:]))

    def test_bootstrap_uses_fresh_identity_before_accessibility(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        adb = FakePrivateFiles()
        runner.peer_config = smoke.PeerConfigLease(adb)
        runner.fresh_peer = Mock(side_effect=[EXPECTED_PEER, EXPECTED_PEER])
        runner.a11y = Mock()
        self.assertTrue(runner.bootstrap()["before_accessibility_enable"])
        self.assertEqual(runner.fresh_peer.call_count, 2)
        runner.a11y.enable.assert_not_called()

    def test_cleanup_stops_owned_accessibility_before_restoring_peer_pin(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        adb = FakePrivateFiles(b"0:1234\n")
        runner.adb = adb
        runner.peer_config = smoke.PeerConfigLease(adb)
        runner.peer_config.provision(EXPECTED_PEER)
        adb.running_token = True
        runner.forward = Mock()
        runner.forward.restore.return_value = {"removed": True}

        def stop():
            adb.events.append("accessibility_stopped")
            adb.running_token = False
            return {"own_component_removed": True}

        runner.a11y = SimpleNamespace(added=True, restore=stop)
        adb.events.clear()
        self.assertTrue(runner.cleanup()["owned_fgs_stopped"])
        self.assertLess(adb.events.index("accessibility_stopped"), adb.events.index("tee files/adb-peer"))
        self.assertEqual(adb.value, b"0:1234\n")

    def test_failed_accessibility_cleanup_retains_pin_until_safe(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.forward = Mock()
        runner.forward.restore.return_value = {"removed": True}
        runner.a11y = Mock(added=True)
        runner.a11y.restore.side_effect = smoke.SmokeError("stop_failed", True)
        runner.peer_config = Mock(modified=True)
        with self.assertRaisesRegex(smoke.SmokeError, "owned_cleanup_failed"):
            runner.cleanup()
        runner.peer_config.restore.assert_not_called()

    def test_owned_global_activation_shutdown_precedes_peer_restore(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.forward = Mock()
        runner.forward.restore.return_value = {"removed": True}
        runner.a11y = SimpleNamespace(added=False, restore=lambda: {"own_activation_stopped": True})
        runner.peer_config = Mock(modified=True)
        runner.peer_config.restore.return_value = {"restored": True}
        runner.adb = SimpleNamespace(run=Mock(return_value=SimpleNamespace(stdout=b"adb-peer\n", returncode=0)))
        result = runner.cleanup()
        self.assertTrue(result["owned_fgs_stopped"])
        runner.adb.run.assert_called_once()
        runner.peer_config.restore.assert_called_once()


def probe_receipt(uid=10234, pid=12345, policy="rejected", provided=True):
    return json.dumps({"peer_policy": policy, "peer_uid": uid, "peer_pid": pid, "token_provided": provided}).encode()


def peer_runner(raw=None):
    runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
    runner.client = SimpleNamespace(credential=DUMMY_CREDENTIAL)
    runner.pid = 42
    runner.expected_peer = dict(EXPECTED_PEER)
    runner.installed_apk = "/data/app/agent/base.apk"
    runner.agent_pid = Mock(return_value=42)
    runner.logs = Mock(return_value=[])
    runner.health = Mock(return_value={"status": "ready"})
    runner.adb = SimpleNamespace(run=Mock(return_value=SimpleNamespace(stdout=raw or probe_receipt(), returncode=0)))
    return runner


class PeerAndCliTests(unittest.TestCase):
    def test_start_waits_for_remote_file_exit_status_and_never_records_credential(self):
        evidence = smoke.Evidence("host-test")
        runner = smoke.Smoke(host_args(), evidence)
        runner.expected_peer = dict(EXPECTED_PEER)
        runner.fresh_peer = Mock(return_value=dict(EXPECTED_PEER))
        runner.a11y = SimpleNamespace(enable=Mock(return_value={}))
        runner.agent_pid = Mock(return_value=42)
        runner.forward.create = Mock(return_value=12345)
        runner.adb = SimpleNamespace(run=Mock(side_effect=[
            SimpleNamespace(returncode=1, stdout=b"", stderr=b"remote_file_not_ready"),
            SimpleNamespace(returncode=0, stdout=DUMMY_CREDENTIAL.encode("ascii"), stderr=b""),
        ]))
        with patch.object(smoke.time, "sleep"):
            self.assertTrue(runner.start()["private_credential_valid"])
        self.assertEqual(runner.adb.run.call_count, 2)
        for call in runner.adb.run.call_args_list:
            self.assertEqual(call.args[:4], ("shell", "-T", "run-as", smoke.AGENT))
        self.assertNotIn(DUMMY_CREDENTIAL, json.dumps(evidence.data))

    def test_probe_requires_rejected_valid_credential_and_app_uid(self):
        self.assertEqual(smoke.probe_result(probe_receipt(), EXPECTED_PEER)["peer_uid"], 10234)
        for raw in (probe_receipt(policy="accepted"), probe_receipt(policy="timeout"), probe_receipt(policy="setup_failed"),
                    probe_receipt(provided=False), probe_receipt(provided="true"), probe_receipt(uid=2000),
                    probe_receipt(uid=True), probe_receipt(pid=0), probe_receipt()[:-1]):
            with self.subTest(length=len(raw)), self.assertRaises(smoke.SmokeError):
                smoke.probe_result(raw, EXPECTED_PEER)

    def test_same_uid_wrong_pid_is_required_not_just_shell_uid(self):
        self.assertEqual(smoke.probe_result(probe_receipt(uid=2000), EXPECTED_PEER, True)["peer_uid"], 2000)
        for raw in (probe_receipt(uid=0), probe_receipt(uid=2000, pid=EXPECTED_PEER["pid"])):
            with self.assertRaisesRegex(smoke.SmokeError, "not_same_uid_wrong_pid"):
                smoke.probe_result(raw, EXPECTED_PEER, True)

    def test_app_uid_probe_is_run_as_and_never_provisions_a_test_token(self):
        runner = peer_runner()
        result = runner.peer()
        self.assertEqual(result["mechanism"], "io_denial_possible_selinux_not_distinguishable")
        self.assertEqual(runner.adb.run.call_args.args,
                         ("shell", "-T", "run-as", smoke.AGENT, "env", "CLASSPATH=/data/app/agent/base.apk",
                          "app_process", "/system/bin", smoke.PROBE))
        self.assertIsNone(runner.adb.run.call_args.kwargs["input_bytes"])
        self.assertEqual(runner.health.call_count, 2)

    def test_wrong_pid_probe_credential_is_exact_stdin_never_argv(self):
        runner = peer_runner(probe_receipt(uid=2000))
        result = runner.peer(True)
        self.assertEqual(result["peer_uid"], 2000)
        args, kwargs = runner.adb.run.call_args.args, runner.adb.run.call_args.kwargs
        self.assertEqual(args, ("shell", "-T", "env", "CLASSPATH=/data/app/agent/base.apk", "app_process", "/system/bin",
                                smoke.PROBE, "--token-stdin"))
        self.assertEqual(kwargs["input_bytes"], DUMMY_CREDENTIAL.encode("ascii"))
        self.assertEqual(len(kwargs["input_bytes"]), 64)
        self.assertNotIn(DUMMY_CREDENTIAL, " ".join(args))
        self.assertEqual(runner.health.call_count, 2)

    def test_server_policy_log_distinguished_from_possible_selinux_denial(self):
        runner = peer_runner(probe_receipt(uid=2000))
        runner.evidence.data["last_error_log_source"] = "pid_scoped_logcat"
        runner.logs.side_effect = [[], [{"category": "forbidden_peer"}]]
        self.assertEqual(runner.peer(True)["mechanism"], "agent_exact_uid_pid_policy")

    def test_peer_policy_source_switch_never_claims_exact_agent_denial(self):
        runner = peer_runner(probe_receipt(uid=2000))
        reads = iter([("pid_scoped_logcat", []), ("app_private_bounded_file", [{"category": "forbidden_peer"}])])
        def logs():
            source, rows = next(reads)
            runner.evidence.data["last_error_log_source"] = source
            return rows
        runner.logs.side_effect = logs
        result = runner.peer(True)
        self.assertFalse(result["agent_exact_peer_policy_log_observed"])

    def test_delayed_prior_private_record_is_not_attributed_to_current_probe(self):
        runner = peer_runner(probe_receipt(uid=2000))
        runner.evidence.data["last_error_log_source"] = "app_private_bounded_file"
        runner.logs.side_effect = [[], [{"category": "forbidden_peer"}]]
        result = runner.peer(True)
        self.assertFalse(result["agent_exact_peer_policy_log_observed"])
        self.assertEqual(result["mechanism"], "io_denial_possible_selinux_not_distinguishable")

    def test_private_error_record_requires_current_run_request(self):
        runner = peer_runner()
        runner.evidence.data["last_error_log_source"] = "app_private_bounded_file"
        runner.evidence.data["responses"] = [{"request_id": RID}]
        runner.logs.return_value = [{"request_id": str(uuid.uuid4()), "phase": "auth", "category": "unauthorized"}]
        with self.assertRaisesRegex(smoke.SmokeError, "current_run_error_record_absent"):
            runner.check_logs()
        runner.logs.return_value = [{"request_id": RID, "phase": "protocol", "category": "unsupported_protocol"}]
        result = runner.check_logs()
        self.assertFalse(result["pid_scoped"])
        self.assertTrue(result["current_run_request_correlated"])
        self.assertTrue(result["private_session_scoped"])

    def test_probe_timeout_still_checks_health_and_never_passes(self):
        runner = peer_runner(probe_receipt(policy="timeout"))
        with self.assertRaisesRegex(smoke.SmokeError, "peer_probe_timeout"):
            runner.peer()
        self.assertEqual(runner.health.call_count, 2)
        runner = peer_runner()
        runner.adb.run.side_effect = smoke.SmokeError("command_timeout_probe", True)
        with self.assertRaisesRegex(smoke.SmokeError, "command_timeout_probe"):
            runner.peer()
        self.assertEqual(runner.health.call_count, 2)

    def test_main_apk_preflight_never_requires_android_test_artifact(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path = Path(directory) / "agent.apk"
            write_host_apk(path)
            runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
            runner.args.apk = path
            self.assertEqual(set(runner.preflight()), {"agent"})

    def test_instance_change_stops_actions_without_credential_adoption(self):
        runner = peer_runner()
        runner.adb.run.return_value = SimpleNamespace(stdout=b"2" * 64, returncode=0)
        with self.assertRaisesRegex(smoke.SmokeError, "instance_changed_requires_new_provisioning"):
            runner.ensure_instance()
        with self.assertRaisesRegex(smoke.SmokeError, "recovery_required_no_automatic_retry"):
            runner.action({"kind": "tap"}, "gesture_callback", "must_not_dispatch")
        self.assertEqual(runner.client.credential, DUMMY_CREDENTIAL)
        runner.health.assert_not_called()

    def test_recovery_capability_stops_read_retries_and_mutations(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.client = Mock()
        runner.client.request.return_value = envelope("health", result={"status": "ok", "capabilities": {
            "accessibility": "available", "recovery_required": True
        }})
        with self.assertRaisesRegex(smoke.SmokeError, "recovery_required_no_automatic_retry"):
            runner.health()
        self.assertEqual(runner.client.request.call_count, 1)

    def test_failed_native_submission_latches_uncertainty_without_replay(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.health = Mock(return_value={"status": "ready"})
        runner.client = Mock()
        runner.client.request.return_value = failed("action_uncertain", "completion", "failed")
        for _ in range(2):
            with self.assertRaises(smoke.SmokeError):
                runner.action({"kind": "node_click", "node_path": [1]}, "accessibility_action_return", "one_attempt")
        self.assertEqual(runner.client.request.call_count, 1)
        self.assertEqual(runner.health.call_count, 1)

    def test_lost_native_receipt_is_never_replayed_or_followed_by_mutation(self):
        runner = smoke.Smoke(host_args(), smoke.Evidence("host-test"))
        runner.health = Mock(return_value={"status": "ready"})
        runner.client = Mock()
        runner.client.request.side_effect = smoke.SmokeError("response_read_timeout", True)
        for _ in range(2):
            with self.assertRaises(smoke.SmokeError):
                runner.action({"kind": "set_text", "node_path": [1], "text": "host-only-input"},
                              "accessibility_action_return", "one_attempt")
        self.assertEqual(runner.client.request.call_count, 1)
        self.assertTrue(runner.recovery_required)

    def test_check_only_never_calls_adb_network_or_writes_evidence(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            target = Path(directory) / "untouched"
            output = io.StringIO()
            with patch.object(smoke.Smoke, "preflight", return_value={"host": True}), \
                    patch.object(smoke.subprocess, "run", side_effect=AssertionError("no commands")), \
                    patch.object(smoke.socket, "create_connection", side_effect=AssertionError("no network")), \
                    redirect_stdout(output):
                code = smoke.main(["--serial", "host-test", "--evidence", str(target), "--check-only"])
            self.assertEqual(code, 0)
            self.assertFalse(target.exists())
            self.assertEqual(json.loads(output.getvalue())["device_commands_executed"], 0)

    def test_existing_root_result_preserved_with_new_unique_runs(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            path = Path(directory) / "result.json"
            path.write_text("historical", encoding="utf-8")
            with patch.object(smoke.Smoke, "run", return_value=None), redirect_stdout(io.StringIO()):
                codes = [smoke.main(["--serial", "host-test", "--evidence", directory]) for _ in range(2)]
            self.assertEqual(codes, [1, 1])  # Fake runs have unmet required cases, never a smoke pass.
            self.assertEqual(path.read_text(encoding="utf-8"), "historical")
            runs = [entry for entry in Path(directory).iterdir() if entry.is_dir()]
            self.assertEqual(len(runs), 2)
            self.assertTrue(all((entry / "result.json").is_file() for entry in runs))

    def test_explicit_run_collision_never_overwrites_or_executes_device_work(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            prior = Path(directory) / "prior"
            prior.mkdir()
            with patch.object(smoke.Smoke, "run") as run, redirect_stdout(io.StringIO()):
                code = smoke.main(["--serial", "host-test", "--evidence", directory, "--run-name", "prior"])
            self.assertEqual(code, 2)
            run.assert_not_called()
            self.assertEqual(list(prior.iterdir()), [])

    def test_run_name_cannot_escape_evidence_root(self):
        with tempfile.TemporaryDirectory(dir=TEMP) as directory:
            with patch.object(smoke.Smoke, "run") as run, redirect_stdout(io.StringIO()):
                code = smoke.main(["--serial", "host-test", "--evidence", directory, "--run-name", "../escape"])
            self.assertEqual(code, 2)
            run.assert_not_called()

    def test_required_unmet_and_optional_skip_are_honest(self):
        evidence = smoke.Evidence("host-test")
        evidence.skip("callback_injection", "unsupported")
        evidence.case("authenticated_health", lambda: smoke.require(False, "device_unavailable", True))
        result = evidence.finish()
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["cases"][0]["status"], "not_exercised")
        self.assertEqual(result["cases"][1]["status"], "blocked")
        self.assertEqual({item["name"] for item in result["cases"] if item["required"]}, set(smoke.REQUIRED))


if __name__ == "__main__":
    unittest.main()
