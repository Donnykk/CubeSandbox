#!/usr/bin/env python3
# Copyright (c) 2026 Tencent Inc.
# SPDX-License-Identifier: Apache-2.0
"""Exercise startup replay and namespace checks without SPDK or block devices."""

import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


COMMON = Path(__file__).resolve().parents[2] / "scripts" / "rcow_common.sh"
FAKE_RPC = r'''
import json
import os
from pathlib import Path
import sys

root = Path(os.environ["REPLAY_TEST_DIR"])

def _send(sock, timeout, method, params):
    records = json.loads((root / "namespaces.json").read_text())
    nqn = params["nqn"]
    return True, [{"nqn": nqn, "namespaces": records.get(nqn, [])}], False

if __name__ == "__main__":
    if sys.argv[1] == "rcow_get_lvstores":
        print((root / "inventory.json").read_text())
    elif sys.argv[1] == "rcow_active_bdev_batch":
        request = json.loads(sys.argv[2])
        with (root / "batches.jsonl").open("a") as output:
            output.write(json.dumps(request) + "\n")
        print(json.dumps({"restored": len(request["volumes"]), "already_active": 0, "failed": []}))
    else:
        raise SystemExit("unexpected RPC: " + sys.argv[1])
'''


class StartupReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "rpc.py").write_text(FAKE_RPC)
        (self.root / "namespaces.json").write_text("{}")
        (self.root / "inventory.json").write_text("[]")
        self.env = dict(os.environ, REPLAY_TEST_DIR=str(self.root),
                        RCOW_RUN_DIR=str(self.root), RCOW_LVS_NAME="test-store",
                        RCOW_ACTIVE_FILE=str(self.root / "active.json"),
                        RCOW_REPLAY_FILE=str(self.root / "replay.json"),
                        RCOW_RPC_PY=str(self.root / "rpc.py"),
                        RCOW_RPC_SOCK=str(self.root / "rpc.sock"),
                        RCOW_RPC_TIMEOUT="2", RCOW_NUM_SUBSYS="4",
                        RCOW_NQN_PREFIX="nqn.test:")

    def run_shell(self, body):
        setup = "set -euo pipefail\nsource " + shlex.quote(str(COMMON)) + "\n"
        setup += 'rcow_rpc() { python3 "$RCOW_RPC_PY" "$@"; }\n'
        return subprocess.run(["bash", "-c", setup + body], env=self.env,
                              cwd=self.root, text=True, capture_output=True)

    def assert_success(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def write_json(self, name, value):
        (self.root / name).write_text(json.dumps(value))

    @staticmethod
    def entry(number, subsystem=0, nsid=None):
        return {"uuid": "00000000-0000-0000-0000-%012d" % number,
                "subsys": subsystem, "nsid": number if nsid is None else nsid}

    def test_stage_combines_active_and_pending_volumes(self):
        active = {"volume-a": self.entry(1)}
        pending = {"volume-b": self.entry(2)}
        self.write_json("active.json", active)
        self.write_json("replay.json", pending)
        self.assert_success(self.run_shell("rcow_stage_start_replay"))
        self.assertEqual(json.loads((self.root / "replay.json").read_text()), dict(active, **pending))

    def test_stage_preserves_files_on_identity_or_slot_conflict(self):
        active = {"volume-a": self.entry(1)}
        for pending in [{"volume-a": self.entry(2)}, {"volume-b": self.entry(2, nsid=1)}]:
            with self.subTest(pending=pending):
                self.write_json("active.json", active)
                self.write_json("replay.json", pending)
                result = self.run_shell("rcow_stage_start_replay")
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(json.loads((self.root / "active.json").read_text()), active)
                self.assertEqual(json.loads((self.root / "replay.json").read_text()), pending)

    def test_layout_requires_exact_namespace_identity(self):
        entry = self.entry(1)
        self.write_json("replay.json", {"volume-a": entry})
        good = {"nsid": 1, "uuid": entry["uuid"]}
        for namespaces, success in [([good], True), ([], False),
                                    ([dict(good, uuid="changed")], False),
                                    ([good, dict(good, nsid=2)], False),
                                    ([good, good], False)]:
            with self.subTest(namespaces=namespaces):
                self.write_json("namespaces.json", {"nqn.test:00": namespaces})
                result = self.run_shell('rcow_verify_replay_layout "$RCOW_REPLAY_FILE"')
                self.assertEqual(result.returncode == 0, success, result.stderr)

    def test_startup_retains_plan_until_layout_verification_succeeds(self):
        plan = {"volume-a": self.entry(1)}
        for failure in ["replay", "layout", "none"]:
            with self.subTest(failure=failure):
                self.write_json("replay.json", plan)
                replay_status = "1" if failure == "replay" else "0"
                layout_status = "1" if failure == "layout" else "0"
                body = ('rcow_stage_start_replay() { :; }\n'
                        'rcow_replay_registry() { test "$1" = startup; return ' + replay_status + '; }\n'
                        'rcow_verify_replay_layout() { test -s "$1"; return ' + layout_status + '; }\n'
                        'rcow_restore_before_listen')
                result = self.run_shell(body)
                self.assertEqual(result.returncode == 0, failure == "none", result.stderr)
                self.assertEqual((self.root / "replay.json").exists(), failure != "none")

    def test_replay_uses_bounded_batches_and_keeps_startup_plan(self):
        plan = {"volume-%03d" % i: self.entry(i + 1, i // 64, i % 64 + 1) for i in range(205)}
        self.write_json("replay.json", plan)
        inventory = [{"lvs_name": "test-store", "lvol_open_failures_complete": True,
                      "lvols": [{"name": name, "uuid": entry["uuid"]} for name, entry in plan.items()]}]
        self.write_json("inventory.json", inventory)
        self.assert_success(self.run_shell("rcow_replay_registry startup"))
        batches = [json.loads(line)["volumes"] for line in (self.root / "batches.jsonl").read_text().splitlines()]
        self.assertEqual([len(batch) for batch in batches], [96, 96, 13])
        self.assertEqual(json.loads((self.root / "replay.json").read_text()), plan)

    def test_startup_refuses_missing_volume_without_dropping_the_plan(self):
        plan = {"volume-a": self.entry(1)}
        self.write_json("replay.json", plan)
        self.write_json("inventory.json", [{"lvs_name": "test-store", "lvol_open_failures_complete": True, "lvols": []}])
        result = self.run_shell("rcow_replay_registry startup")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(json.loads((self.root / "replay.json").read_text()), plan)
        self.assertFalse((self.root / "batches.jsonl").exists())

    def test_device_verification_waits_for_reconnect_and_rejects_changed_uuid(self):
        source = COMMON.read_text()
        start = source.index("rcow_verify_active()\n{")
        end = source.index("\n# Apply each active volume", start)
        record = [{"device_name": "volume-a", "uuid": "original", "subsys": 0,
                   "nsid": 1, "device_path": "/dev/test-volume"}]
        self.write_json("expected.json", record)
        self.write_json("live.json", record)
        setup = r'''
set -euo pipefail
RCOW_RUN_DIR="$PWD"
rcow_log() { echo "$*"; }
rcow_err() { echo "$*" >&2; }
rcow_die() { echo "$*" >&2; exit 1; }
function [ {
    if builtin [ "$#" -eq 4 ] && builtin [ "$1" = '!' ] && builtin [ "$2" = '-b' ]; then
        return 1
    fi
    builtin [ "$@"
}
rcow_rpc() {
    local n
    n=$(cat count 2>/dev/null || echo 0)
    n=$((n + 1)); echo "$n" > count
    if (( n <= 2 )); then echo '[]'; else cat live.json; fi
}
'''
        script = setup + source[start:end] + "\nrcow_verify_active --expect expected.json 6\n"
        result = subprocess.run(["bash", "-c", script], cwd=self.root, text=True, capture_output=True)
        self.assert_success(result)
        self.assertGreaterEqual(int((self.root / "count").read_text()), 3)
        record[0]["uuid"] = "different"
        self.write_json("live.json", record)
        (self.root / "count").write_text("2")
        result = subprocess.run(["bash", "-c", script], cwd=self.root, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(StartupReplayTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    failed = len(result.failures) + len(result.errors)
    passed = result.testsRun - failed - len(result.skipped)
    print("=== %d passed, %d failed ===" % (passed, failed))
    raise SystemExit(0 if result.wasSuccessful() else 1)
