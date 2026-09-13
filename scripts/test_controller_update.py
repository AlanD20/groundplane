"""Rationale: deployment follows protected acceptance, never guesses after disconnect."""
import json
import os
from pathlib import Path
import tempfile
import unittest

import controller_update as update

RELEASE = "sha256:" + "a" * 64
TASK = "task_01M2482EK5HAACSE0000000001"


class Transport:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.calls = []

    def request(self, method, path, body=None, key=None):
        self.calls.append((method, path, body, key))
        reply = next(self.replies)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def task(status):
    return 200, {"id": TASK, "type": "update", "target": "controller", "status": status}


class ControllerUpdateClientTest(unittest.TestCase):
    def setUp(self):
        parent = Path(__file__).resolve().parents[1] / ".tmp" / "test-controller-update"
        parent.mkdir(mode=0o700, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=parent)
        self.root = Path(self.temporary.name)
        self.receipt = update.Receipt(self.root, os.geteuid())

    def tearDown(self):
        self.receipt.close()
        self.temporary.cleanup()

    def client(self, transport):
        return update.Client(self.receipt, transport, lambda: 0, lambda seconds: None)

    # QA: UP-11, UI-04; local update client and receipt, not Controller acceptance.
    # Rationale: a lost reply must preserve the protected request and original Task.
    def test_lost_post_replays_same_key_then_reads_only_same_task(self):
        transport = Transport([OSError("lost response"), (202, {"task_id": TASK}),
                               OSError("restarting"), task("running"), task("completed")])
        self.assertEqual(self.client(transport).run(RELEASE, "deploy-protected-key", 10), 0)
        self.assertEqual(transport.calls[0], transport.calls[1])
        self.assertEqual([call[0] for call in transport.calls], ["POST", "POST", "GET", "GET", "GET"])
        self.assertEqual(self.receipt.read()["status"], "completed")

    # QA: UP-11, UI-04; local receipt reuse, not a process-crash recovery test.
    # Rationale: a resumed client must observe the known Task without a second POST.
    def test_restart_retains_task_and_does_not_republish(self):
        original = self.receipt.begin(RELEASE, "deploy-original-key")
        original["task_id"] = TASK
        self.receipt.write(original)
        transport = Transport([task("completed")])
        self.assertEqual(self.client(transport).run(RELEASE, "deploy-different-key", 10), 0)
        self.assertEqual(transport.calls[0][0], "GET")
        self.assertEqual(self.receipt.read()["key"], "deploy-original-key")

    # QA: UP-11; local request ownership, not native publication fencing.
    # Rationale: selecting another release cannot erase an unresolved operation.
    def test_other_release_cannot_replace_uncertain_intent(self):
        self.receipt.begin(RELEASE, "deploy-original-key")
        with self.assertRaisesRegex(ValueError, "unresolved"):
            self.receipt.begin("sha256:" + "b" * 64, "deploy-different-key")
        self.assertEqual(self.receipt.read()["release"], RELEASE)

    # QA: UP-07; local result handling, not predecessor activation or traffic.
    # Rationale: failed native work stays failed and cannot trigger a client Abort.
    def test_failed_update_is_not_success_and_never_aborts(self):
        transport = Transport([(202, {"task_id": TASK}), task("failed")])
        self.assertEqual(self.client(transport).run(RELEASE, "deploy-protected-key", 10), 1)
        self.assertEqual(len(transport.calls), 2)
        self.assertEqual(self.receipt.read()["status"], "failed")

    # QA: UP-11; local observation deadline, not the Controller Task deadline.
    # Rationale: an expired client budget must retain uncertainty without dispatch.
    def test_timeout_keeps_receipt_and_unknown_status(self):
        transport = Transport([])
        self.assertEqual(self.client(transport).run(RELEASE, "deploy-protected-key", 0), 2)
        self.assertEqual(self.receipt.read()["status"], "pending")
        self.assertEqual(transport.calls, [])

    # QA: UP-11; local receipt path and Task identity, not server authorization.
    # Rationale: neither a substituted receipt path nor another Task can settle intent.
    def test_unsafe_receipt_and_mismatched_task_are_rejected(self):
        (self.root / "deployment.json").symlink_to(self.root / "sentinel")
        with self.assertRaises(OSError):
            self.receipt.begin(RELEASE, "deploy-protected-key")
        (self.root / "deployment.json").unlink()
        wrong = task("completed")[1]
        wrong["target"] = "agent"
        transport = Transport([(202, {"task_id": TASK}), (200, wrong)])
        with self.assertRaisesRegex(ValueError, "identity"):
            self.client(transport).run(RELEASE, "deploy-protected-key", 10)
        self.assertEqual(self.receipt.read()["task_id"], TASK)


if __name__ == "__main__":
    unittest.main()
