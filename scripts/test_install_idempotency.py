"""PKG-04: repeated installation must not create another update or hide recovery."""
import unittest
from unittest import mock

import controller_update as update
from test_controller_update import Transport, RELEASE, TASK, task

DIGEST = "sha256:" + "b" * 64
IMAGE = "localhost:5000/groundplane-agent@sha256:" + "c" * 64


def host():
    return {"controller": {"status": "healthy", "update": {
        "available": True, "error": "", "running_sha256": DIGEST,
        "candidate": {"release": RELEASE, "controller_sha256": DIGEST, "agent_image": IMAGE},
        "last_update": None}}, "agent": {"status": "healthy"}, "etcd": {"status": "healthy"}}


def runtime():
    return {"State": {"Running": True}, "Image": "sha256:" + "d" * 64,
            "Config": {"Image": IMAGE, "Labels": {"com.groundplane.managed": "true",
                "com.groundplane.kind": "agent", "com.groundplane.agent-id": "agent-enrolled"}}}


class InstalledProofTest(unittest.TestCase):
    def check(self, document, container=None, installed=DIGEST):
        transport = Transport([(200, document), (200, {"items": [{"id": "agent-enrolled", "status": "healthy"}]})])
        selected = container if container is not None else runtime()
        return update.already_installed(transport, RELEASE, lambda: installed,
                                        lambda image: (selected, "sha256:" + "d" * 64))

    def test_fresh_bootstrap_and_completed_update_can_be_verified_without_mutation(self):
        self.assertTrue(self.check(host()))
        document = host()
        document["controller"]["update"]["last_update"] = {"status": "completed", "phase": "healthy"}
        self.assertTrue(self.check(document))
        # A pre-activation cancellation has no journal phase; Host.available
        # independently proves that no unsettled recovery journal exists.
        document["controller"]["update"]["last_update"] = {"status": "aborted", "phase": ""}
        self.assertTrue(self.check(document))

    def test_different_running_binary_requires_normal_update(self):
        document = host()
        document["controller"]["update"]["running_sha256"] = "sha256:" + "e" * 64
        self.assertFalse(self.check(document))

    def test_absent_agent_requires_guarded_activation_before_enrollment(self):
        document = host()
        document["agent"]["status"] = "unknown"
        transport = Transport([(200, document), (200, {"items": []})])
        self.assertFalse(update.already_installed(transport, RELEASE, lambda: DIGEST))

    def test_health_recovery_selection_and_disk_drift_cannot_be_skipped(self):
        variants = []
        unhealthy = host()
        unhealthy["agent"]["status"] = "degraded"
        variants.append(unhealthy)
        active = host()
        active["controller"]["update"]["last_update"] = {"status": "running", "phase": "starting"}
        variants.append(active)
        changed = host()
        changed["controller"]["update"]["candidate"]["release"] = "sha256:" + "f" * 64
        variants.append(changed)
        for document in variants:
            with self.subTest(document=document), self.assertRaises(ValueError):
                self.check(document)
        with self.assertRaisesRegex(ValueError, "installed bytes"):
            self.check(host(), installed="sha256:" + "f" * 64)

    def test_agent_image_bytes_ownership_and_running_state_are_required(self):
        variants = []
        for path, value in ((("Image",), "foreign"), (("State", "Running"), False),
                            (("Config", "Image"), "wrong:tag"),
                            (("Config", "Labels", "com.groundplane.agent-id"), "other-agent"),
                            (("Config", "Labels", "com.groundplane.managed"), "false")):
            item = runtime()
            target = item
            for part in path[:-1]:
                target = target[part]
            target[path[-1]] = value
            variants.append(item)
        for item in variants:
            with self.subTest(item=item), self.assertRaisesRegex(ValueError, "running Agent differs"):
                self.check(host(), item)


class EnsureClientTest(unittest.TestCase):
    def test_changed_release_uses_normal_protected_update(self):
        receipt = mock.Mock()
        receipt.read.return_value = None
        receipt.begin.return_value = {"release": RELEASE, "key": "deploy-protected-key", "task_id": "", "status": "pending"}
        document = host()
        document["controller"]["update"]["running_sha256"] = "sha256:" + "e" * 64
        transport = Transport([(200, document), (202, {"task_id": TASK}), task("completed")])
        client = update.Client(receipt, transport, sleep=lambda seconds: None)
        self.assertEqual(client.ensure(RELEASE, "deploy-protected-key", 10), 0)
        self.assertEqual([call[0] for call in transport.calls], ["GET", "POST", "GET"])
        self.assertEqual(transport.calls[1], ("POST", "/controller/update", {"release": RELEASE}, "deploy-protected-key"))

    def test_unavailable_host_never_submits_or_writes_a_new_receipt(self):
        receipt = mock.Mock()
        receipt.read.return_value = None
        transport = Transport([(503, None)])
        with self.assertRaisesRegex(ValueError, "cannot verify"):
            update.Client(receipt, transport).ensure(RELEASE, "deploy-protected-key", 10)
        receipt.begin.assert_not_called()
        self.assertEqual([call[0] for call in transport.calls], ["GET"])

    def test_matching_installation_does_not_publish_or_rewrite_receipt(self):
        receipt = mock.Mock()
        receipt.read.return_value = {"status": "completed"}
        transport = mock.Mock()
        client = update.Client(receipt, transport)
        with mock.patch.object(update, "already_installed", return_value=True):
            self.assertEqual(client.ensure(RELEASE, "deploy-protected-key", 10), 0)
        receipt.begin.assert_not_called()
        receipt.write.assert_not_called()
        transport.request.assert_not_called()

    def test_unresolved_receipt_always_resumes_before_already_installed_check(self):
        receipt = mock.Mock()
        value = {"release": RELEASE, "key": "deploy-original-key", "task_id": TASK, "status": "running"}
        receipt.read.return_value = value
        receipt.begin.return_value = value
        transport = Transport([task("completed")])
        with mock.patch.object(update, "already_installed") as proof:
            self.assertEqual(update.Client(receipt, transport).ensure(RELEASE, "deploy-new-key", 10), 0)
        proof.assert_not_called()
        self.assertEqual(transport.calls, [("GET", "/tasks/" + TASK, None, None)])
