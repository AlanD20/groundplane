"""PKG-08: unknown Agent acceptance must retain one request identity across retries."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import controller_update
import install_agent
from test_controller_update import Transport, TASK

IMAGE = "ghcr.io/aland20/groundplane-agent@sha256:" + "a" * 64
AGENT = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
INVENTORY = (200, {"items": [{"id": AGENT, "status": "healthy"}]})


class AgentInstallationTests(unittest.TestCase):
    def test_lost_acceptance_reuses_key_and_blocks_a_different_image(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = Transport([INVENTORY, OSError("connection lost after acceptance")])
            retry = Transport([INVENTORY, (202, {"task_id": TASK}), (200, {"status": "completed"})])
            other = Transport([INVENTORY])
            with patch.object(install_agent, "Path", return_value=root), \
                    patch.object(install_agent, "Receipt", side_effect=lambda *_: controller_update.Receipt(root, os.geteuid())), \
                    patch.object(install_agent.subprocess, "run"), \
                    patch.object(install_agent, "current_image", return_value="old-image"), \
                    patch.object(install_agent, "Transport", return_value=first):
                with self.assertRaises(OSError):
                    install_agent.install(IMAGE)
                with patch.object(install_agent, "Transport", return_value=other):
                    with self.assertRaisesRegex(ValueError, "unresolved acceptance"):
                        install_agent.install("ghcr.io/aland20/groundplane-agent@sha256:" + "b" * 64)
                self.assertEqual(len(other.calls), 1)
                with patch.object(install_agent, "Transport", return_value=retry), \
                        patch.object(install_agent, "current_image", return_value=IMAGE):
                    install_agent.install(IMAGE)
            self.assertEqual(first.calls[1], retry.calls[1])
            self.assertEqual(retry.calls[1][2], {"image": IMAGE})
            receipt = controller_update.Receipt(root, os.geteuid())
            try:
                self.assertEqual(receipt.read()["status"], "completed")
            finally:
                receipt.close()
