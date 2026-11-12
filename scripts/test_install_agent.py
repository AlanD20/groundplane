"""PKG-08: unknown Agent acceptance must retain one request identity across retries."""
import os
import json
import subprocess
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
    # IMG-05: names must come from the selected artifact, including independent
    # Agent and Runner versions, rather than the installing Controller's version.
    def test_selected_artifact_tags_are_retained(self):
        cases = [
            ("ghcr.io/aland20/groundplane-agent", {"com.groundplane.source-kind": "release", "org.opencontainers.image.version": "agent/v0.0.1"}, "v0.0.1"),
            ("ghcr.io/aland20/groundplane-runner", {"com.groundplane.source-kind": "release", "com.groundplane.version": "0.0.2", "org.opencontainers.image.version": "2.337.0"}, "v0.0.2"),
            ("ghcr.io/aland20/groundplane-postgres16-tools", {"com.groundplane.image-tag": "v0.0.1"}, "v0.0.1"),
            ("ghcr.io/aland20/groundplane-agent", {"com.groundplane.source-kind": "source_ref", "org.opencontainers.image.revision": "a" * 40}, "a" * 40),
        ]
        for repository, labels, tag in cases:
            labels["org.opencontainers.image.source"] = "https://github.com/AlanD20/groundplane"
            image = repository + "@sha256:" + "b" * 64
            with self.subTest(repository=repository), \
                    patch.object(install_agent.subprocess, "check_output", return_value=json.dumps([{"Config": {"Labels": labels}}])), \
                    patch.object(install_agent.subprocess, "run") as run:
                install_agent.preserve_image_tag(image, replace=True)
                run.assert_called_once_with(["docker", "tag", image, repository + ":" + tag], check=True, timeout=30)
        image = "docker.io/library/postgres:16-alpine@sha256:" + "c" * 64
        with patch.object(install_agent.subprocess, "run") as run:
            install_agent.preserve_image_tag(image, replace=True)
            run.assert_called_once_with(["docker", "tag", image, "docker.io/library/postgres:16-alpine"], check=True, timeout=30)
        with patch.object(install_agent.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)) as run:
            install_agent.preserve_image_tag(image)
            run.assert_called_once_with(["docker", "image", "inspect", "docker.io/library/postgres:16-alpine"], capture_output=True, text=True, timeout=30)
        with patch.object(install_agent.subprocess, "check_output", return_value="[{}]"), \
                patch.object(install_agent.subprocess, "run") as run:
            install_agent.preserve_image_tag(IMAGE)
            run.assert_not_called()

    # PKG-08: ref-built Agents use managed credentials; public pulls must not
    # silently adopt GP's credential selection or overwrite global Docker state.
    def test_pull_selects_credentials_only_for_managed_registry(self):
        for authority in ("localhost:5000", "registry.groundplane.internal:5000", "ghcr.io", "localhost:5000.evil.example"):
            with self.subTest(authority=authority), patch.object(install_agent.subprocess, "run") as run, \
                    patch.object(install_agent.subprocess, "check_output", return_value="[{}]"):
                image = authority + "/groundplane-agent@sha256:" + "a" * 64
                install_agent.pull_image(image)
                command = ["docker"]
                if authority in {"localhost:5000", "registry.groundplane.internal:5000"}:
                    command += ["--config", "/etc/groundplane/registry/client"]
                run.assert_called_once_with([*command, "pull", image], check=True, timeout=600)

    def test_lost_acceptance_reuses_key_and_blocks_a_different_image(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = Transport([INVENTORY, OSError("connection lost after acceptance")])
            retry = Transport([INVENTORY, (202, {"task_id": TASK}),
                               (200, {"task": {"id": TASK, "resource_kind": "agent", "status": "succeeded"}})])
            other = Transport([INVENTORY])
            with patch.object(install_agent, "Path", return_value=root), \
                    patch.object(install_agent, "Receipt", side_effect=lambda *_: controller_update.Receipt(root, os.geteuid())), \
                    patch.object(install_agent, "pull_image"), \
                    patch.object(install_agent, "preserve_image_tag"), \
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
                self.assertEqual(receipt.read()["status"], "succeeded")
            finally:
                receipt.close()
