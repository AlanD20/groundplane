"""Rationale: native releases never replace Runner and must not rebuild or resend it."""
import ipaddress
import io
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import deploy


def target():
    return deploy.Deployment(Path("/keys/qa"), ipaddress.ip_address("192.0.2.42"),
                             "0.0.1", False, None, None, False, False)


class DeploymentImagesTest(unittest.TestCase):
    # Delivery: build environment selection, not build or runtime performance.
    # Rationale: constrain this build without mutating the caller's environment.
    def test_native_compilation_has_bounded_parallelism_without_changing_parent_environment(self):
        parent = {"GOMAXPROCS": "64", "PRESERVED": "yes"}
        with patch.object(deploy, "run") as run, \
                patch.object(deploy, "build_environment", return_value=parent), \
                patch.object(deploy, "local_image_id", return_value="sha256:" + "b" * 64), \
                patch.object(deploy, "image_input_digest", return_value="c" * 64):
            deploy.build_artifacts(target(), "a" * 32, include_runner=False)
        environment = run.call_args_list[0].kwargs["environment"]
        self.assertEqual(environment["GOMAXPROCS"], "2")
        self.assertEqual(environment["PRESERVED"], "yes")
        self.assertEqual(parent["GOMAXPROCS"], "64")

    # Delivery: deployment command error boundary, not network recovery.
    # Rationale: expected transport failures must return failure with a useful message.
    def test_transport_input_and_timeout_errors_are_reported_without_a_traceback(self):
        for error in (ValueError("invalid content identity"), subprocess.TimeoutExpired("image inspection", 30)):
            with self.subTest(error=type(error).__name__), patch.object(deploy, "parse_arguments", return_value=target()), \
                    patch.object(deploy, "deploy", side_effect=error), patch("sys.stderr", new_callable=io.StringIO) as output:
                self.assertEqual(deploy.main(), 1)
                self.assertIn("deployment failed:", output.getvalue())

    # Delivery: image-set selection with fake host probe, not live discovery.
    # Rationale: native updates cannot introduce an unrelated Runner replacement.
    def test_native_target_selects_only_agent_transport(self):
        with patch.object(deploy, "run", return_value=subprocess.CompletedProcess([], 0, "native\n")):
            self.assertFalse(deploy.runner_required(target()))

    # Delivery: bootstrap image-set selection, not installation.
    # Rationale: fresh targets need Runner input, but unknown target mode cannot guess.
    def test_fresh_target_requires_runner_and_unknown_probe_fails_closed(self):
        with patch.object(deploy, "run", return_value=subprocess.CompletedProcess([], 0, "bootstrap\n")):
            self.assertTrue(deploy.runner_required(target()))
        with patch.object(deploy, "run", return_value=subprocess.CompletedProcess([], 0, "unexpected\n")):
            with self.assertRaises(ValueError):
                deploy.runner_required(target())

    # Delivery: recorded build/image commands, not image validity or runtime behavior.
    # Rationale: an Agent-only update must avoid all unrelated Runner build/cache work.
    def test_native_artifact_build_does_not_build_inspect_or_tag_runner(self):
        commands = []
        with patch.object(deploy, "run", side_effect=lambda command, **kwargs: commands.append(command)), \
                patch.object(deploy, "build_environment", return_value={}), \
                patch.object(deploy, "local_image_id", return_value="sha256:" + "b" * 64), \
                patch.object(deploy, "image_input_digest", return_value="c" * 64) as digest:
            agent, runner = deploy.build_artifacts(target(), "a" * 32, include_runner=False)
        self.assertEqual(agent, "groundplane-agent:deploy-" + "a" * 32)
        self.assertEqual(runner, "groundplane-runner:deploy-" + "a" * 32)
        self.assertEqual(digest.call_args_list[0].args[0], "agent")
        self.assertEqual(digest.call_count, 1)
        self.assertFalse(any("runner" in str(command) for command in commands))


if __name__ == "__main__":
    unittest.main()
