"""Rationale: low checkout capacity must fail before builds or target writes."""
import ipaddress
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import deploy
import deployment_capacity


def target():
    return deploy.Deployment(Path("/keys/qa"), ipaddress.ip_address("192.0.2.42"),
                             "0.0.1", False, None, None, False, False)


class DeploymentCapacityTest(unittest.TestCase):
    def test_exact_threshold_passes_and_one_byte_below_fails(self):
        for minimum in (deployment_capacity.BUILD_HEADROOM, deployment_capacity.TRANSFER_HEADROOM):
            with self.subTest(minimum=minimum):
                with patch("shutil.disk_usage", return_value=SimpleNamespace(free=minimum)) as usage:
                    deployment_capacity.require_capacity(Path("/checkout"), minimum)
                    usage.assert_called_once_with(Path("/checkout"))
                with patch("shutil.disk_usage", return_value=SimpleNamespace(free=minimum - 1)):
                    with self.assertRaisesRegex(RuntimeError, "insufficient free space"):
                        deployment_capacity.require_capacity(Path("/checkout"), minimum)

    def test_capacity_inspection_failure_is_not_ignored(self):
        with patch("shutil.disk_usage", side_effect=OSError("unavailable")):
            with self.assertRaises(OSError):
                deployment_capacity.require_capacity(Path("/checkout"))

    def test_space_is_rechecked_before_an_uncached_image_build(self):
        with patch("shutil.disk_usage", side_effect=[SimpleNamespace(free=20 << 30), SimpleNamespace(free=1)]), \
                patch.object(deploy, "run") as run, \
                patch.object(deploy, "build_environment", return_value={}), \
                patch.object(deploy, "local_image_id", return_value=None), \
                patch.object(deploy, "image_input_digest", return_value="c" * 64):
            with self.assertRaises(RuntimeError):
                deploy.build_artifacts(target(), "a" * 32, include_runner=False)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.args[0][:3], ["make", "controller", "cli"])

    def test_low_space_refuses_artifact_build_before_any_command(self):
        with patch("shutil.disk_usage", return_value=SimpleNamespace(free=1)), \
                patch.object(deploy, "run") as run, \
                patch.object(deploy, "build_environment", return_value={}), \
                patch.object(deploy, "local_image_id", return_value="sha256:" + "b" * 64), \
                patch.object(deploy, "image_input_digest", return_value="c" * 64):
            with self.assertRaisesRegex(RuntimeError, "free.*GiB"):
                deploy.build_artifacts(target(), "a" * 32, include_runner=False)
            run.assert_not_called()

    def test_low_space_refuses_deployment_before_target_access(self):
        with patch("shutil.disk_usage", return_value=SimpleNamespace(free=1)), \
                patch.object(deploy, "require_local_tools"), \
                patch.object(deploy, "verify_architecture", side_effect=AssertionError("target accessed")):
            with self.assertRaisesRegex(RuntimeError, "free.*GiB"):
                deploy.deploy(target())


if __name__ == "__main__":
    unittest.main()
