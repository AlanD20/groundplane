"""Rationale: guarded deployments exit before legacy installation and preserve uncertainty."""
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

import deploy


class NativeDeployBranchTest(unittest.TestCase):
    def run_branch(self, *, stage_only=False, update_status=0, bootstrap=False):
        parent = Path(__file__).resolve().parents[1] / ".tmp" / "test-deploy-native"
        parent.mkdir(mode=0o700, exist_ok=True)
        temporary = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        bundle = root / "groundplane-deploy-0123456789abcdef0123456789abcdef"
        bundle.mkdir(mode=0o700)
        finish = re.search(r"(?ms)^finish\(\) \{\n.*?^\}\n", deploy.REMOTE_INSTALL).group(0)
        start = deploy.REMOTE_INSTALL.index("deployment_mode=$(python3")
        end = deploy.REMOTE_INSTALL.index("runner_ref=$(publish_image", start)
        branch = deploy.REMOTE_INSTALL[start:end]
        script = "\n".join([
            "set -eu", "deploy_dir=$1", "calls=$2",
            "deploy_id=0123456789abcdef0123456789abcdef", "source_agent_image=agent",
            "rollback=0", "retain_recovery=0", f"stage_only={int(stage_only)}", f"bootstrap={int(bootstrap)}",
            "publish_image() { echo pinned-agent; }",
            'systemctl() { echo "unexpected systemctl" >&2; exit 90; }',
            'python3() {',
            '  printf "%s\\n" "$*" >> "$calls"',
            '  case "$1" in',
            '    */controller_bootstrap.py) echo native ;;',
            '    */controller_release.py) echo sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ;;',
            f'    */controller_update.py) return {update_status} ;;',
            '    *) exit 91 ;;',
            '  esac',
            '}',
            finish, "trap finish EXIT HUP INT TERM", branch,
            'echo "unsafe legacy fallthrough" >&2', "exit 92",
        ])
        calls = root / "calls"
        result = subprocess.run(["sh", "-c", script, "--", str(bundle), str(calls)],
                                capture_output=True, text=True, check=False)
        return result, bundle, calls.read_text()

    def test_success_uses_native_task_without_legacy_fallthrough(self):
        result, bundle, calls = self.run_branch()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("controller_update.py sha256:", calls)
        self.assertIn("groundplane-deploy-0123456789abcdef0123456789abcdef", calls)
        self.assertFalse(bundle.exists())

    def test_stage_only_never_activates(self):
        result, bundle, calls = self.run_branch(stage_only=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("controller_update.py", calls)
        self.assertFalse(bundle.exists())

    def test_unknown_or_failed_task_retains_bundle_and_never_rolls_back(self):
        for status in (1, 2):
            with self.subTest(status=status):
                result, bundle, calls = self.run_branch(update_status=status)
                self.assertEqual(result.returncode, status, result.stderr)
                self.assertTrue(bundle.exists())
                self.assertIn("no SSH rollback attempted", result.stderr)
                self.assertNotIn("remove-empty", calls)

    def test_native_installation_refuses_bootstrap_override(self):
        result, bundle, calls = self.run_branch(bootstrap=True)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertNotIn("controller_release.py", calls)
        self.assertFalse(bundle.exists())


if __name__ == "__main__":
    unittest.main()
