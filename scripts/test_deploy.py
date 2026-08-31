#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import ipaddress
import os
import re
import shlex
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("deploy.py")
SPEC = importlib.util.spec_from_file_location("deploy", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load {SCRIPT}")
deploy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = deploy
SPEC.loader.exec_module(deploy)


def shell_function(name: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\(\) \{{\n.*?^\}}\n",
        deploy.REMOTE_INSTALL,
    )
    if match is None:
        raise AssertionError(f"missing shell function: {name}")
    return match.group(0)


class DeployConnectionArgumentsTest(unittest.TestCase):
    def test_ssh_commands_ignore_workstation_and_system_configuration(self) -> None:
        deployment = deploy.Deployment(
            key=Path("/srv/keys/groundplane"),
            ip=ipaddress.ip_address("192.0.2.42"),
            version="v1.2.3",
            setup=False,
            expose_port=None,
            known_hosts=Path("/etc/groundplane/known_hosts"),
        )

        commands = (
            deployment.ssh_base,
            deploy.console_tunnel_command(deployment, 8080),
        )
        for command in commands:
            self.assertEqual(
                command[:5],
                ["ssh", "-i", "/srv/keys/groundplane", "-F", "/dev/null"],
            )
            self.assertEqual(command[-1], deployment.ssh_target)
            for option in (
                "BatchMode=yes",
                "StrictHostKeyChecking=yes",
                "ConnectTimeout=10",
                "ConnectionAttempts=1",
                "UserKnownHostsFile=/etc/groundplane/known_hosts",
                "GlobalKnownHostsFile=/dev/null",
            ):
                self.assertIn(option, command)


class DeployRollbackTest(unittest.TestCase):
    def test_backup_rejects_directory_installation_path(self) -> None:
        self.assert_backup_rejected("directory")

    def test_backup_rejects_symlink_to_directory_installation_path(self) -> None:
        self.assert_backup_rejected("symlink-to-directory")

    def test_backup_rejects_dangling_symlink_installation_path(self) -> None:
        self.assert_backup_rejected("dangling-symlink")

    def test_restore_atomically_replaces_an_executing_binary(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deploy_dir = root / "groundplane-deploy-0123456789abcdef0123456789abcdef"
            deploy_dir.mkdir()
            destination = root / "controller"
            backup = deploy_dir / "backup-controller"
            destination.write_bytes(Path("/bin/sleep").read_bytes())
            destination.chmod(0o755)
            backup.write_bytes(Path("/bin/true").read_bytes())
            backup.chmod(0o701)
            (deploy_dir / "had-controller").touch()

            process = subprocess.Popen([str(destination), "30"])
            try:
                result = subprocess.run(
                    [
                        "sh",
                        "-c",
                        "set -eu\n"
                        "deploy_dir=$1\n"
                        "deploy_id=0123456789abcdef0123456789abcdef\n"
                        f"{shell_function('restore_path')}\n"
                        "restore_path \"$2\" controller\n",
                        "--",
                        str(deploy_dir),
                        str(destination),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                process_was_running = process.poll() is None
            finally:
                process.terminate()
                process.wait(timeout=5)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(process_was_running)
            self.assertEqual(destination.read_bytes(), backup.read_bytes())
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o701)
            self.assertEqual(
                list(root.glob("controller.groundplane-rollback-*")),
                [],
            )

    def test_finish_preserves_original_error_after_complete_rollback(self) -> None:
        result, deploy_dir, systemctl_log, destinations = self.run_finish_fixture(
            fail_stop=False,
        )

        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertFalse(deploy_dir.exists())
        self.assertEqual(systemctl_log.read_text(encoding="utf-8").splitlines()[0],
                         "stop groundplane-controller.service")
        for name, destination in destinations.items():
            self.assertEqual(destination.read_text(encoding="utf-8"), f"old-{name}\n")
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o640)

    def test_finish_retains_recovery_and_prioritizes_rollback_error(self) -> None:
        result, deploy_dir, systemctl_log, destinations = self.run_finish_fixture(
            fail_stop=True,
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(deploy_dir.exists())
        self.assertIn("rollback incomplete; recovery files retained", result.stderr)
        self.assertEqual(systemctl_log.read_text(encoding="utf-8").splitlines()[0],
                         "stop groundplane-controller.service")
        for name, destination in destinations.items():
            self.assertEqual(destination.read_text(encoding="utf-8"), f"old-{name}\n")

    def test_finish_restores_prior_absent_disabled_inactive_unit(self) -> None:
        result, deploy_dir, systemctl_log, destinations = self.run_finish_fixture(
            fail_stop=False,
            prior_unit_present=False,
            service_was_active=False,
            service_was_enabled=False,
            missing_unit_after_reload=True,
        )

        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertFalse(deploy_dir.exists())
        self.assertFalse(destinations["controller-unit"].exists())
        self.assertEqual(
            systemctl_log.read_text(encoding="utf-8").splitlines(),
            ["stop groundplane-controller.service", "daemon-reload"],
        )

    def assert_backup_rejected(self, kind: str) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            deploy_dir = root / "groundplane-deploy-0123456789abcdef0123456789abcdef"
            deploy_dir.mkdir()
            source = root / "controller"
            if kind == "directory":
                source.mkdir()
            elif kind == "symlink-to-directory":
                target = root / "target-directory"
                target.mkdir()
                source.symlink_to(target, target_is_directory=True)
            elif kind == "dangling-symlink":
                source.symlink_to(root / "missing")
            else:
                raise AssertionError(f"unknown fixture kind: {kind}")

            result = subprocess.run(
                [
                    "sh",
                    "-c",
                    "set -eu\n"
                    "deploy_dir=$1\n"
                    f"{shell_function('backup_path')}\n"
                    "backup_path \"$2\" controller\n",
                    "--",
                    str(deploy_dir),
                    str(source),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0, kind)
            self.assertIn("must be a regular file", result.stderr)
            self.assertFalse((deploy_dir / "backup-controller").exists())
            self.assertFalse((deploy_dir / "had-controller").exists())

    def run_finish_fixture(
        self,
        *,
        fail_stop: bool,
        prior_unit_present: bool = True,
        service_was_active: bool = True,
        service_was_enabled: bool = True,
        missing_unit_after_reload: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], Path, Path, dict[str, Path]]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        deploy_dir = root / "groundplane-deploy-0123456789abcdef0123456789abcdef"
        deploy_dir.mkdir()
        systemctl_log = root / "systemctl.log"
        bin_dir = root / "bin"
        bin_dir.mkdir()
        systemctl = bin_dir / "systemctl"
        systemctl.write_text(
            "#!/bin/sh\n"
            "printf '%s\\n' \"$*\" >> \"$SYSTEMCTL_LOG\"\n"
            "if test \"${FAIL_SYSTEMCTL_STOP:-0}\" -eq 1 && test \"$1\" = stop; then\n"
            "    exit 9\n"
            "fi\n"
            "if test \"$1\" = daemon-reload; then\n"
            "    : > \"$SYSTEMCTL_RELOADED\"\n"
            "    exit 0\n"
            "fi\n"
            "if test \"${MISSING_UNIT_AFTER_RELOAD:-0}\" -eq 1 && "
            "test -e \"$SYSTEMCTL_RELOADED\"; then\n"
            "    case \"$1\" in\n"
            "        disable | stop) echo 'Unit groundplane-controller.service not found' >&2; exit 5 ;;\n"
            "    esac\n"
            "fi\n"
            "exit 0\n",
            encoding="utf-8",
        )
        systemctl.chmod(0o755)

        path_names = {
            "/usr/local/libexec/groundplane/controller": "controller",
            "/usr/local/bin/groundplane": "cli",
            "/etc/systemd/system/groundplane-controller.service": "controller-unit",
            "/usr/lib/tmpfiles.d/groundplane.conf": "tmpfiles",
            "/etc/groundplane/controller.yaml": "controller-config",
            "/etc/groundplane/controller.age": "controller-age",
        }
        destinations: dict[str, Path] = {}
        finish = shell_function("finish")
        for original, name in path_names.items():
            destination = root / f"installed-{name}"
            destination.write_text(f"new-{name}\n", encoding="utf-8")
            destination.chmod(0o600)
            backup = deploy_dir / f"backup-{name}"
            if name != "controller-unit" or prior_unit_present:
                backup.write_text(f"old-{name}\n", encoding="utf-8")
                backup.chmod(0o640)
                (deploy_dir / f"had-{name}").touch()
            destinations[name] = destination
            finish = finish.replace(original, shlex.quote(str(destination)))

        command = (
            "set -eu\n"
            "deploy_dir=$1\n"
            "deploy_id=0123456789abcdef0123456789abcdef\n"
            "rollback=1\n"
            f"service_was_active={1 if service_was_active else 0}\n"
            f"service_was_enabled={1 if service_was_enabled else 0}\n"
            "retain_recovery=0\n"
            "unresolved_task_id=\n"
            "unresolved_task_state=unknown\n"
            f"{shell_function('restore_path')}\n"
            f"{finish}\n"
            "trap finish EXIT HUP INT TERM\n"
            "exit 23\n"
        )
        environment = os.environ.copy()
        environment["PATH"] = f"{bin_dir}:{environment['PATH']}"
        environment["SYSTEMCTL_LOG"] = str(systemctl_log)
        environment["SYSTEMCTL_RELOADED"] = str(root / "systemctl-reloaded")
        environment["FAIL_SYSTEMCTL_STOP"] = "1" if fail_stop else "0"
        environment["MISSING_UNIT_AFTER_RELOAD"] = "1" if missing_unit_after_reload else "0"
        result = subprocess.run(
            ["sh", "-c", command, "--", str(deploy_dir)],
            capture_output=True,
            text=True,
            check=False,
            env=environment,
        )
        return result, deploy_dir, systemctl_log, destinations


if __name__ == "__main__":
    unittest.main()
