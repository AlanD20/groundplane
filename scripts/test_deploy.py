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

TEST_TEMPORARY_ROOT = SCRIPT.parents[1] / ".tmp" / "test-deploy"


def temporary_directory() -> tempfile.TemporaryDirectory:
    deploy.ensure_private_directory(deploy.LOCAL_DEPLOY_ROOT)
    deploy.ensure_private_directory(TEST_TEMPORARY_ROOT)
    return tempfile.TemporaryDirectory(dir=TEST_TEMPORARY_ROOT)


def shell_function(name: str) -> str:
    match = re.search(
        rf"(?ms)^{re.escape(name)}\(\) \{{\n.*?^\}}\n",
        deploy.REMOTE_INSTALL,
    )
    if match is None:
        raise AssertionError(f"missing shell function: {name}")
    return match.group(0)


class DeployConnectionArgumentsTest(unittest.TestCase):
    # Delivery: deployment transport arguments, not an SSH connection or product case.
    # Rationale: ambient SSH config must not redirect a deployment or weaken host trust.
    def test_ssh_commands_ignore_workstation_and_system_configuration(self) -> None:
        deployment = deploy.Deployment(
            key=Path("/srv/keys/groundplane"),
            ip=ipaddress.ip_address("192.0.2.42"),
            version="v1.2.3",
            setup=False,
            expose_port=None,
            known_hosts=Path("/etc/groundplane/known_hosts"),
            stage_only=False,
            bootstrap=False,
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


class DeployStagingPathTest(unittest.TestCase):
    @staticmethod
    def receive_script(root: Path) -> str:
        private_root = root / "groundplane"
        private_parent = private_root / ".tmp"
        script = deploy.REMOTE_RECEIVE
        script = script.replace(
            "/root/.groundplane/.tmp",
            str(private_parent),
        )
        script = script.replace("/root/.groundplane", str(private_root))
        script = script.replace(" -o root -g root", "")
        script = script.replace(
            "if test \"$(stat -c '%u:%a' \"$private_parent\")\" != \"0:700\"; then",
            "if test \"$(stat -c '%a' \"$private_parent\")\" != \"700\"; then",
        )
        return script.replace(
            "/run/lock/groundplane-deploy.lock",
            str(root / "deployment.lock"),
        )

    def run_receive(
        self,
        root: Path,
        deploy_dir: Path,
        bundle: bytes = b"",
    ) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            [
                "sh",
                "-c",
                self.receive_script(root),
                "--",
                "0",
                str(deploy_dir),
                "dev",
                "agent",
                "runner",
                "127.0.0.1",
                "0",
                "0",
            ],
            input=bundle,
            capture_output=True,
            check=False,
        )

    # Delivery: executed path guard only, not a remote installation.
    # Rationale: the former system-temp namespace cannot authorize staging effects.
    def test_remote_guard_rejects_old_system_tmp_path(self) -> None:
        command = "set -eu\ndeploy_dir=$1\n" + deploy.REMOTE_DEPLOY_GUARD
        valid = subprocess.run(
            [
                "sh",
                "-c",
                command,
                "--",
                "/root/.groundplane/.tmp/groundplane-deploy-0123456789abcdef0123456789abcdef",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        old = subprocess.run(
            [
                "sh",
                "-c",
                command,
                "--",
                "/tmp/groundplane-deploy-0123456789abcdef0123456789abcdef",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stderr)
        self.assertNotEqual(old.returncode, 0, old.stderr)

    # Delivery: receive-script cleanup ownership on disposable local paths.
    # Rationale: a colliding staging directory is foreign state, not cleanup authority.
    def test_existing_deployment_directory_and_sentinel_survive_rejection(self) -> None:
        with temporary_directory() as temporary:
            root = Path(temporary)
            private_parent = root / "groundplane" / ".tmp"
            private_parent.mkdir(parents=True)
            deploy_dir = private_parent / (
                "groundplane-deploy-0123456789abcdef0123456789abcdef"
            )
            deploy_dir.mkdir()
            sentinel = deploy_dir / "sentinel"
            sentinel.write_text("keep\n", encoding="utf-8")

            result = self.run_receive(root, deploy_dir)

            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertTrue(deploy_dir.is_dir())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    # Delivery: local receive-script path rejection, not remote filesystem ownership.
    # Rationale: a dangling symlink is still pre-existing state and cannot be adopted.
    def test_dangling_deployment_symlink_survives_rejection(self) -> None:
        with temporary_directory() as temporary:
            root = Path(temporary)
            private_parent = root / "groundplane" / ".tmp"
            private_parent.mkdir(parents=True)
            deploy_dir = private_parent / (
                "groundplane-deploy-0123456789abcdef0123456789abcdef"
            )
            deploy_dir.symlink_to(root / "missing")

            result = self.run_receive(root, deploy_dir)

            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertTrue(deploy_dir.is_symlink())

    # Delivery: local receive-script ancestor validation.
    # Rationale: replacing the private root with a symlink must not expose its target.
    def test_symlinked_private_root_rejects_without_touching_target(self) -> None:
        with temporary_directory() as temporary:
            root = Path(temporary)
            private_root = root / "groundplane"
            target = root / "private-target"
            target.mkdir()
            sentinel = target / "sentinel"
            sentinel.write_text("keep\n", encoding="utf-8")
            private_root.symlink_to(target, target_is_directory=True)
            deploy_dir = private_root / ".tmp" / (
                "groundplane-deploy-0123456789abcdef0123456789abcdef"
            )

            result = self.run_receive(root, deploy_dir)

            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertTrue(private_root.is_symlink())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    # Delivery: local receive-script immediate-parent validation.
    # Rationale: trusting the root alone must not permit a substituted staging parent.
    def test_symlinked_private_tmp_parent_rejects_without_touching_target(self) -> None:
        with temporary_directory() as temporary:
            root = Path(temporary)
            private_root = root / "groundplane"
            private_root.mkdir()
            private_parent = private_root / ".tmp"
            target = root / "tmp-target"
            target.mkdir()
            sentinel = target / "sentinel"
            sentinel.write_text("keep\n", encoding="utf-8")
            private_parent.symlink_to(target, target_is_directory=True)
            deploy_dir = private_parent / (
                "groundplane-deploy-0123456789abcdef0123456789abcdef"
            )

            result = self.run_receive(root, deploy_dir)

            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertTrue(private_parent.is_symlink())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    # Delivery: local failed-transfer cleanup, not a remote deployment.
    # Rationale: corrupt archives must release only the directory this receive created.
    def test_created_deployment_directory_is_cleaned_after_receive_failure(self) -> None:
        with temporary_directory() as temporary:
            root = Path(temporary)
            private_parent = root / "groundplane" / ".tmp"
            private_parent.mkdir(parents=True)
            deploy_dir = private_parent / (
                "groundplane-deploy-0123456789abcdef0123456789abcdef"
            )

            result = self.run_receive(
                root,
                deploy_dir,
                b"not a tar archive\n",
            )

            self.assertNotEqual(result.returncode, 0, result.stderr)
            self.assertFalse(deploy_dir.exists())


class DeployRollbackTest(unittest.TestCase):
    # QA: HOST-02; bootstrap file capture only, not native update recovery.
    # Rationale: a directory cannot masquerade as a recoverable installed binary.
    def test_backup_rejects_directory_installation_path(self) -> None:
        self.assert_backup_rejected("directory")

    # QA: HOST-02; local bootstrap file capture only.
    # Rationale: following a directory symlink would capture outside installation state.
    def test_backup_rejects_symlink_to_directory_installation_path(self) -> None:
        self.assert_backup_rejected("symlink-to-directory")

    # QA: HOST-02; local bootstrap file capture only.
    # Rationale: a dangling installed-path symlink is not an absent owned file.
    def test_backup_rejects_dangling_symlink_installation_path(self) -> None:
        self.assert_backup_rejected("dangling-symlink")

    # QA: HOST-02; local bootstrap rollback file operation, not Controller recovery.
    # Rationale: replacing an executing inode must preserve process liveness while
    # restoring exact predecessor bytes/mode without a partial destination.
    def test_restore_atomically_replaces_an_executing_binary(self) -> None:
        with temporary_directory() as temporary:
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

    # QA: HOST-02; bootstrap trap with fake systemd, not native update recovery.
    # Rationale: successful compensation must restore bytes/modes and retain the
    # original deployment failure, not report success.
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

    # QA: HOST-02; bootstrap trap with fake systemd, not a real stop failure.
    # Rationale: failed stop makes overwrite unsafe; keep installation and evidence.
    def test_finish_retains_recovery_and_prioritizes_rollback_error(self) -> None:
        result, deploy_dir, systemctl_log, destinations = self.run_finish_fixture(
            fail_stop=True,
        )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(deploy_dir.exists())
        self.assertIn("Controller stop failed; retaining installation and recovery files", result.stderr)
        self.assertEqual(systemctl_log.read_text(encoding="utf-8").splitlines()[0],
                         "stop groundplane-controller.service")
        for name, destination in destinations.items():
            self.assertEqual(destination.read_text(encoding="utf-8"), f"new-{name}\n")

    # QA: HOST-02; local bootstrap trap, not real systemd unit lifecycle.
    # Rationale: rollback of first installation must not invent or operate a prior unit.
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
        with temporary_directory() as temporary:
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
        temporary = temporary_directory()
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
            "/usr/local/libexec/groundplane/controller-recovery": "controller-recovery",
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
            "native_initialized=0\n"
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


class DeployAgentIdlePreflightTest(unittest.TestCase):
    def run_dispatch_fixture(
        self,
        responses: list[str],
        *,
        list_failure: bool = False,
    ) -> tuple[subprocess.CompletedProcess[str], str, str]:
        with temporary_directory() as temporary:
            root = Path(temporary)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            cli = bin_dir / "groundplane"
            list_count = root / "list-count"
            list_count.write_text("0\n", encoding="utf-8")
            update_log = root / "update.log"
            update_log.write_text("", encoding="utf-8")
            response_paths = []
            for index, response in enumerate(responses):
                response_path = root / f"agent-list-{index}"
                response_path.write_text(response + "\n", encoding="utf-8")
                response_paths.append(response_path)

            cases = "".join(
                f'        {index}) cat {shlex.quote(str(path))} ;;\n'
                for index, path in enumerate(response_paths)
            )
            last_response = shlex.quote(str(response_paths[-1]))
            cli.write_text(
                "#!/bin/sh\n"
                'case "$*" in\n'
                '    *"agent list")\n'
                '        if test "${AGENT_LIST_FAILURE:-0}" -eq 1; then\n'
                "            echo 'agent list read failed' >&2\n"
                "            exit 17\n"
                "        fi\n"
                '        count=$(sed -n \'1p\' "$AGENT_LIST_COUNT")\n'
                "        case \"$count\" in\n"
                f"{cases}"
                f"            *) cat {last_response} ;;\n"
                "        esac\n"
                '        printf \'%s\\n\' "$((count + 1))" > "$AGENT_LIST_COUNT"\n'
                "        ;;\n"
                '    *"agent update --all --image "*)\n'
                '        printf \'update\\n\' >> "$AGENT_UPDATE_LOG"\n'
                '        printf \'{\\n  "task_id": "task_1"\\n}\\n\'\n'
                "        ;;\n"
                "    *)\n"
                "        echo \"unexpected CLI operation: $*\" >&2\n"
                "        exit 19\n"
                "        ;;\n"
                "esac\n",
                encoding="utf-8",
            )
            cli.chmod(0o755)

            load_agent_list = shell_function("load_agent_list").replace(
                "/usr/local/bin/groundplane", shlex.quote(str(cli))
            )
            dispatch_agent_update = shell_function("dispatch_agent_update").replace(
                "/usr/local/bin/groundplane", shlex.quote(str(cli))
            ).replace("sleep 1", ":")
            command = (
                "set -eu\n"
                "agent_task_id=\n"
                "retain_recovery=0\n"
                "unresolved_task_id=\n"
                "unresolved_task_state=unknown\n"
                f"{load_agent_list}\n"
                f"{dispatch_agent_update}\n"
                "if ! load_agent_list; then exit 1; fi\n"
                "selected_agent_id=$agent_id\n"
                "agent_ref=registry.example/agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
                "dispatch_agent_update\n"
            )
            environment = os.environ.copy()
            environment["AGENT_LIST_COUNT"] = str(list_count)
            environment["AGENT_UPDATE_LOG"] = str(update_log)
            if list_failure:
                environment["AGENT_LIST_FAILURE"] = "1"
            result = subprocess.run(
                ["sh", "-c", command, "--"],
                capture_output=True,
                text=True,
                check=False,
                env=environment,
            )
            return (
                result,
                update_log.read_text(encoding="utf-8"),
                list_count.read_text(encoding="utf-8"),
            )

    # QA: UP-04; installer polling against fake CLI replies, not atomic admission.
    # Rationale: active work must finish before the helper requests Agent replacement.
    def test_busy_agent_is_not_updated_until_authoritatively_idle(self) -> None:
        result, updates, list_count = self.run_dispatch_fixture(
            [
                '{"items":[{"id":"agt_1","in_flight":1}]}',
                '{"items":[{"id":"agt_1","in_flight":0}]}',
            ]
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(updates.splitlines(), ["update"])
        self.assertEqual(list_count.strip(), "2")

    # QA: UP-04; polling-count bound with sleeps removed, not elapsed-time proof.
    # Rationale: persistent work cannot cause endless polling or forced replacement.
    def test_persistent_busy_agent_exhausts_bound_without_update(self) -> None:
        result, updates, list_count = self.run_dispatch_fixture(
            ['{"items":[{"id":"agt_1","in_flight":1}]}']
        )

        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertEqual(updates, "")
        self.assertEqual(list_count.strip(), "330")
        self.assertIn("remained busy for 330 seconds", result.stderr)

    # QA: UP-04; local preflight parsing, not Controller availability.
    # Rationale: malformed observation is not evidence that the Agent is idle.
    def test_malformed_agent_list_fails_closed_without_update(self) -> None:
        result, updates, list_count = self.run_dispatch_fixture(["not json"])

        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertEqual(updates, "")
        self.assertEqual(list_count.strip(), "1")

    # QA: UP-04; local CLI-error handling, not a live disconnection.
    # Rationale: a failed read must stop preflight before requesting replacement.
    def test_agent_list_read_failure_fails_closed_without_update(self) -> None:
        result, updates, list_count = self.run_dispatch_fixture(
            ['{"items":[{"id":"agt_1","in_flight":0}]}'],
            list_failure=True,
        )

        self.assertNotEqual(result.returncode, 0, result.stderr)
        self.assertEqual(updates, "")
        self.assertEqual(list_count.strip(), "0")


if __name__ == "__main__":
    unittest.main()
