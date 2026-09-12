#!/usr/bin/env python3

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
SUPERVISOR_PATH = SCRIPT_DIR / "ssh_tunnel_supervisor.py"
spec = importlib.util.spec_from_file_location("ssh_tunnel_supervisor", SUPERVISOR_PATH)
assert spec is not None and spec.loader is not None
supervisor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(supervisor)


class SupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.stop = self.root / "stop"
        self.ready = self.root / "ready"
        self.stopped = self.root / "stopped"
        self.failure = self.root / "failure"
        self.output = self.root / "child-output"
        self.control_socket = self.root / "control.socket"
        self.control_target = "fake-target"
        self.check_args = self.root / "check-args"
        self.check_started = self.root / "check-started"
        self.success_check = self.root / "check-success.py"
        self.blocking_check = self.root / "check-blocking.py"
        self.failure_check = self.root / "check-failure.py"
        self.success_check.write_text(
            "#!/usr/bin/env python3\n"
            "import pathlib, sys\n"
            f"pathlib.Path({str(self.check_args)!r}).write_text('\\n'.join(sys.argv[1:]))\n"
        )
        self.blocking_check.write_text(
            "#!/usr/bin/env python3\n"
            "import pathlib, time\n"
            f"pathlib.Path({str(self.check_started)!r}).touch()\n"
            "time.sleep(60)\n"
        )
        self.failure_check.write_text(
            "#!/usr/bin/env python3\n"
            "raise SystemExit(7)\n"
        )
        for path in (self.success_check, self.blocking_check, self.failure_check):
            path.chmod(0o700)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def command(
        self,
        deadline: float = 5.0,
        child_code: str | None = None,
        ready: Path | None = None,
        check_program: Path | None = None,
        check_timeout: float = 0.2,
        ready_timeout: float = 1.0,
    ) -> list[str]:
        child_code = child_code or "import time; time.sleep(60)"
        return [
            sys.executable,
            str(SUPERVISOR_PATH),
            "--stop-file",
            str(self.stop),
            "--ready-file",
            str(ready or self.ready),
            "--stopped-file",
            str(self.stopped),
            "--failure-file",
            str(self.failure),
            "--child-output",
            str(self.output),
            "--deadline-seconds",
            str(deadline),
            "--control-socket",
            str(self.control_socket),
            "--control-target",
            self.control_target,
            "--control-check-program",
            str(check_program or self.success_check),
            "--control-check-timeout",
            str(check_timeout),
            "--control-ready-timeout",
            str(ready_timeout),
            "--control-check-option=-F",
            "--control-check-option=/dev/null",
            "--",
            sys.executable,
            "-c",
            child_code,
        ]

    def wait_for(self, path: Path) -> None:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if path.exists():
                return
            time.sleep(0.01)
        self.fail(f"timed out waiting for {path}")

    def assert_receipts_bounded(self) -> None:
        for path in (self.ready, self.stopped, self.failure):
            if path.exists():
                self.assertLessEqual(path.stat().st_size, supervisor.MAX_RECEIPT_BYTES)

    def wait_for_child_exit(self, pid_file: Path) -> None:
        child_pid = int(pid_file.read_text())
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.01)
        self.fail(f"child {child_pid} did not exit")

    def test_startup_failure_writes_failure_receipt(self) -> None:
        command = self.command()
        separator = command.index("--")
        command[separator + 1 :] = ["/definitely/missing-child"]
        result = subprocess.run(command, capture_output=True, text=True, timeout=3)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("reason=startup", self.failure.read_text())
        self.assert_receipts_bounded()

    def test_preexisting_stop_does_not_start_child(self) -> None:
        self.stop.write_text("stop\n")
        result = subprocess.run(self.command(), capture_output=True, text=True, timeout=3)
        self.assertEqual(result.returncode, 0)
        self.assertFalse(self.ready.exists())
        self.assertIn("reason=prelaunch-stop", self.stopped.read_text())
        self.assert_receipts_bounded()

    def test_normal_stop_terminates_child_and_writes_receipts(self) -> None:
        process = subprocess.Popen(
            self.command(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try:
            self.wait_for(self.ready)
            self.stop.write_text("stop\n")
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
        self.assertIn("reason=stop-file", self.stopped.read_text())
        self.assert_receipts_bounded()

    def test_ready_is_absent_until_control_proof(self) -> None:
        process = subprocess.Popen(
            self.command(
                check_program=self.blocking_check,
                check_timeout=0.1,
                ready_timeout=2.0,
            ),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            self.wait_for(self.check_started)
            self.assertFalse(self.ready.exists())
            self.stop.write_text("stop\n")
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
        self.assertFalse(self.ready.exists())

    def test_early_child_exit_fails_before_ready(self) -> None:
        result = subprocess.run(
            self.command(child_code="import sys; sys.exit(7)"),
            capture_output=True,
            text=True,
            timeout=3,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.ready.exists())
        self.assertIn("reason=control-proof", self.failure.read_text())

    def test_normal_control_proof_emits_ready(self) -> None:
        process = subprocess.Popen(
            self.command(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        try:
            self.wait_for(self.ready)
            check_args = self.check_args.read_text().splitlines()
            socket_index = check_args.index("-S")
            self.assertEqual(check_args[socket_index + 1], str(self.control_socket))
            self.assertEqual(
                check_args[socket_index + 2 :],
                ["-O", "check", "--", self.control_target],
            )
            self.stop.write_text("stop\n")
            self.assertEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_deadline_writes_failure_and_stops_child(self) -> None:
        result = subprocess.run(
            # Allow Python control-proof startup before testing the later lifetime deadline.
            self.command(deadline=1.0, ready_timeout=0.75, check_timeout=0.5),
            capture_output=True,
            text=True,
            timeout=3,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(self.ready.exists())
        self.assertIn("reason=deadline", self.failure.read_text())
        self.assertIn("reason=deadline", self.stopped.read_text())
        self.assert_receipts_bounded()

    def test_supervisor_death_stops_ssh_child_with_pdeathsig(self) -> None:
        child_pid_file = self.root / "child.pid"
        child_code = (
            "import os, pathlib, time; "
            f"pathlib.Path({str(child_pid_file)!r}).write_text(str(os.getpid())); "
            "time.sleep(60)"
        )
        process = subprocess.Popen(
            self.command(child_code=child_code),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            self.wait_for(self.ready)
            self.wait_for(child_pid_file)
            process.kill()
            self.assertNotEqual(process.wait(timeout=3), 0)
            self.wait_for_child_exit(child_pid_file)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    def test_ready_receipt_failure_still_tears_down_child(self) -> None:
        ready_directory = self.root / "ready-directory"
        ready_directory.mkdir()
        child_code = "import time; time.sleep(60)"
        process = subprocess.Popen(
            self.command(child_code=child_code, ready=ready_directory),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            self.assertNotEqual(process.wait(timeout=3), 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
        self.assertIn("reason=supervisor-exception", self.failure.read_text())
        self.assertIn("action=terminate", self.stopped.read_text())

    def test_term_to_kill_escalation(self) -> None:
        class UncooperativeChild:
            def __init__(self) -> None:
                self.terminated = False
                self.killed = False

            def terminate(self) -> None:
                self.terminated = True

            def kill(self) -> None:
                self.killed = True

            def wait(self, timeout: float) -> int:
                del timeout
                if self.killed:
                    return -9
                raise subprocess.TimeoutExpired(cmd="fake-child", timeout=1)

        child = UncooperativeChild()
        stopped, returncode, action = supervisor._stop_child(child)  # type: ignore[arg-type]
        self.assertTrue(stopped)
        self.assertEqual(returncode, -9)
        self.assertEqual(action, "kill")
        self.assertTrue(child.terminated)
        self.assertTrue(child.killed)

    def test_receipt_writer_rejects_unbounded_values(self) -> None:
        with self.assertRaises(ValueError):
            supervisor.write_receipt(
                self.root / "receipt",
                value="x" * supervisor.MAX_RECEIPT_BYTES,
            )


if __name__ == "__main__":
    unittest.main()
