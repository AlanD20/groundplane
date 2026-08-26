#!/usr/bin/env python3
"""Own one SSH tunnel child until an explicit stop request or deadline."""

from __future__ import annotations

import argparse
import ctypes
import errno
import os
import resource
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MAX_RECEIPT_BYTES = 8192
MAX_CHILD_OUTPUT_BYTES = 2 * 1024 * 1024
SHUTDOWN_GRACE_SECONDS = 5.0
stop_requested = False


class ControlProofError(RuntimeError):
    """The owned child did not produce a provable SSH control master."""


def _signal_handler(signum: int, _frame: object) -> None:
    del signum
    global stop_requested
    stop_requested = True


def _receipt_value(value: object) -> str:
    text = str(value)
    if "\n" in text or "\r" in text:
        raise ValueError("receipt values must be single-line")
    return text


def write_receipt(path: str | os.PathLike[str], **fields: object) -> None:
    lines = [f"{key}={_receipt_value(fields[key])}" for key in fields]
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    if len(payload) > MAX_RECEIPT_BYTES:
        raise ValueError("receipt exceeds the size bound")

    destination = Path(path)
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        offset = 0
        while offset < len(payload):
            written = os.write(fd, payload[offset:])
            if written <= 0:
                raise OSError(errno.EIO, "receipt write made no progress")
            offset += written
        os.fsync(fd)
        os.close(fd)
        fd = -1
        os.replace(temporary_path, destination)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        try:
            temporary_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _prepare_child(supervisor_pid: int) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    resource.setrlimit(
        resource.RLIMIT_FSIZE,
        (MAX_CHILD_OUTPUT_BYTES, MAX_CHILD_OUTPUT_BYTES),
    )
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
        error_number = ctypes.get_errno() or errno.EIO
        raise OSError(error_number, "prctl(PR_SET_PDEATHSIG) failed")
    if os.getppid() != supervisor_pid:
        os._exit(143)


def _stop_child(child: subprocess.Popen[bytes]) -> tuple[bool, int | None, str]:
    terminate_error: OSError | None = None
    try:
        child.terminate()
    except OSError as error:
        terminate_error = error

    try:
        return True, child.wait(timeout=SHUTDOWN_GRACE_SECONDS), (
            "terminate-error" if terminate_error else "terminate"
        )
    except subprocess.TimeoutExpired:
        pass
    except ChildProcessError:
        return False, None, "wait-error"

    kill_error: OSError | None = None
    try:
        child.kill()
    except OSError as error:
        kill_error = error

    try:
        return True, child.wait(timeout=SHUTDOWN_GRACE_SECONDS), (
            "kill-error" if kill_error else "kill"
        )
    except subprocess.TimeoutExpired:
        return False, None, "kill-timeout"
    except ChildProcessError:
        return False, None, "wait-error"


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    try:
        separator = argv.index("--")
    except ValueError as error:
        raise SystemExit("supervisor command must be separated by --") from error

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stop-file", required=True)
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--stopped-file", required=True)
    parser.add_argument("--failure-file", required=True)
    parser.add_argument("--child-output", required=True)
    parser.add_argument("--deadline-seconds", type=float, required=True)
    parser.add_argument("--control-socket", required=True)
    parser.add_argument("--control-target", required=True)
    parser.add_argument("--control-check-program", default="ssh")
    parser.add_argument("--control-check-option", action="append", default=[])
    parser.add_argument("--control-check-timeout", type=float, default=10.0)
    parser.add_argument("--control-ready-timeout", type=float, default=30.0)
    args = parser.parse_args(argv[:separator])
    command = argv[separator + 1 :]
    if not command:
        parser.error("a child command is required")
    if not 0 < args.deadline_seconds <= 3600:
        parser.error("--deadline-seconds must be between 0 and 3600")
    if not 0 < args.control_check_timeout <= 60:
        parser.error("--control-check-timeout must be between 0 and 60")
    if not 0 < args.control_ready_timeout <= args.deadline_seconds:
        parser.error(
            "--control-ready-timeout must be between 0 and --deadline-seconds"
        )
    return args, command


def _stop_exists(path: str) -> bool:
    return os.path.exists(path)


def _wait_for_control_proof(
    child: subprocess.Popen[bytes],
    args: argparse.Namespace,
    deadline: float,
) -> tuple[bool, str]:
    proof_deadline = min(
        deadline, time.monotonic() + args.control_ready_timeout
    )
    check_command = [
        args.control_check_program,
        *args.control_check_option,
        "-S",
        args.control_socket,
        "-O",
        "check",
        "--",
        args.control_target,
    ]
    last_error = "control proof failed"
    while True:
        if stop_requested or _stop_exists(args.stop_file):
            return False, "stop-file"
        if child.poll() is not None:
            raise ControlProofError("child exited before control proof")
        remaining = proof_deadline - time.monotonic()
        if remaining <= 0:
            raise ControlProofError(last_error)
        try:
            check = subprocess.run(
                check_command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=min(args.control_check_timeout, remaining),
                check=False,
            )
        except subprocess.TimeoutExpired:
            last_error = "control proof timed out"
        except (OSError, subprocess.SubprocessError) as error:
            raise ControlProofError(f"control proof could not run: {error}") from error
        else:
            if child.poll() is not None:
                raise ControlProofError("child exited during control proof")
            if stop_requested or _stop_exists(args.stop_file):
                return False, "stop-file"
            if check.returncode == 0:
                return True, "control-proof"
            last_error = f"control proof exited {check.returncode}"
        sleep_remaining = proof_deadline - time.monotonic()
        if sleep_remaining <= 0:
            raise ControlProofError(last_error)
        time.sleep(min(0.05, sleep_remaining))


def run(argv: list[str]) -> int:
    args, command = _parse_args(argv)
    signal.signal(signal.SIGHUP, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    deadline = time.monotonic() + args.deadline_seconds
    if stop_requested or _stop_exists(args.stop_file):
        write_receipt(
            args.stopped_file,
            reason="prelaunch-stop",
            child_started=0,
        )
        return 0

    output = None
    child: subprocess.Popen[bytes] | None = None
    try:
        output = open(args.child_output, "wb", buffering=0)
        supervisor_pid = os.getpid()
        child = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            preexec_fn=lambda: _prepare_child(supervisor_pid),
        )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        if output is not None:
            output.close()
        write_receipt(args.failure_file, reason="startup", error=error)
        return 1

    outcome = 0
    reason = "stop-file"
    failure_reason: str | None = None
    failure_error: object | None = None
    stopped = False
    returncode: int | None = None
    action = "not-stopped"
    try:
        proof_succeeded, proof_reason = _wait_for_control_proof(
            child, args, deadline
        )
        if proof_succeeded:
            write_receipt(
                args.ready_file,
                child_started=1,
                pid=child.pid,
                control_proof=proof_reason,
            )
        else:
            reason = proof_reason
        while True:
            if stop_requested:
                reason = "signal"
                break
            if _stop_exists(args.stop_file):
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                reason = "deadline"
                failure_reason = reason
                outcome = 1
                break
            time.sleep(min(0.05, remaining))
    except ControlProofError as error:
        reason = "control-proof"
        failure_reason = "control-proof"
        failure_error = error
        outcome = 1
    except BaseException as error:
        failure_reason = "supervisor-exception"
        failure_error = error
        outcome = 1
    finally:
        try:
            stopped, returncode, action = _stop_child(child)
        except BaseException as error:
            stopped = False
            failure_reason = failure_reason or "child-shutdown-exception"
            failure_error = error
            outcome = 1
        if not stopped:
            failure_reason = failure_reason or "child-shutdown-timeout"
            outcome = 1
        try:
            if output is not None:
                output.close()
        except BaseException as error:
            failure_reason = failure_reason or "child-output-close"
            failure_error = error
            outcome = 1

        if stopped:
            try:
                write_receipt(
                    args.stopped_file,
                    action=action,
                    child_started=1,
                    reason=reason,
                    returncode=returncode,
                )
            except BaseException as error:
                failure_reason = failure_reason or "stopped-receipt"
                failure_error = error
                outcome = 1
        if failure_reason is not None:
            fields: dict[str, object] = {"reason": failure_reason}
            if failure_error is not None:
                fields["error"] = failure_error
            try:
                write_receipt(args.failure_file, **fields)
            except BaseException:
                outcome = 1

    return outcome


def main() -> int:
    try:
        return run(sys.argv[1:])
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
