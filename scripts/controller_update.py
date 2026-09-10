#!/usr/bin/env python3
"""Private deployment client for the normal protected native Controller update API."""
import argparse
import fcntl
import http.client
import json
import os
import re
import secrets
import stat
import sys
import time

from controller_release import DIGEST, RELEASE_ROOT, ReleaseStore, canonical, directory, regular, unique_object

TASK_ID = re.compile(r"task_[0-7][0-9A-HJKMNP-TV-Z]{25}")
KEY = re.compile(r"[A-Za-z0-9._:-]{16,128}")
TERMINAL = {"completed", "failed", "aborted", "timed_out", "rejected"}
DEFINITIVE = {400, 401, 403, 404, 405, 409, 422}


class Receipt:
    def __init__(self, root, uid):
        self.uid = uid
        self.root = directory(root, uid, private=True)
        try:
            self.lock = os.open("deployment.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
                                0o600, dir_fd=self.root)
            info = os.fstat(self.lock)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_nlink != 1
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise ValueError("deployment receipt lock is unsafe")
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            if hasattr(self, "lock"):
                os.close(self.lock)
            os.close(self.root)
            raise

    def close(self):
        os.close(self.lock)
        os.close(self.root)

    def read(self):
        try:
            with regular(self.root, "deployment.json", self.uid, 4096) as source:
                value = json.loads(source.read(4097), object_pairs_hook=unique_object)
        except FileNotFoundError:
            return None
        if (not isinstance(value, dict) or set(value) != {"release", "key", "task_id", "status"}
                or not isinstance(value["release"], str) or not DIGEST.fullmatch(value["release"])
                or not isinstance(value["key"], str) or not KEY.fullmatch(value["key"])
                or not isinstance(value["task_id"], str)
                or (value["task_id"] and not TASK_ID.fullmatch(value["task_id"]))
                or not isinstance(value["status"], str) or value["status"] not in TERMINAL | {"pending", "running"}):
            raise ValueError("deployment receipt is invalid; preserve it for recovery")
        return value

    def write(self, value):
        self.read()  # Refuse unsafe/malformed existing records, never repair by overwriting.
        name = ".deployment-" + secrets.token_hex(16)
        try:
            ReleaseStore.write(self.root, name, 0o600, lambda output: output.write(canonical(value)))
            os.replace(name, "deployment.json", src_dir_fd=self.root, dst_dir_fd=self.root)
            os.fsync(self.root)
        finally:
            try:
                os.unlink(name, dir_fd=self.root)
            except FileNotFoundError:
                pass

    def begin(self, release, key):
        if not DIGEST.fullmatch(release) or not KEY.fullmatch(key):
            raise ValueError("deployment release or protected key is invalid")
        current = self.read()
        if current is not None and current["status"] not in TERMINAL:
            if current["release"] != release:
                raise ValueError("another release has unresolved acceptance; resume the retained deployment first")
            return current
        value = {"release": release, "key": key, "task_id": "", "status": "pending"}
        self.write(value)
        return value


class Transport:
    def request(self, method, path, body=None, key=None):
        # Fixed loopback destination. HTTPConnection does not follow redirects.
        connection = http.client.HTTPConnection("127.0.0.1", 8080, timeout=10)
        try:
            headers = {"Accept": "application/json"}
            if key is not None:
                headers["Idempotency-Key"] = key
                headers["Content-Type"] = "application/json"
            connection.request(method, "/api/v1" + path, body=canonical(body) if body is not None else None,
                               headers=headers)
            response = connection.getresponse()
            raw = response.read((1 << 20) + 1)
            if len(raw) > 1 << 20:
                raise ValueError("Controller response exceeds deployment limit")
            if response.status not in (200, 202):
                return response.status, None
            return response.status, json.loads(raw, object_pairs_hook=unique_object)
        finally:
            connection.close()


class Client:
    def __init__(self, receipt, transport, now=time.monotonic, sleep=time.sleep):
        self.receipt, self.transport, self.now, self.sleep = receipt, transport, now, sleep

    def run(self, release, key, timeout):
        value = self.receipt.begin(release, key)
        return self.follow(value, timeout)

    def follow(self, value, timeout):
        deadline = self.now() + timeout
        while self.now() < deadline:
            try:
                status = self.step(value)
                if status is not None:
                    return status
            except (OSError, http.client.HTTPException):
                pass  # Acceptance is uncertain: retain exact key/body or known Task.
            self.sleep(2)
        print("Controller update outcome unresolved; retained deployment receipt. Resume before a new release.",
              file=sys.stderr)
        return 2

    def step(self, value):
        task_id = value["task_id"]
        if not task_id:
            status, result = self.transport.request("POST", "/controller/update",
                                                    {"release": value["release"]}, value["key"])
            if status in DEFINITIVE:
                value["status"] = "rejected"
                self.receipt.write(value)
                print(f"Controller update rejected (HTTP {status}); no binary rollback attempted.", file=sys.stderr)
                return 1
            if status != 202:
                return None
            if not isinstance(result, dict) or not isinstance(result.get("task_id"), str):
                raise ValueError("Controller acceptance has no Task identity; receipt retained")
            task_id = result["task_id"]
            if not TASK_ID.fullmatch(task_id):
                raise ValueError("Controller acceptance has invalid Task identity; receipt retained")
            value["task_id"] = task_id
            self.receipt.write(value)
            print(f"Controller update Task: {task_id}", flush=True)
            return None
        status, task = self.transport.request("GET", "/tasks/" + task_id)
        if status != 200:
            return None
        if (not isinstance(task, dict) or task.get("id") != task_id
                or task.get("type") != "update" or task.get("target") != "controller"):
            raise ValueError("Controller Task identity mismatch; receipt retained")
        state = task.get("status")
        if not isinstance(state, str) or state not in {"pending", "running"} | (TERMINAL - {"rejected"}):
            raise ValueError("Controller Task has invalid status; receipt retained")
        value["status"] = state
        self.receipt.write(value)
        if state in TERMINAL:
            print(f"Controller update Task {task_id}: {state}", flush=True)
            return 0 if state == "completed" else 1
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("release", nargs="?")
    parser.add_argument("key", nargs="?")
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError("deployment requires root")
    if args.resume == bool(args.release or args.key) or (not args.resume and not args.key):
        parser.error("supply release and key, or --resume alone")
    receipt = Receipt(RELEASE_ROOT, 0)
    try:
        client = Client(receipt, Transport())
        if args.resume:
            value = receipt.read()
            if value is None:
                raise ValueError("no deployment receipt to resume")
            if value["status"] in TERMINAL:
                print(f"Retained deployment: {value['task_id']} {value['status']}")
                return 0 if value["status"] == "completed" else 1
            return client.follow(value, 720)
        return client.run(args.release, args.key, 720)
    finally:
        receipt.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, http.client.HTTPException) as error:
        print(f"controller-update: {error}; deployment receipt retained", file=sys.stderr)
        sys.exit(2)
