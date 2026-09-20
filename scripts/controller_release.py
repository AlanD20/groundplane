#!/usr/bin/env python3
"""Private deployment tooling: publish immutable native release bytes, never run them."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import stat
import sys

RELEASE_ROOT = Path("/var/lib/groundplane/controller-updates")
MAX_BINARY = 256 << 20
MAX_METADATA = 4096
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
BUILD_FIELDS = {"schema", "controller_sha256", "controller_version", "storage_epoch", "channel_schema"}
DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
FILE_FLAGS = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK


def directory(path: Path, uid: int, *, private: bool = False) -> int:
    if not path.is_absolute() or str(path) != os.path.normpath(path):
        raise ValueError("release path must be absolute and clean")
    current = os.open("/", DIRECTORY_FLAGS)
    try:
        trusted_directory(current, uid)
        for part in path.parts[1:]:
            child = os.open(part, DIRECTORY_FLAGS, dir_fd=current)
            os.close(current)
            current = child
            trusted_directory(current, uid)
        if private:
            private_directory(current, uid)
        return current
    except BaseException:
        os.close(current)
        raise


def trusted_directory(fd: int, uid: int) -> None:
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in (0, uid) or info.st_mode & 0o022:
        raise ValueError("release directory has unsafe ownership or permissions")


def private_directory(fd: int, uid: int) -> None:
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != uid or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("release directory must be private and owner-controlled")


@contextmanager
def regular(parent: int, name: str, uid: int, maximum: int):
    fd = os.open(name, FILE_FLAGS, dir_fd=parent)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != uid or info.st_nlink != 1
                or info.st_mode & 0o022 or info.st_size <= 0 or info.st_size > maximum):
            raise ValueError("release input has unsafe type, ownership, size or permissions")
        with os.fdopen(fd, "rb", closefd=False) as source:
            yield source
    finally:
        os.close(fd)


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("release metadata contains a duplicate field")
        result[key] = value
    return result


def canonical(value: dict) -> bytes:
    # These closed records contain only ASCII identifiers and bounded integers;
    # sorted compact JSON is byte-identical to the Controller's JCS encoding.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def build_metadata(raw: bytes) -> dict:
    metadata = json.loads(raw, object_pairs_hook=unique_object)
    if not isinstance(metadata, dict) or set(metadata) != BUILD_FIELDS:
        raise ValueError("release build metadata fields are invalid")
    for key in ("schema", "storage_epoch", "channel_schema"):
        if type(metadata[key]) is not int or not 1 <= metadata[key] <= 2147483647:
            raise ValueError("release compatibility metadata is invalid")
    if not isinstance(metadata["controller_sha256"], str) or not DIGEST.fullmatch(metadata["controller_sha256"]):
        raise ValueError("release Controller digest is invalid")
    version = metadata["controller_version"]
    if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9._+/-]{1,128}", version):
        raise ValueError("release Controller version is invalid")
    return metadata


def build_manifest(raw: bytes, agent_image: str) -> dict:
    metadata = build_metadata(raw)
    if len(agent_image) > 1024 or not re.fullmatch(r"[a-z0-9][a-z0-9._:/\[\]-]*@sha256:[0-9a-f]{64}", agent_image):
        raise ValueError("release Agent image must be digest-pinned")
    return {**metadata, "agent_image": agent_image}


def digest_stream(source, output=None) -> str:
    digest = hashlib.sha256()
    total = 0
    while chunk := source.read(32768):
        total += len(chunk)
        if total > MAX_BINARY:
            raise ValueError("release Controller binary exceeds size limit")
        digest.update(chunk)
        if output is not None:
            output.write(chunk)
    if total == 0:
        raise ValueError("release Controller binary is empty")
    return "sha256:" + digest.hexdigest()


class ReleaseStore:
    def __init__(self, root: Path, uid: int):
        self.uid = uid
        self.root = directory(root, uid, private=True)
        try:
            self.releases = os.open("releases", DIRECTORY_FLAGS, dir_fd=self.root)
            private_directory(self.releases, uid)
        except BaseException:
            if hasattr(self, "releases"):
                os.close(self.releases)
            os.close(self.root)
            raise

    def close(self):
        os.close(self.releases)
        os.close(self.root)

    @contextmanager
    def locked(self):
        fd = os.open("journal.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
                     0o600, dir_fd=self.root)
        try:
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != self.uid or info.st_nlink != 1
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise ValueError("release journal lock is unsafe")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)

    def stage(self, bundle: Path, agent_image: str) -> str:
        source = directory(bundle, self.uid, private=True)
        try:
            with regular(source, "controller-release.json", self.uid, MAX_METADATA) as metadata:
                manifest = build_manifest(metadata.read(MAX_METADATA + 1), agent_image)
            raw = canonical(manifest)
            if len(raw) > MAX_METADATA:
                raise ValueError("release manifest exceeds size limit")
            identity = "sha256:" + hashlib.sha256(raw).hexdigest()
            with regular(source, "controller", self.uid, MAX_BINARY) as binary:
                with self.locked():
                    self.assert_selector()
                    # Verify source on replay too: a stale descriptor must never
                    # make changed transfer bytes look like the selected release.
                    if digest_stream(binary) != manifest["controller_sha256"]:
                        raise ValueError("release Controller digest does not match build metadata")
                    binary.seek(0)
                    self.publish(identity, raw, binary, manifest["controller_sha256"])
                    self.select(identity)
            return identity
        finally:
            os.close(source)

    def assert_selector(self):
        try:
            info = os.stat("candidate.json", dir_fd=self.root, follow_symlinks=False)
        except FileNotFoundError:
            return
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != self.uid or info.st_nlink != 1
                or info.st_mode & 0o022):
            raise ValueError("release candidate selector is unsafe")

    def publish(self, identity: str, raw: bytes, binary, expected: str):
        leaf = identity[7:]
        try:
            existing = os.open(leaf, DIRECTORY_FLAGS, dir_fd=self.releases)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            try:
                private_directory(existing, self.uid)
                with regular(existing, "manifest.json", self.uid, MAX_METADATA) as manifest:
                    if manifest.read(MAX_METADATA + 1) != raw:
                        raise ValueError("published release manifest was modified")
                with regular(existing, "controller", self.uid, MAX_BINARY) as installed:
                    if digest_stream(installed) != expected:
                        raise ValueError("published release binary was modified")
                return
            finally:
                os.close(existing)
        temporary = ".staging-" + secrets.token_hex(16)
        os.mkdir(temporary, 0o700, dir_fd=self.releases)
        staging = os.open(temporary, DIRECTORY_FLAGS, dir_fd=self.releases)
        published = False
        try:
            self.write(staging, "manifest.json", 0o400, lambda output: output.write(raw))

            def copy(output):
                if digest_stream(binary, output) != expected:
                    raise ValueError("release Controller digest changed during staging")
            self.write(staging, "controller", 0o500, copy)
            os.fsync(staging)
            os.rename(temporary, leaf, src_dir_fd=self.releases, dst_dir_fd=self.releases)
            published = True
            os.fsync(self.releases)
        finally:
            if not published:
                for name in ("controller", "manifest.json"):
                    try:
                        os.unlink(name, dir_fd=staging)
                    except FileNotFoundError:
                        pass
                os.rmdir(temporary, dir_fd=self.releases)
            os.close(staging)

    @staticmethod
    def write(parent: int, name: str, mode: int, write):
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                     0o600, dir_fd=parent)
        with os.fdopen(fd, "wb") as output:
            write(output)
            output.flush()
            os.fchmod(output.fileno(), mode)
            os.fsync(output.fileno())

    def select(self, identity: str):
        temporary = ".candidate-" + secrets.token_hex(16)
        try:
            self.write(self.root, temporary, 0o400, lambda output: output.write(canonical({"release": identity})))
            self.assert_selector()
            os.replace(temporary, "candidate.json", src_dir_fd=self.root, dst_dir_fd=self.root)
            os.fsync(self.root)
        finally:
            try:
                os.unlink(temporary, dir_fd=self.root)
            except FileNotFoundError:
                pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("agent_image")
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError("native release staging requires root")
    store = ReleaseStore(RELEASE_ROOT, 0)
    try:
        print(store.stage(args.bundle, args.agent_image))
    finally:
        store.close()


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"controller-release: {error}", file=sys.stderr)
        sys.exit(1)
