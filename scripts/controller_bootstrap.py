#!/usr/bin/env python3
"""Private fixed-path bootstrap checks; not an alternative update mechanism."""
import argparse
from contextlib import closing
import os
from pathlib import Path
import stat
import sys
from urllib.parse import urlencode

from controller_release import DIRECTORY_FLAGS, MAX_BINARY, RELEASE_ROOT, ReleaseStore, directory, regular
from controller_update import TERMINAL, Transport

GUARD = Path("/usr/local/libexec/groundplane/controller-recovery")
UNIT = Path("/etc/systemd/system/groundplane-controller.service")
GUARD_LINE = "ExecStartPre=/usr/local/libexec/groundplane/controller-recovery --upgrade-guard"


def require_idle(transport):
    cursor = ""
    seen = set()
    for _ in range(100):
        status, page = transport.request("GET", "/tasks?" + urlencode({"limit": 100, "cursor": cursor}))
        if status != 200 or not isinstance(page, dict) or not isinstance(page.get("items"), list):
            raise ValueError("cannot prove bootstrap host is idle")
        for task in page["items"]:
            if not isinstance(task, dict) or task.get("status") not in TERMINAL - {"rejected"}:
                raise ValueError("bootstrap requires all Tasks terminal; wait without aborting active work")
        cursor = page.get("next_cursor", "")
        if not isinstance(cursor, str) or len(cursor) > 8192 or cursor in seen:
            raise ValueError("invalid bootstrap Task page cursor")
        if not cursor:
            return
        seen.add(cursor)
    raise ValueError("bootstrap idle check exceeded bounded Task history")


class Layout:
    def __init__(self, root: Path, guard: Path, unit: Path, uid: int):
        self.root, self.guard, self.unit, self.uid = root, guard, unit, uid

    @staticmethod
    def exists(path: Path) -> bool:
        try:
            path.lstat()
            return True
        except FileNotFoundError:
            return False

    def read_leaf(self, path: Path, limit: int) -> bytes:
        parent = directory(path.parent, self.uid)
        try:
            with regular(parent, path.name, self.uid, limit) as source:
                return source.read(limit + 1)
        finally:
            os.close(parent)

    def mode(self) -> str:
        has_root, has_guard = self.exists(self.root), self.exists(self.guard)
        has_unit_guard = False
        if self.exists(self.unit):
            unit = self.read_leaf(self.unit, 65536).decode("utf-8")
            has_unit_guard = GUARD_LINE in unit.splitlines()
        if not has_root and not has_guard and not has_unit_guard:
            return "bootstrap"
        if has_root:
            with closing(ReleaseStore(self.root, self.uid)):
                pass
        if not has_root or not has_guard or not has_unit_guard:
            raise ValueError("incomplete native installation; repair its recovery guard before deploying")
        parent = directory(self.guard.parent, self.uid)
        try:
            with regular(parent, self.guard.name, self.uid, MAX_BINARY) as guard:
                mode = os.fstat(guard.fileno()).st_mode
                if not mode & stat.S_IXUSR or mode & 0o222:
                    raise ValueError("native recovery guard must be immutable and executable")
        finally:
            os.close(parent)
        return "native"

    def initialize(self):
        # The sole missing parent permitted is the fixed Groundplane data root.
        ancestor = directory(self.root.parent.parent, self.uid)
        try:
            try:
                os.mkdir(self.root.parent.name, 0o700, dir_fd=ancestor)
                os.fsync(ancestor)
            except FileExistsError:
                pass
        finally:
            os.close(ancestor)
        parent = directory(self.root.parent, self.uid)
        try:
            os.mkdir(self.root.name, 0o700, dir_fd=parent)
            root = os.open(self.root.name, DIRECTORY_FLAGS, dir_fd=parent)
            try:
                os.mkdir("releases", 0o700, dir_fd=root)
                os.fsync(root)
            finally:
                os.close(root)
            os.fsync(parent)
        finally:
            os.close(parent)

    def remove_empty(self):
        # Called only for this deployment's newly created layout, after stopping
        # Controller. Never recursively remove native release or journal data.
        with closing(ReleaseStore(self.root, self.uid)) as store:
            with store.locked():
                if set(os.listdir(store.root)) != {"releases", "journal.lock"} or os.listdir(store.releases):
                    raise ValueError("native recovery evidence exists; retaining installation and backups")
                os.rmdir("releases", dir_fd=store.root)
                os.unlink("journal.lock", dir_fd=store.root)
                os.fsync(store.root)
                parent = directory(self.root.parent, self.uid)
                try:
                    os.rmdir(self.root.name, dir_fd=parent)
                    os.fsync(parent)
                finally:
                    os.close(parent)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("mode", "initialize", "remove-empty", "require-idle"))
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise ValueError("native bootstrap requires root")
    layout = Layout(RELEASE_ROOT, GUARD, UNIT, 0)
    if args.action == "mode":
        print(layout.mode())
    elif args.action == "initialize":
        layout.initialize()
    elif args.action == "remove-empty":
        layout.remove_empty()
    else:
        require_idle(Transport())


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"controller-bootstrap: {error}", file=sys.stderr)
        sys.exit(1)
