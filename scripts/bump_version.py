#!/usr/bin/env python3
"""Prepare a GP release version and changelog; never commit, tag, push or publish."""

import argparse
import difflib
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parent.parent
FILES = ("VERSION", "console/package.json", "console/package-lock.json", "CHANGELOG.md")
SEMVER = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"


def version_tuple(value):
    if not re.fullmatch(SEMVER, value):
        raise ValueError("use a stable MAJOR.MINOR.PATCH version without a v prefix")
    return tuple(map(int, value.split(".")))


def sections(changelog):
    headings = list(re.finditer(r"^## (.+)$", changelog, re.MULTILINE))
    result = {}
    for index, heading in enumerate(headings):
        title = heading[1]
        if title == "[Unreleased]":
            key = "Unreleased"
        else:
            match = re.fullmatch(rf"({SEMVER}) — (?:pending publication|[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})", title)
            if not match:
                raise ValueError(f"unrecognized changelog heading: {title}")
            key = match[1]
        if key in result:
            raise ValueError(f"duplicate changelog section: {key}")
        end = headings[index + 1].start() if index + 1 < len(headings) else len(changelog)
        result[key] = (heading.start(), heading.end(), end)
    if not headings or headings[0][1] != "[Unreleased]":
        raise ValueError("changelog must start with one ## [Unreleased] section")
    return result


def read_state(root):
    for name in FILES:
        if any(part.is_symlink() for part in (root / name, *(root / name).parents)):
            raise ValueError(f"refusing symlinked release metadata: {name}")
    originals = {name: (root / name).read_text() for name in FILES}
    current = originals["VERSION"].strip()
    version_tuple(current)
    package = json.loads(originals["console/package.json"])
    lock = json.loads(originals["console/package-lock.json"])
    if any(value != current for value in (package["version"], lock["version"], lock["packages"][""]["version"])):
        raise ValueError("VERSION and Console package/lock versions disagree")
    entries = sections(originals["CHANGELOG.md"])
    if current not in entries:
        raise ValueError("current VERSION has no changelog section")
    return originals, current, package, lock, entries


def release_notes(root, expected=None):
    originals, current, _, _, entries = read_state(root)
    if expected is not None and expected != current:
        raise ValueError(f"release tag/version {expected} does not match VERSION {current}")
    _, start, end = entries[current]
    notes = originals["CHANGELOG.md"][start:end].strip()
    if not any(line.strip() and not line.startswith("#") for line in notes.splitlines()):
        raise ValueError("release changelog contains no feature or change notes")
    return notes.replace("(docs/", f"(https://github.com/AlanD20/groundplane/blob/v{current}/docs/") + "\n"


def prepare(root, new_version):
    new = version_tuple(new_version)
    originals, current, package, lock, entries = read_state(root)
    if new <= version_tuple(current) or new_version in entries:
        raise ValueError("new version must be greater than current and absent from changelog")
    package["version"] = lock["version"] = lock["packages"][""]["version"] = new_version
    begin, start, end = entries["Unreleased"]
    changelog = originals["CHANGELOG.md"]
    notes = changelog[start:end].strip() or "### Added\n\n### Changed\n\n### Fixed"
    updated = dict(originals)
    updated["VERSION"] = new_version + "\n"
    updated["console/package.json"] = json.dumps(package, indent=2, ensure_ascii=False) + "\n"
    updated["console/package-lock.json"] = json.dumps(lock, indent=2, ensure_ascii=False) + "\n"
    updated["CHANGELOG.md"] = (changelog[:begin] + "## [Unreleased]\n\n"
                               + f"## {new_version} — pending publication\n\n{notes}\n\n" + changelog[end:])
    return originals, updated


def main(argv=None, root=ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", nargs="?")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--check", action="store_true", help="validate metadata and release notes; optionally match a tag version")
    mode.add_argument("--notes", action="store_true", help="print only current release notes for GitHub publication")
    args = parser.parse_args(argv)
    try:
        if args.check or args.notes:
            notes = release_notes(root, args.version)
            print(notes if args.notes else "Release metadata and changelog match.", end="" if args.notes else "\n")
            return 0
        if not args.version:
            raise ValueError("version is required for a bump")
        originals, updated = prepare(root, args.version)
        if args.dry_run:
            for name in FILES:
                print("".join(difflib.unified_diff(originals[name].splitlines(True), updated[name].splitlines(True),
                                                  fromfile=name, tofile=name)), end="")
            return 0
        written = []
        try:
            for name in FILES:
                written.append(name)
                (root / name).write_text(updated[name])
        except OSError:
            for name in written:
                (root / name).write_text(originals[name])
            raise
        print(f"Prepared {args.version}. Review CHANGELOG.md, run CI, then commit and tag explicitly.")
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(f"Version preparation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
