#!/usr/bin/env python3
"""Report strict architecture findings; defer only the owner-approved 0.0.1 debt."""

import collections
import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parent.parent
DEFERRED = ROOT / "architecture-deferred.json"
SIZE_RULES = {"frozen-total-drift", "missing-oversized-baseline", "oversized-file-growth"}


def identities(findings):
    if not isinstance(findings, list):
        raise ValueError("architecture findings must be a JSON array")
    result = collections.Counter()
    for finding in findings:
        if not isinstance(finding, dict):
            raise ValueError("invalid architecture finding")
        key = tuple(finding.get(field, "") for field in ("path", "rule", "subject", "message"))
        if not all(isinstance(value, str) for value in key) or not all(key[i] for i in (0, 1, 3)):
            raise ValueError("incomplete architecture finding")
        result[key] += 1
    return result


def check_report(returncode, output, deferred):
    findings = json.loads(output)
    # The Go command encodes a nil findings slice as null on success.
    if findings is None and returncode == 0:
        findings = []
    current = identities(findings)
    if returncode != (1 if current else 0):
        raise ValueError("architecture checker did not complete normally")
    allowed = identities(deferred)
    for path, rule, _, _ in allowed:
        if rule not in SIZE_RULES and not (rule == "layer-import" and path.endswith("_test.go")):
            raise ValueError("deferral contains a non-structural or production import finding")
    return current - allowed, sum(current.values())


def main():
    result = subprocess.run(
        ["bash", "scripts/repo-env.sh", "go", "run", "./cmd/architecture-check",
         "-root", ".", "-baseline", "architecture-baseline.json"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    # Retain the full strict report in CI output, including deferred findings.
    print(result.stdout, end="")
    print(result.stderr, end="", file=sys.stderr)
    try:
        deferred = json.loads(DEFERRED.read_text())
        unexpected, count = check_report(result.returncode, result.stdout, deferred)
    except (OSError, ValueError) as error:
        print(f"Architecture release gate failed: {error}", file=sys.stderr)
        return 1
    if unexpected:
        print("Architecture release gate: new or changed findings (BLOCKING):", file=sys.stderr)
        for finding, occurrences in unexpected.items():
            print(f"{finding} ({occurrences})", file=sys.stderr)
        return 1
    print(f"Architecture release gate: {count} existing findings DEFERRED for 0.0.1; "
          "strict architecture compliance is NOT qualified.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
