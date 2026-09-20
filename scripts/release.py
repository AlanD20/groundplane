#!/usr/bin/env python3
"""Build Agent, Controller bundle, or both; publishing GitHub releases is separate."""
import argparse
from pathlib import Path
import re
import subprocess

from release_selection import SEMVER


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    flags = parser.add_mutually_exclusive_group()
    flags.add_argument("--agent-only", action="store_true")
    flags.add_argument("--controller-only", action="store_true")
    parser.add_argument("--image", help="Agent repository:tag for the native build")
    parser.add_argument("--runner-image", help="published Runner repository@sha256:digest for bundles")
    parser.add_argument("--push", action="store_true", help="push the built Agent image")
    parser.add_argument("--output", default=".tmp/releases")
    args = parser.parse_args()
    if not re.fullmatch(SEMVER, args.version):
        parser.error("use stable X.Y.Z; the component namespace is selected by the flags")
    if not args.controller_only and not args.image:
        parser.error("Agent builds require --image REPOSITORY:TAG")
    if not args.agent_only and not args.runner_image:
        parser.error("Controller bundles require a published --runner-image digest")
    if not args.agent_only and not args.controller_only and not args.push:
        parser.error("a combined bundle requires --push to pin its built Agent's published digest")
    root = Path(__file__).resolve().parents[1]
    image = None
    if not args.controller_only:
        command = ["bash", "scripts/release-agent.sh", "--version", f"agent/v{args.version}", "--image", args.image]
        if args.push:
            command.append("--push")
        image = subprocess.check_output(command, cwd=root, text=True).strip()
        if args.push:
            subprocess.run(["python3", "scripts/release_agent_bundle.py", "--version", args.version,
                            "--image", image, "--output", args.output], cwd=root, check=True)
    if not args.agent_only:
        command = ["python3", "scripts/release_bundle.py", "--version", args.version,
                   "--runner-image", args.runner_image, "--output", args.output]
        command += ["--controller-only"] if args.controller_only else ["--agent-image", image]
        subprocess.run(command, cwd=root, check=True)


if __name__ == "__main__":
    main()
