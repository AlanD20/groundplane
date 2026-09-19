"""Release Agent image command tests with Docker and Make kept out of process."""

import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPOSITORY_ROOT / "scripts" / "release-agent.sh"
IMAGE = "registry.example.test/groundplane-agent:1.2.3"
DIGEST = "sha256:" + "a" * 64
OLD_DIGEST = "sha256:" + "b" * 64


class ReleaseAgentTest(unittest.TestCase):
    def setUp(self):
        (REPOSITORY_ROOT / ".tmp").mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(
            prefix="release-agent-test-", dir=REPOSITORY_ROOT / ".tmp",
        )
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.bin = self.directory / "bin"
        self.bin.mkdir()
        self.docker_log = self.directory / "docker.log"
        self.make_log = self.directory / "make.log"
        self._write_fake_tools()

    def _write_fake_tools(self):
        make = self.bin / "make"
        make.write_text(textwrap.dedent("""\
            #!/bin/sh
            printf '%s\\n' "$*" >> "$FAKE_MAKE_LOG"
        """))
        make.chmod(0o755)

        docker = self.bin / "docker"
        docker.write_text(textwrap.dedent("""\
            #!/usr/bin/env python3
            import os
            import sys

            arguments = sys.argv[1:]
            with open(os.environ["FAKE_DOCKER_LOG"], "a", encoding="utf-8") as log:
                log.write(" ".join(arguments) + "\\n")

            if arguments[:2] == ["image", "inspect"]:
                image_format = arguments[arguments.index("--format") + 1]
                if image_format == "{{json .Config.Entrypoint}}":
                    print('["/usr/local/bin/groundplane-agent"]')
                elif image_format == "{{json .Config.Cmd}}":
                    print("null")
                elif image_format == "{{.Config.User}}":
                    print("0:0")
                elif "RepoDigests" in image_format:
                    print(os.environ["FAKE_REPO_DIGESTS"])
                else:
                    raise SystemExit("unexpected image inspect format")
            elif arguments[0] == "run" and arguments[-1] == "--version":
                print("Docker version 29.1.3, build release-test")
            elif arguments[0] == "run" and arguments[-3:] == ["compose", "version", "--short"]:
                print("2.40.3")
            elif arguments[0] == "run" and arguments[-1] == "compose-helper":
                print("agent compose helper: validation.failed: incomplete input", file=sys.stderr)
                raise SystemExit(1)
            elif arguments[0] == "run" and "config" in arguments:
                sys.stdin.read()
            elif arguments[0] == "push":
                print(f"{arguments[1]}: digest: {os.environ['FAKE_PUSH_DIGEST']} size: 1234")
            else:
                raise SystemExit("unexpected docker invocation: " + " ".join(arguments))
        """))
        docker.chmod(0o755)

    def run_release(self, *, push, repo_digests=None):
        environment = os.environ.copy()
        environment.update({
            "PATH": f"{self.bin}:{environment['PATH']}",
            "FAKE_DOCKER_LOG": str(self.docker_log),
            "FAKE_MAKE_LOG": str(self.make_log),
            "FAKE_PUSH_DIGEST": DIGEST,
            "FAKE_REPO_DIGESTS": repo_digests or "",
        })
        command = [
            "bash", str(SCRIPT), "--version", "1.2.3", "--image", IMAGE,
        ]
        if push:
            command.append("--push")
        return subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )

    # Release delivery: publication identity, not registry transport behavior.
    # Rationale: Docker's tagged push line must return the newly reported RepoDigest.
    def test_push_returns_digest_from_realistic_output_after_repo_digest_confirmation(self):
        published = f"registry.example.test/groundplane-agent@{DIGEST}"
        result = self.run_release(
            push=True,
            repo_digests=f"registry.example.test/groundplane-agent@{OLD_DIGEST}\n{published}",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, published + "\n")

    # Release delivery: post-push identity confirmation, not registry availability.
    # Rationale: an unrelated RepoDigest already attached to the image cannot be returned.
    def test_push_rejects_unconfirmed_new_digest_even_when_an_old_repo_digest_exists(self):
        result = self.run_release(
            push=True,
            repo_digests=f"registry.example.test/groundplane-agent@{OLD_DIGEST}",
        )
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertIn("did not record the pushed RepoDigest", result.stderr)

    # Release delivery: explicit publication boundary, not image build behavior.
    # Rationale: the default command must build and smoke-test without publishing.
    def test_default_does_not_push_or_return_a_local_identity(self):
        result = self.run_release(push=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertNotIn("push ", self.docker_log.read_text())


if __name__ == "__main__":
    unittest.main()
