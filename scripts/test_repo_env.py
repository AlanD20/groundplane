from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
VERIFY = Path(".agents/skills/verify-groundplane/scripts")
PATH_VARS = ("TMPDIR", "GOTMPDIR", "GOCACHE", "GROUNDPLANE_VERIFY_RUNTIME_DIR", "GROUNDPLANE_EVIDENCE_DIR")
PROBE = "import json,os,tempfile; print(json.dumps({**{k:os.environ[k] for k in ('TMPDIR','GOTMPDIR','GOCACHE','GROUNDPLANE_COVERAGE_FILE')},'actual':tempfile.mkdtemp()}))"
FAKE_GO = '''#!/usr/bin/env python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['GP_TEST_RECORD'], 'a') as out:
    out.write(json.dumps({'args':args,'env':{k:os.environ.get(k) for k in ('TMPDIR','GOTMPDIR','GOCACHE')}})+'\\n')
if os.environ.get('GP_TEST_FAIL'):
    sys.exit(7)
if args == ['env', 'GOOS']:
    print('linux')
if args[:2] == ['version', '-m']:
    print('build -tags=groundplane_console')
if '-c' in args and '-o' in args:
    output = pathlib.Path(args[args.index('-o')+1])
    output.write_text('#!/bin/sh\\nexit 0\\n')
    output.chmod(0o700)
if '-list' in args:
    print('TestFixture')
if '-output' in args:
    pathlib.Path(args[args.index('-output')+1]).write_text('{}')
for arg in args:
    if arg.startswith('-coverprofile='):
        pathlib.Path(arg.split('=',1)[1]).write_text('mode: atomic\\n')
'''


class RepoEnvironmentTest(unittest.TestCase):
    def setUp(self) -> None:
        (ROOT / ".tmp").mkdir(exist_ok=True)
        self.scratch = tempfile.TemporaryDirectory(prefix="tooling-test-", dir=ROOT / ".tmp")
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name) / "repo"
        (self.root / "scripts").mkdir(parents=True)
        (self.root / VERIFY).mkdir(parents=True)
        shutil.copy2(ROOT / "Makefile", self.root)
        shutil.copy2(ROOT / "scripts/repo-env.sh", self.root / "scripts")
        for source in (ROOT / VERIFY).glob("*.sh"):
            shutil.copy2(source, self.root / VERIFY)
        for name in ("component-sdk", "registered-components", "commands"):
            (self.root / name).mkdir()
        (self.root / "openapi.json").write_text("{}")
        self.env = os.environ.copy()
        for name in (*PATH_VARS, "GROUNDPLANE_CLI_PATH", "MAKEFLAGS", "MFLAGS", "MAKELEVEL"):
            self.env.pop(name, None)
        self.env["PATH"] = str(self.root / "commands") + os.pathsep + self.env["PATH"]
        self.env["GP_TEST_RECORD"] = str(self.root / "commands.jsonl")
        self.stub("go", FAKE_GO)
        self.stub("ssh", "#!/bin/sh\nexit 7\n")
        self.stub("ssh-keyscan", "#!/bin/sh\nexit 7\n")

    def stub(self, name: str, body: str) -> None:
        file = self.root / "commands" / name
        file.write_text(body)
        file.chmod(0o700)

    def run_command(self, *args: str, ok: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run(args, cwd=self.root, env=self.env, capture_output=True, text=True, timeout=20)
        if ok:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def probe(self) -> dict:
        return json.loads(self.run_command("bash", "scripts/repo-env.sh", "python3", "-c", PROBE).stdout)

    def records(self) -> list[dict]:
        return [json.loads(line) for line in Path(self.env["GP_TEST_RECORD"]).read_text().splitlines()]

    def assert_local_records(self) -> None:
        for record in self.records():
            for value in record["env"].values():
                self.assertIsNotNone(value)
                self.assertTrue(Path(value).is_relative_to(self.root / ".tmp"), value)

    def test_unset_defaults_create_paths_before_child_starts(self) -> None:
        result = self.probe()
        for name, relative in (("TMPDIR", "tmp"), ("GOTMPDIR", "go-tmp"), ("GOCACHE", "go-cache")):
            expected = self.root / ".tmp" / relative
            self.assertEqual(result[name], str(expected))
            self.assertTrue(expected.is_dir())
        self.assertEqual(Path(result["actual"]).parent, self.root / ".tmp/tmp")
        self.assertEqual(result["GROUNDPLANE_COVERAGE_FILE"], str(self.root / ".tmp/coverage.out"))

    def test_custom_local_paths_preserve_existing_cache(self) -> None:
        for name in ("TMPDIR", "GOTMPDIR", "GOCACHE"):
            self.env[name] = ".tmp-custom/" + name.lower() + " with space"
        marker = self.root / self.env["GOCACHE"] / "keep"
        marker.parent.mkdir(parents=True)
        marker.write_text("existing cache")
        for _ in range(2):
            result = self.probe()
            self.assertEqual(result["GOCACHE"], str(marker.parent))
        self.assertEqual(marker.read_text(), "existing cache")

    def test_unsafe_overrides_fail_before_child_runs(self) -> None:
        outside = Path(self.scratch.name) / "outside"
        for name in ("TMPDIR", "GOTMPDIR", "GOCACHE"):
            for path in (str(outside), ".tmp/../escape", ".tmp/./dirty", ".tmp//dirty", ".tmp/dirty\nname"):
                with self.subTest(name=name, path=path):
                    self.env[name] = path
                    self.run_command("bash", "scripts/repo-env.sh", "touch", "child-ran", ok=False)
                    self.assertFalse((self.root / "child-ran").exists())
                    self.assertFalse(outside.exists())
            del self.env[name]

    def test_symlink_ancestors_and_files_are_rejected(self) -> None:
        outside = Path(self.scratch.name) / "outside"
        outside.mkdir()
        (self.root / ".tmp").mkdir()
        (self.root / ".tmp/link").symlink_to(outside, target_is_directory=True)
        (self.root / ".tmp/file").write_text("keep")
        for path in (".tmp/link", ".tmp/link/child", ".tmp/file/child"):
            with self.subTest(path=path):
                self.env["TMPDIR"] = path
                self.run_command("bash", "scripts/repo-env.sh", "true", ok=False)
        self.assertEqual(list(outside.iterdir()), [])
        self.assertEqual((self.root / ".tmp/file").read_text(), "keep")

    def test_coverage_parent_and_leaf_cannot_escape(self) -> None:
        outside = Path(self.scratch.name) / "outside"
        outside.mkdir()
        (self.root / ".tmp").symlink_to(outside, target_is_directory=True)
        for name in ("TMPDIR", "GOTMPDIR", "GOCACHE"):
            self.env[name] = ".tmp-custom/" + name.lower()
        self.run_command("bash", "scripts/repo-env.sh", "true", ok=False)
        (self.root / ".tmp").unlink()
        (self.root / ".tmp").mkdir()
        (self.root / ".tmp/coverage.out").symlink_to(outside / "coverage")
        self.run_command("bash", "scripts/repo-env.sh", "true", ok=False)
        self.assertFalse((outside / "coverage").exists())

    def test_make_test_and_recursive_make_receive_local_paths(self) -> None:
        self.run_command("make", "test")
        self.assert_local_records()
        self.assertGreater(len(self.records()), 1)
        self.assertTrue((self.root / ".tmp/coverage.out").is_file())
        self.assertFalse((self.root / "coverage.out").exists())

    # Rationale: ignored caches can contain invalid Go fixtures. Formatting must
    # ignore them while rejecting malformed or unformatted repository source.
    def test_format_check_uses_source_roots_and_preserves_formatter_failures(self) -> None:
        for name in ("cmd", "component-sdk", "console", "internal", "pkg", "proto", "registered-components"):
            (self.root / name).mkdir(exist_ok=True)
        cache = self.root / ".tmp/dependency-cache"
        cache.mkdir(parents=True)
        invalid_fixture = cache / "fixture.go"
        invalid_fixture.write_text("not valid Go\n")
        source = self.root / "cmd/main.go"
        source.write_text("package main\n\nfunc main() {}\n")
        self.run_command("make", "format-check")
        source.write_text("package main\nfunc main(){ }\n")
        result = self.run_command("make", "format-check", ok=False)
        self.assertIn("cmd/main.go", result.stdout)
        source.write_text("not valid Go\n")
        result = self.run_command("make", "format-check", ok=False)
        self.assertIn("cmd/main.go", result.stderr)
        self.assertEqual(invalid_fixture.read_text(), "not valid Go\n")
        source.write_text("package main\n\nfunc main() {}\n")
        self.env["GP_TEST_FAIL"] = "1"
        self.run_command("make", "format-check", ok=False)

    def test_sudo_recipe_preserves_paths_after_environment_reset(self) -> None:
        self.stub("id", "#!/bin/sh\nprintf '1000\\n'\n")
        self.stub("sudo", '#!/bin/sh\nshift\nexec env -i "PATH=$PATH" "GP_TEST_RECORD=$GP_TEST_RECORD" "$@"\n')
        self.run_command("make", "backupstage-host-acceptance")
        self.assert_local_records()
        privileged = next(r for r in self.records() if "backupstage_mount_acceptance" in r["args"])
        self.assertEqual(privileged["env"]["GOCACHE"], str(self.root / ".tmp/go-cache-root"))

    def test_release_smoke_binary_uses_owned_temporary_directory(self) -> None:
        (self.root / "bin").mkdir()
        controller = self.root / "bin/controller"
        controller.write_text("#!/bin/sh\nexit 0\n")
        controller.chmod(0o700)
        assets = self.root / "console/dist/assets"
        assets.mkdir(parents=True)
        (assets / "fixture-01234567.js").write_text("fixture")
        (assets.parent / "index.html").write_text("fixture")
        self.run_command("make", "console-release-smoke")
        build = next(r["args"] for r in self.records() if "-c" in r["args"])
        output = Path(build[build.index("-o") + 1])
        self.assertTrue(output.is_relative_to(self.root / ".tmp/tmp"))
        self.assertFalse(output.parent.exists())
        self.assertEqual(list((self.root / "bin").iterdir()), [controller])

    def run_verifier(self, name: str, ok: bool = True) -> subprocess.CompletedProcess:
        self.env.update(GROUNDPLANE_ETCD_ENDPOINT="http://example.invalid:2379",
                        GROUNDPLANE_C07_ETCD_PREFIX="/groundplane-c07-acceptance/run-fixture/")
        return self.run_command("bash", str(VERIFY / name), ok=ok)

    def test_local_verifiers_cleanup_only_their_runtime_and_retain_evidence(self) -> None:
        for name in ("network-zone-route-etcd.sh", "c11-attach-l2.sh"):
            with self.subTest(name=name):
                runtime = self.root / ".tmp/runtime"
                runtime.mkdir(parents=True, exist_ok=True)
                marker = runtime / "keep"
                marker.write_text("pre-existing")
                self.env["GROUNDPLANE_VERIFY_RUNTIME_DIR"] = str(runtime)
                self.run_verifier(name)
                self.assertEqual(list(runtime.iterdir()), [marker])
                self.assertEqual(marker.read_text(), "pre-existing")
        self.assert_local_records()
        receipts = list((self.root / ".tmp/verify-groundplane").glob("*/result.txt"))
        self.assertEqual(len(receipts), 2)
        for receipt in receipts:
            self.assertIn("runtime_cleanup=passed", receipt.read_text())

    def test_failed_verifier_retains_evidence_and_cleans_runtime(self) -> None:
        self.env["GP_TEST_FAIL"] = "1"
        self.run_verifier("network-zone-route-etcd.sh", ok=False)
        receipt = next((self.root / ".tmp/verify-groundplane").glob("*/result.txt"))
        self.assertIn("exit_status=7", receipt.read_text())
        self.assertIn("runtime_cleanup=passed", receipt.read_text())
        self.assertEqual(list((self.root / ".tmp/tmp").iterdir()), [])

    def test_verifier_rejects_outside_evidence_and_runtime(self) -> None:
        for name in ("GROUNDPLANE_EVIDENCE_DIR", "GROUNDPLANE_VERIFY_RUNTIME_DIR"):
            with self.subTest(name=name):
                self.env[name] = str(Path(self.scratch.name) / "outside")
                self.run_verifier("network-zone-route-etcd.sh", ok=False)
                self.assertFalse(Path(self.env[name]).exists())
                self.assertFalse(Path(self.env["GP_TEST_RECORD"]).exists())
                del self.env[name]

    def test_remote_verifiers_cleanup_on_failed_preflight_without_host_calls(self) -> None:
        key = self.root / "test-key"
        key.write_text("fixture")
        known_hosts = self.root / "known-hosts"
        known_hosts.write_text("example.test ssh-ed25519 AAAA\n")
        self.env.update(GROUNDPLANE_SSH_TARGET="root@example.test", GROUNDPLANE_SSH_KEY=str(key),
                        GROUNDPLANE_SSH_KNOWN_HOSTS=str(known_hosts),
                        GROUNDPLANE_TENANT_ID="ten_01J00000000000000000000000",
                        GROUNDPLANE_PROJECT_ID="prj_01J00000000000000000000000",
                        GROUNDPLANE_ENVIRONMENT_ID="env_01J00000000000000000000000",
                        GROUNDPLANE_VOLUME_SLUG="fixture", GROUNDPLANE_VOLUME_RENAMED_SLUG="renamed")
        for name in ("host-health-ssh.sh", "volume-lifecycle-ssh.sh"):
            with self.subTest(name=name):
                self.run_verifier(name, ok=False)
        receipts = list((self.root / ".tmp/verify-groundplane").glob("*/result.txt"))
        self.assertEqual(len(receipts), 2)
        for receipt in receipts:
            self.assertIn("runtime_cleanup=passed", receipt.read_text())
            self.assertIn("service_mutation=none", receipt.read_text())
        self.assertEqual(list((self.root / ".tmp/tmp").iterdir()), [])
