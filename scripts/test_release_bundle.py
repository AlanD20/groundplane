"""PKG-01/02: artifact identity, unsafe archives, and native-update isolation.

These host-free checks prevent execution of wrong/tampered downloads and prevent
the installer from treating an upgrade as a fresh installation. They do not
qualify a real-host install or uninterrupted application traffic.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

import install_bundle
import release_bundle


class BundleTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.agent = "ghcr.io/test/agent@sha256:" + "a" * 64
        self.runner = "ghcr.io/test/runner@sha256:" + "b" * 64
        self.sources = self.root / "sources"
        self.sources.mkdir()
        binary = b"test controller bytes"
        metadata = {"schema": 1, "controller_sha256": "sha256:" + hashlib.sha256(binary).hexdigest(),
                    "controller_version": "1.2.3", "storage_epoch": 1, "channel_schema": 1}
        for name, data in (("controller", binary), ("groundplane", b"test CLI"),
                           ("controller-release.json", json.dumps(metadata).encode())):
            (self.sources / name).write_bytes(data)
        for constant, name in (("CONTROLLER", "controller"), ("CLI", "groundplane"),
                               ("CONTROLLER_METADATA", "controller-release.json")):
            patcher = mock.patch.object(release_bundle.deploy, constant, self.sources / name)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.files = release_bundle.payload("1.2.3", self.agent, self.runner, "amd64")
        script = (release_bundle.ROOT / "install.sh").read_text()
        self.extractor = script.split("python3 -c '\n", 1)[1].split("\n' \"$deploy_dir\"", 1)[0]

    def extract(self, files=None, extra=None, version="1.2.3", arch="amd64"):
        target = self.root / "install"
        target.mkdir(exist_ok=True)
        archive = target / "bundle.tar.gz"
        with tarfile.open(archive, "w:gz") as out:
            for name, data in (files or self.files).items():
                entry = tarfile.TarInfo(name)
                entry.size = len(data)
                out.addfile(entry, io.BytesIO(data))
            if extra is not None:
                out.addfile(extra, io.BytesIO(b"x" * extra.size))
        result = subprocess.run(["python3", "-c", self.extractor, str(target), version, arch],
                                capture_output=True, text=True)
        return target, result

    def test_roundtrip_preserves_pinned_images_and_controller_bytes(self):
        target, result = self.extract()
        self.assertEqual(result.returncode, 0, result.stderr)
        manifest = install_bundle.validate(target, "1.2.3", "amd64")
        self.assertEqual(manifest["agent_image"], self.agent)
        self.assertEqual(manifest["runner_image"], self.runner)
        self.assertEqual((target / "controller").read_bytes(), b"test controller bytes")

    def test_wrong_version_and_architecture_refuse_before_extraction(self):
        for version, arch in (("9.0.0", "amd64"), ("1.2.3", "arm64")):
            with self.subTest(version=version, arch=arch):
                target, result = self.extract(version=version, arch=arch)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((target / "install_bundle.py").exists())

    def test_unsafe_archive_paths_links_duplicates_and_missing_members_refuse(self):
        for name, kind in (("../escape", tarfile.REGTYPE), ("controller", tarfile.REGTYPE),
                           ("controller", tarfile.SYMTYPE), ("controller", tarfile.LNKTYPE)):
            with self.subTest(name=name, kind=kind):
                entry = tarfile.TarInfo(name)
                entry.type = kind
                entry.linkname = "/etc/passwd"
                target, result = self.extract(extra=entry)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((target / "install_bundle.py").exists())
        files = dict(self.files)
        del files["controller"]
        _, result = self.extract(files)
        self.assertNotEqual(result.returncode, 0)

    def test_tampered_member_and_controller_descriptor_are_rejected(self):
        files = dict(self.files, controller=b"tampered")
        _, result = self.extract(files)
        self.assertNotEqual(result.returncode, 0)
        (self.sources / "controller").write_bytes(b"wrong binary")
        with self.assertRaisesRegex(ValueError, "bytes differ"):
            release_bundle.payload("1.2.3", self.agent, self.runner, "amd64")

    def test_archive_is_repeatable_and_refuses_overwrite(self):
        first, second = self.root / "a.tar.gz", self.root / "b.tar.gz"
        release_bundle.write_archive(first, self.files)
        release_bundle.write_archive(second, self.files)
        self.assertEqual(first.read_bytes(), second.read_bytes())
        with self.assertRaises(FileExistsError):
            release_bundle.write_archive(first, self.files)

    def invoke(self, mode, stage=False, config=None):
        args = argparse.Namespace(version="1.2.3", stage_only=stage, config=config, listen_ip="127.0.0.1")
        layout = mock.Mock()
        layout.mode.return_value = mode
        manifest = {"agent_image": self.agent, "runner_image": self.runner}
        with mock.patch.object(install_bundle.subprocess, "run") as run, \
                mock.patch.object(install_bundle.os, "execvp") as execute, \
                mock.patch.object(install_bundle.shutil, "disk_usage") as capacity:
            capacity.return_value.free = 3 * 1024**3
            install_bundle.install(self.root, args, manifest, layout)
        return run, execute

    def test_native_update_only_pulls_agent_and_uses_guarded_activation(self):
        for stage in (False, True):
            run, execute = self.invoke("native", stage)
            run.assert_called_once_with(["docker", "pull", self.agent], check=True)
            argv = execute.call_args.args[1]
            self.assertEqual(argv[1], str(self.root / "install-runtime.sh"))
            self.assertEqual(argv[-2:], ["1" if stage else "0", "0"])

    def test_fresh_install_provisions_and_pulls_both_images(self):
        with mock.patch.object(install_bundle.Path, "exists", return_value=False):
            run, _ = self.invoke("bootstrap")
        self.assertEqual(run.call_args_list, [
            mock.call(["sh", str(self.root / "setup-host.sh")], check=True),
            mock.call(["docker", "pull", self.agent], check=True),
            mock.call(["docker", "pull", self.runner], check=True)])

    def test_stage_on_fresh_host_and_config_on_native_refuse(self):
        with self.assertRaisesRegex(ValueError, "existing native"):
            self.invoke("bootstrap", stage=True)
        with self.assertRaisesRegex(ValueError, "initial-install only"):
            self.invoke("native", config="unused.yaml")

    def test_partial_layout_and_low_capacity_have_no_setup_or_pull_effects(self):
        args = argparse.Namespace(version="1.2.3", stage_only=False, config=None, listen_ip="127.0.0.1")
        layout = mock.Mock()
        manifest = {"agent_image": self.agent, "runner_image": self.runner}
        with mock.patch.object(install_bundle.subprocess, "run") as run, \
                mock.patch.object(install_bundle.os, "execvp") as execute, \
                mock.patch.object(install_bundle.shutil, "disk_usage") as capacity:
            layout.mode.side_effect = ValueError("incomplete native installation")
            with self.assertRaisesRegex(ValueError, "incomplete native"):
                install_bundle.install(self.root, args, manifest, layout)
            layout.mode.side_effect = None
            layout.mode.return_value = "native"
            capacity.return_value.free = 1024
            with self.assertRaisesRegex(ValueError, "2 GiB"):
                install_bundle.install(self.root, args, manifest, layout)
            run.assert_not_called()
            execute.assert_not_called()

    def test_help_and_invalid_version_do_not_require_root_or_host_setup(self):
        script = str(release_bundle.ROOT / "install.sh")
        for arguments, expected in ((["--help"], 0), (["--version", "latest"], 2),
                                    (["--version", "1.2.3", "--bundle", "missing"], 2)):
            result = subprocess.run(["sh", script, *arguments], capture_output=True, text=True)
            self.assertEqual(result.returncode, expected, result.stderr)


if __name__ == "__main__":
    unittest.main()
