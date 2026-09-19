#!/usr/bin/env python3
"""Distribution provisioning checks using a mocked package manager and runtime.

These host-free delivery tests prove OS admission, distro-owned package selection,
and the no-replacement boundary. They do not qualify an installed host or a
distribution's packages at runtime.
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("deploy.py")
SPEC = importlib.util.spec_from_file_location("deploy_distros", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load {SCRIPT}")
deploy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = deploy
SPEC.loader.exec_module(deploy)

REPOSITORY_ROOT = SCRIPT.parents[1]
TEST_TEMPORARY_ROOT = REPOSITORY_ROOT / ".tmp" / "test-deploy-distros"
INSTALLER = REPOSITORY_ROOT / "install.sh"


class ProvisioningHarness:
    def __init__(self, root: Path, distribution: str, version: str) -> None:
        self.root = root
        self.bin = root / "bin"
        self.bin.mkdir(parents=True)
        self.log = root / "effects.log"
        self.active = root / "docker-active"
        self.unit = root / "docker-unit"
        self.registry = root / "registry"
        self.os_release = root / "os-release"
        self.os_release.write_text(
            f"ID={distribution}\nVERSION_ID={version}\n",
            encoding="utf-8",
        )
        self.environment = {
            **os.environ,
            "PATH": str(self.bin),
            "MOCK_ACTIVE": str(self.active),
            "MOCK_BIN": str(self.bin),
            "MOCK_DOCKER_PACKAGES": "0",
            "MOCK_EFFECTS": str(self.log),
            "MOCK_REGISTRY": str(self.registry),
            "MOCK_UNIT": str(self.unit),
        }
        self._write_mocks()

    def _command(self, name: str, body: str) -> None:
        target = self.bin / name
        target.write_text("#!/bin/sh\nset -eu\n" + body, encoding="utf-8")
        target.chmod(0o755)

    def _write_mocks(self) -> None:
        (self.bin / "grep").symlink_to("/bin/grep")
        self._command("python3", "exit 0\n")
        self._command("id", "test \"${1:-}\" = -u && printf '0\\n'\n")
        self._command(
            "dpkg-query",
            "case \"$*\" in\n"
            "  *ca-certificates*) printf 'install ok installed\\n'; exit 0 ;;\n"
            "esac\n"
            "test \"$MOCK_DOCKER_PACKAGES\" = 1 || exit 1\n"
            "printf 'install ok installed\\n'\n",
        )
        self._command(
            "apt-get",
            "printf 'apt-get %s\\n' \"$*\" >>\"$MOCK_EFFECTS\"\n"
            "if test \"${1:-}\" = install; then\n"
            "  /bin/ln -sf \"$MOCK_BIN/mock-docker\" \"$MOCK_BIN/docker\"\n"
            "  : >\"$MOCK_ACTIVE\"\n"
            "fi\n",
        )
        self._command(
            "systemctl",
            "printf 'systemctl %s\\n' \"$*\" >>\"$MOCK_EFFECTS\"\n"
            "case \"${1:-}\" in\n"
            "  cat) test -e \"$MOCK_UNIT\" ;;\n"
            "  is-active) test -e \"$MOCK_ACTIVE\" ;;\n"
            "  start) : >\"$MOCK_ACTIVE\" ;;\n"
            "esac\n",
        )
        self._command(
            "mock-docker",
            "printf 'docker %s\\n' \"$*\" >>\"$MOCK_EFFECTS\"\n"
            "case \"${1:-}\" in\n"
            "  compose) test \"${MOCK_COMPOSE:-1}\" = 1 && printf 'Docker Compose version mock\\n' ;;\n"
            "  version) exit 0 ;;\n"
            "  --version) printf 'Docker version mock\\n' ;;\n"
            "  container)\n"
            "    test -e \"$MOCK_REGISTRY\" || exit 1\n"
            "    case \"$*\" in\n"
            "      *Config.Image*) printf '%s\\n' 'registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373' ;;\n"
            "      *State.Running*) printf 'true\\n' ;;\n"
            "    esac ;;\n"
            "  run) : >\"$MOCK_REGISTRY\" ;;\n"
            "  start) : >\"$MOCK_REGISTRY\" ;;\n"
            "esac\n",
        )
        self._command(
            "curl",
            "test -e \"$MOCK_REGISTRY\"\n",
        )
        for name in (
            "age-keygen",
            "fuse-overlayfs",
            "newuidmap",
            "nft",
            "rootlesskit",
            "slirp4netns",
            "socat",
        ):
            self._command(name, "exit 0\n")

    def install_docker(self, *, compose: bool = True) -> None:
        (self.bin / "docker").symlink_to(self.bin / "mock-docker")
        self.unit.touch()
        self.active.touch()
        self.environment["MOCK_DOCKER_PACKAGES"] = "1"
        self.environment["MOCK_COMPOSE"] = "1" if compose else "0"

    def healthy_registry(self) -> None:
        self.registry.touch()

    def run(self) -> subprocess.CompletedProcess[str]:
        script = deploy.REMOTE_SETUP.replace(
            ". /etc/os-release",
            f'. "{self.os_release}"',
        )
        return subprocess.run(
            ["/bin/sh", "-c", script],
            capture_output=True,
            text=True,
            check=False,
            env=self.environment,
        )

    def effects(self) -> list[str]:
        if not self.log.exists():
            return []
        return self.log.read_text(encoding="utf-8").splitlines()


def installer_os_guard(os_release: Path) -> str:
    script = INSTALLER.read_text(encoding="utf-8")
    start = script.index("# shellcheck source=/dev/null")
    end = script.index('case "$(uname -m)"')
    return script[start:end].replace(
        ". /etc/os-release",
        f'. "{os_release}"',
    )


class DeployDistributionTest(unittest.TestCase):
    def setUp(self) -> None:
        TEST_TEMPORARY_ROOT.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=TEST_TEMPORARY_ROOT)
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    # Delivery: mocked apt selection, not proof that distribution repositories work.
    # Rationale: supported releases intentionally use their native binary package names.
    def test_fresh_host_selects_distribution_docker_and_compose_packages(self) -> None:
        cases = (
            ("ubuntu", "24.04", "docker.io docker-compose-v2"),
            ("ubuntu", "26.04", "docker.io docker-compose-v2"),
            ("debian", "13", "docker.io docker-cli docker-compose"),
        )
        for distribution, version, packages in cases:
            with self.subTest(distribution=distribution, version=version):
                root = self.root / f"{distribution}-{version}"
                root.mkdir()
                harness = ProvisioningHarness(root, distribution, version)

                result = harness.run()

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(f"apt-get install -y {packages}", harness.effects())
                self.assertNotIn("docker.io-app", "\n".join(harness.effects()))

    # Delivery: executed distro allowlist for the deploy helper and public installer.
    # Rationale: adjacent and older releases must not inherit unqualified provisioning.
    def test_only_ubuntu_24_26_and_debian_13_are_admitted(self) -> None:
        supported = (("ubuntu", "24.04"), ("ubuntu", "26.04"), ("debian", "13"))
        unsupported = (
            ("ubuntu", "22.04"),
            ("ubuntu", "25.10"),
            ("debian", "12"),
            ("debian", "14"),
            ("fedora", "43"),
        )
        for distribution, version in supported + unsupported:
            with self.subTest(distribution=distribution, version=version):
                root = self.root / f"guard-{distribution}-{version.replace('.', '-')}"
                root.mkdir()
                harness = ProvisioningHarness(root, distribution, version)
                setup = harness.run()
                os_release = root / "installer-os-release"
                os_release.write_text(
                    f"ID={distribution}\nVERSION_ID={version}\n",
                    encoding="utf-8",
                )
                installer = subprocess.run(
                    ["sh", "-c", installer_os_guard(os_release)],
                    capture_output=True,
                    text=True,
                    check=False,
                )

                expected = 0 if (distribution, version) in supported else 1
                self.assertEqual(setup.returncode, expected, setup.stderr)
                self.assertEqual(installer.returncode, expected, installer.stderr)
                if expected:
                    self.assertEqual(harness.effects(), [])

    # Delivery: mocked preflight refusal, not detection of every third-party install.
    # Rationale: a partial Docker installation is not authority to replace packages.
    def test_incomplete_existing_docker_is_refused_without_package_or_service_effects(self) -> None:
        harness = ProvisioningHarness(self.root / "incomplete", "debian", "13")
        harness.install_docker(compose=False)

        result = harness.run()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("incomplete or incompatible", result.stderr)
        effects = harness.effects()
        self.assertFalse(any(effect.startswith("apt-get ") for effect in effects), effects)
        self.assertFalse(any(effect.startswith("systemctl reset-failed") for effect in effects), effects)
        self.assertFalse(any(effect.startswith("systemctl enable") for effect in effects), effects)
        self.assertFalse(any(effect.startswith("systemctl start") for effect in effects), effects)

    # Delivery: mocked repeated provisioning, not real daemon or registry continuity.
    # Rationale: a healthy distro installation is reused without reinstall/restart/recreate.
    def test_repeat_provisioning_has_no_reinstall_restart_or_recreate_effects(self) -> None:
        for distribution, version in (
            ("ubuntu", "24.04"),
            ("ubuntu", "26.04"),
            ("debian", "13"),
        ):
            with self.subTest(distribution=distribution, version=version):
                root = self.root / f"repeat-{distribution}-{version}"
                root.mkdir()
                harness = ProvisioningHarness(root, distribution, version)
                harness.install_docker()
                harness.healthy_registry()

                result = harness.run()

                self.assertEqual(result.returncode, 0, result.stderr)
                effects = harness.effects()
                self.assertFalse(any(effect.startswith("apt-get ") for effect in effects), effects)
                self.assertNotIn("systemctl start docker.service", effects)
                self.assertFalse(any(effect.startswith("docker run ") for effect in effects), effects)
                self.assertFalse(any(effect.startswith("docker start ") for effect in effects), effects)

    # Delivery: public help contract only, not installation behavior.
    def test_entrypoint_help_names_the_exact_supported_distributions(self) -> None:
        installer = subprocess.run(
            ["sh", str(INSTALLER), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        deploy_help = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        for result in (installer, deploy_help):
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("Ubuntu 24.04/26.04 or Debian 13", result.stdout)
            self.assertNotIn("Ubuntu 22.04", result.stdout)


if __name__ == "__main__":
    unittest.main()
