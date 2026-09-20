"""PKG-07: cleanup on build failure must preserve pre-existing Docker state."""
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import install_ref


class RefBuildCleanupTests(unittest.TestCase):
    def exercise(self, fail_at):
        images = {install_ref.CLIENT_IMAGE, "unrelated:keep"}
        builders = {"unrelated"}
        cache = {"unrelated"}
        identity = "a" * 32
        owned = f"groundplane-ref-{identity}"

        def docker(*args, **kwargs):
            if args[:3] == ("docker", "image", "rm"):
                images.remove(args[3])
                return
            operation = args[args.index("buildx") + 1:]
            if operation[0] == "create":
                builders.add(owned)
                cache.add(owned)
                images.add(install_ref.BUILDKIT_IMAGE)
            elif operation[0] == "rm":
                builders.remove(operation[-1])
                cache.remove(operation[-1])
            elif operation[0] == "build":
                dockerfile = operation[operation.index("--file") + 1]
                if dockerfile == fail_at:
                    raise subprocess.CalledProcessError(1, args)

        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / ".runner-version").write_text("2.337.0")
            with patch.object(install_ref, "run", side_effect=docker), \
                 patch.object(install_ref, "DOCKER_SOCKET", source), \
                 patch.object(install_ref, "present", side_effect=lambda image: image in images), \
                 patch.object(install_ref.subprocess, "run"):
                if fail_at:
                    with self.assertRaises(subprocess.CalledProcessError):
                        install_ref.build(source, source, "0.0.1-ref.fixture", identity,
                                          source_epoch=1789900000)
                else:
                    install_ref.build(source, source, "0.0.1-ref.fixture", identity,
                                      source_epoch=1789900000)
        self.assertEqual(builders, {"unrelated"})
        self.assertEqual(cache, {"unrelated"})
        self.assertEqual(images, {install_ref.CLIENT_IMAGE, "unrelated:keep"})

    def test_failed_native_agent_or_runner_build_removes_only_owned_builder_state(self):
        for dockerfile in ("Dockerfile.build", "Dockerfile.agent", "Dockerfile.runner"):
            with self.subTest(dockerfile=dockerfile):
                self.exercise(dockerfile)

    def test_success_also_removes_builder_cache_and_preserves_existing_tool_image(self):
        self.exercise(None)


if __name__ == "__main__":
    unittest.main()
