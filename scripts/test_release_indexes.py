"""PKG-05: registry indexes must preserve both tested native image identities."""
import copy
import unittest

from release_indexes import checked_index


class ReleaseIndexTests(unittest.TestCase):
    def setUp(self):
        self.children = {arch: "ghcr.io/example/agent@sha256:" + digit * 64
                         for arch, digit in (("amd64", "1"), ("arm64", "2"))}
        self.index = {"manifests": [
            {"digest": ref.split("@")[1], "platform": {"os": "linux", "architecture": arch}}
            for arch, ref in self.children.items()]}

    def test_exact_native_pair_accepts_optional_arm_variant(self):
        checked_index(self.index, self.children)
        self.index["manifests"][1]["platform"]["variant"] = "v8"
        checked_index(self.index, self.children)

    def test_missing_extra_and_duplicate_platforms_refuse(self):
        for manifests in (self.index["manifests"][:1], self.index["manifests"] * 2,
                          [self.index["manifests"][0]] * 2):
            with self.subTest(manifests=manifests), self.assertRaises(ValueError):
                checked_index({"manifests": manifests}, self.children)

    def test_wrong_os_arch_variant_and_digest_refuse(self):
        for field, value in (("os", "windows"), ("architecture", "386"), ("variant", "v9")):
            index = copy.deepcopy(self.index)
            index["manifests"][0]["platform"][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                checked_index(index, self.children)
        self.index["manifests"][1]["digest"] = "sha256:" + "3" * 64
        with self.assertRaises(ValueError):
            checked_index(self.index, self.children)
