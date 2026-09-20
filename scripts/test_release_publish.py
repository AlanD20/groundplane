"""PKG-08: scoped publication must reuse artifacts and refuse tag conflicts before writes."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error

import release_publish


class ReleasePublicationTests(unittest.TestCase):
    def exercise(self, conflict=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for component in ("groundplane", "groundplane-agent"):
                for arch in ("amd64", "arm64"):
                    for suffix in ("tar.gz", "tar.gz.sha256"):
                        (root / f"{component}-1.2.3-linux-{arch}.{suffix}").write_bytes(b"qualified artifact")
            (root / "agent.json").write_text("{}")
            calls = []

            def run(*args):
                calls.append(args)
                if args[:2] == ("git", "rev-parse"):
                    return "a" * 40
                if args[:2] == ("git", "ls-remote"):
                    if conflict and "refs/tags/controller/v1.2.3" in args:
                        return "b" * 40 + "\trefs/tags/controller/v1.2.3"
                    return ""
                return ""

            missing = urllib.error.HTTPError("https://api.github.com", 404, "absent", {}, None)
            with patch.object(release_publish, "run", side_effect=run), \
                    patch.object(release_publish, "release_notes", return_value="Release feature notes"), \
                    patch.object(release_publish, "read_json", side_effect=missing):
                if conflict:
                    with self.assertRaisesRegex(ValueError, "another commit"):
                        release_publish.publish("v1.2.3", root)
                else:
                    release_publish.publish("v1.2.3", root)
            return calls

    def test_combined_release_creates_scoped_tags_at_same_commit_and_separates_assets(self):
        calls = self.exercise()
        tags = [call for call in calls if call[:2] == ("gh", "api")]
        self.assertEqual(len(tags), 3)
        self.assertTrue(all("sha=" + "a" * 40 in call for call in tags))
        releases = {call[3]: call for call in calls if call[:3] == ("gh", "release", "create")}
        self.assertEqual(set(releases), {"v1.2.3", "agent/v1.2.3", "controller/v1.2.3"})
        agent_files = [Path(arg).name for arg in releases["agent/v1.2.3"] if ".tar.gz" in arg]
        controller_files = [Path(arg).name for arg in releases["controller/v1.2.3"] if ".tar.gz" in arg]
        self.assertEqual(len(agent_files), 4)
        self.assertEqual(len(controller_files), 4)
        self.assertTrue(all(name.startswith("groundplane-agent-") for name in agent_files))
        self.assertTrue(all(name.startswith("groundplane-1.2.3-") for name in controller_files))

    def test_conflicting_component_tag_prevents_every_publication_write(self):
        calls = self.exercise(conflict=True)
        self.assertFalse(any(call[0] == "gh" for call in calls))
