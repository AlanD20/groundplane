"""PKG-06: select one stable release, fail closed, and preserve explicit pins.

Execute the installer's real resolver and download section under sh and bash,
with a fake curl boundary. No root access, network, or host mutation is needed.
"""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "install.sh").read_text()
FUNCTIONS = SCRIPT.split("version='' bundle=", 1)[0]
DOWNLOAD = SCRIPT.split("\nresolve_version\n", 1)[1].split("\nactual=", 1)[0]


class InstallVersionTests(unittest.TestCase):
    def run_selection(self, shell, url, version="", failure="0"):
        with tempfile.TemporaryDirectory() as directory:
            env = dict(os.environ, RESOLVED_URL=url, PIN=version,
                       CURL_FAILURE=failure, PROBE_DIR=directory)
            # The resolver uses -I; downloads use -o. Record URLs independently
            # so a second latest lookup or moving download cannot pass unnoticed.
            stub = r'''
curl() {
    output=''
    for argument in "$@"; do
        if test "$previous" = --output; then output=$argument; fi
        previous=$argument
        url=$argument
    done
    printf '%s\n' "$url" >> "$deploy_dir/requests"
    case "$url" in
        */releases/latest)
            test "$CURL_FAILURE" = 0 || return "$CURL_FAILURE"
            printf '%s' "$RESOLVED_URL" ;;
        *.sha256) printf '%s  %s\n' "$digest" "$asset" > "$output" ;;
        *.tar.gz) printf 'fixture' > "$output" ;;
        *) return 90 ;;
    esac
}
version=$PIN
deploy_dir=$PROBE_DIR
bundle='' checksum='' arch=amd64 previous=''
digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
resolve_version
'''
            result = subprocess.run([shell], input=FUNCTIONS + stub + DOWNLOAD,
                                    text=True, capture_output=True, env=env)
            requests = Path(directory, "requests")
            return result, requests.read_text().splitlines() if requests.exists() else []

    def test_default_resolves_once_and_pins_bundle_and_checksum(self):
        for shell in ("sh", "bash"):
            with self.subTest(shell=shell):
                result, requests = self.run_selection(
                    shell, "https://github.com/AlanD20/groundplane/releases/tag/v1.2.3")
                self.assertEqual(result.returncode, 0, result.stderr)
                base = "https://github.com/AlanD20/groundplane/releases/"
                asset = "download/v1.2.3/groundplane-1.2.3-linux-amd64.tar.gz"
                self.assertEqual(requests, [base + "latest", base + asset, base + asset + ".sha256"])

    def test_explicit_pin_never_queries_latest(self):
        for shell in ("sh", "bash"):
            with self.subTest(shell=shell):
                result, requests = self.run_selection(shell, "must not be used", "0.0.1", "22")
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(len(requests), 2)
                self.assertTrue(all("/download/v0.0.1/" in url for url in requests))

    def test_unavailable_or_untrusted_latest_never_downloads_bundle(self):
        for shell in ("sh", "bash"):
            for url in ("", "https://github.com/AlanD20/groundplane/releases",
                        "https://example.com/releases/tag/v1.2.3",
                        "https://github.com/AlanD20/other/releases/tag/v1.2.3",
                        "https://github.com/AlanD20/groundplane/releases/tag/v1.2.3-rc.1",
                        "https://github.com/AlanD20/groundplane/releases/tag/v01.2.3",
                        "https://github.com/AlanD20/groundplane/releases/tag/v1.2.3?bad=1"):
                with self.subTest(shell=shell, url=url):
                    result, requests = self.run_selection(shell, url)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertEqual(len(requests), 1)

    def test_network_failure_never_downloads_bundle(self):
        for shell in ("sh", "bash"):
            result, requests = self.run_selection(shell, "", failure="22")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Cannot resolve latest", result.stderr)
            self.assertEqual(len(requests), 1)

    def test_local_bundle_requires_explicit_pin_before_host_effects(self):
        for shell in ("sh", "bash"):
            result = subprocess.run([shell, str(ROOT / "install.sh"), "--bundle", "unused"],
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 2)
            self.assertIn("--bundle requires --version", result.stderr)


if __name__ == "__main__":
    unittest.main()
