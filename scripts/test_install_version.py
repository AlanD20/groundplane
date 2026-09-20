"""PKG-06/08: execute scoped release lookup and pinned downloads without host effects."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "install.sh").read_text()
FUNCTIONS = SCRIPT.split("\nversion=", 1)[0]
DOWNLOAD = SCRIPT.split("\nresolve_version\n", 1)[1].split("\nactual=", 1)[0]


class InstallVersionTests(unittest.TestCase):
    def select(self, shell, component, catalog, version="", failure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # Replace only the external HTTP boundary, not the resolver logic.
            (root / "sitecustomize.py").write_text('''
import io,os,urllib.request
def fetch(request, timeout):
    with open(os.environ["PROBE_DIR"]+"/requests","a") as out: out.write(request.full_url+"\\n")
    if os.environ["FAIL_LOOKUP"] == "1": raise OSError("lookup unavailable")
    response=io.BytesIO(os.environ["CATALOG"].encode())
    response.url=request.full_url
    return response
urllib.request.urlopen=fetch
''')
            environment = dict(os.environ, PYTHONPATH=directory, PROBE_DIR=directory,
                               CATALOG=json.dumps(catalog), FAIL_LOOKUP="1" if failure else "0",
                               PIN=version, COMPONENT=component)
            stub = r'''
curl() {
    output='' previous=''
    for argument in "$@"; do
        if test "$previous" = --output; then output=$argument; fi
        previous=$argument
        url=$argument
    done
    printf '%s\n' "$url" >> "$deploy_dir/requests"
    case "$url" in
        *.sha256) printf '%s  %s\n' "$digest" "$asset" > "$output" ;;
        *.tar.gz) printf fixture > "$output" ;;
        *) return 90 ;;
    esac
}
version=$PIN release_component=$COMPONENT scope=both
test "$COMPONENT" != agent || scope=agent
deploy_dir=$PROBE_DIR
bundle='' checksum='' arch=amd64
digest=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
resolve_version
'''
            result = subprocess.run([shell], input=FUNCTIONS + stub + DOWNLOAD, env=environment,
                                    text=True, capture_output=True)
            path = root / "requests"
            return result, path.read_text().splitlines() if path.exists() else []

    def test_namespaces_never_cross_and_stable_versions_sort_numerically(self):
        catalog = [{"tag_name": tag} for tag in ("v9.0.0", "agent/v1.9.0", "agent/v1.10.0",
                    "controller/v2.0.0", "agent/v01.11.0", "agent/v9.0.0-rc.1")]
        catalog += [{"tag_name": "agent/v8.0.0", "draft": True}]
        for shell in ("sh", "bash"):
            for component, version in (("", "9.0.0"), ("agent", "1.10.0"), ("controller", "2.0.0")):
                with self.subTest(shell=shell, component=component):
                    result, requests = self.select(shell, component, catalog)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(len(requests), 3)
                    tag = f"{component}%2Fv{version}" if component else f"v{version}"
                    self.assertTrue(all(f"/download/{tag}/" in url for url in requests[1:]))

    def test_explicit_pin_bypasses_lookup_and_missing_namespace_stops_download(self):
        for shell in ("sh", "bash"):
            result, requests = self.select(shell, "agent", [], "1.2.3", failure=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(requests), 2)
            self.assertTrue(all("/download/agent%2Fv1.2.3/" in url for url in requests))
            for catalog, failure in (([{"tag_name": "v1.2.3"}], False), ([], True)):
                result, requests = self.select(shell, "agent", catalog, failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(requests), 1)


if __name__ == "__main__":
    unittest.main()
