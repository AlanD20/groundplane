"""Rationale: remote verification must enforce the current Host contract, including absence."""
import copy
import json
from pathlib import Path
import re
import subprocess
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / ".agents/skills/verify-groundplane/scripts/host-health-ssh.sh"
CONTRACT = re.search(r"host_contract='\n(.*?)\n'", SCRIPT.read_text(), re.S).group(1)


class HostVerificationContractTest(unittest.TestCase):
    def host(self):
        resource = {"total": "1 GiB", "used": "0 B", "used_pct": 0}
        return {"hostname": "qa", "arch": "amd64", "os": "Ubuntu", "uptime": "1 day", "docker": "29.1.3",
                "cpu": {"cores": 2, "model": "CPU", "load": 0}, "memory": resource, "disk": resource, "swap": resource,
                "etcd": {"db_size": "1 MiB", "node": "single-node", "status": "healthy"},
                "agent": {"labels": [], "max_concurrent": 1, "pull_interval": "2s", "status": "healthy"},
                "controller": {"service": "groundplane-controller.service", "status": "healthy", "version": "dev",
                               "update": {"running_sha256": "sha256:" + "a" * 64, "available": False, "error": "",
                                          "candidate": None, "last_update": None}}}

    def valid(self, host):
        result = subprocess.run(["jq", "-e", CONTRACT + "\ncanonical_host"], input=json.dumps(host),
                                text=True, capture_output=True, check=False)
        return result.returncode == 0

    # Delivery: verifier predicate against fixed examples, not a HOST-01/UP-11 pass.
    # Rationale: valid nullable update state and recovered history must be readable.
    def test_valid_absence_and_candidate_history(self):
        host = self.host()
        self.assertTrue(self.valid(host))
        update = host["controller"]["update"]
        update["candidate"] = {"release": "sha256:" + "b" * 64, "controller_sha256": "sha256:" + "c" * 64,
                               "controller_version": "1.2.3", "agent_image": "localhost:5000/agent@sha256:" + "d" * 64,
                               "storage_epoch": 1, "channel_schema": 1}
        update["last_update"] = {"task_id": "task_01M2482EK5HAACSE0000000001", "release": "sha256:" + "b" * 64,
                                 "status": "failed", "phase": "recovered", "created_at": "2026-09-10T00:00:00Z"}
        self.assertTrue(self.valid(host))

    # Delivery: verifier negative controls, not deployed Host health.
    # Rationale: incomplete or wrongly typed update evidence must not pass the oracle.
    def test_missing_or_malformed_update_metadata_is_rejected(self):
        original = self.host()
        for key, bad in (("available", "false"), ("running_sha256", "mutable-tag"), ("candidate", {}),
                         ("last_update", {}), ("error", False)):
            with self.subTest(key=key):
                host = copy.deepcopy(original)
                host["controller"]["update"][key] = bad
                self.assertFalse(self.valid(host))
        del original["controller"]["update"]
        self.assertFalse(self.valid(original))


if __name__ == "__main__":
    unittest.main()
