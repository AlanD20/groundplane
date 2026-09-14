"""Delivery safety: run the actual remote restart body against fake commands.

REL-01 support only. This guards the verifier's decision, not live continuity.
No SSH, systemd, Docker or installed Groundplane command is executed.
"""

import os
from pathlib import Path
import subprocess
import unittest


SCRIPT = Path(__file__).with_name("foundation-host-acceptance.sh")
AGENT_ID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

FAKES = r'''
restarted=0
die() { printf '%s\n' "$*" >&2; return 1; }
systemctl() {
  case "$*" in
    'show groundplane-controller.service -p RuntimeDirectoryPreserve --value') printf 'restart\n' ;;
    'show groundplane-controller.service -p MainPID --value') printf '%s\n' "$((100 + restarted))" ;;
    'restart groundplane-controller.service') restarted=1 ;;
    'is-active groundplane-controller.service') printf 'active\n' ;;
    *) return 99 ;;
  esac
}
stat() { [[ $* == '-c %i /run/groundplane/controller' ]] && printf '123\n'; }
function /usr/local/bin/groundplane {
  [[ $* == 'agent list --output json' ]] || return 99
  printf '{"items":[{"id":"%s","status":"healthy"}]}\n' "$TEST_AGENT_ID"
}
docker() {
  [[ $1 == inspect ]] || return 99
  if [[ $2 == groundplane-agent ]]; then
    if [[ $restarted == 1 ]]; then
      [[ $TEST_CASE != changed_agent ]] || { printf 'foreign|new-start|1\n'; return; }
      printf 'agent-container|new-start|1\n'
    else
      printf 'agent-container|old-start|1\n'
    fi
  elif [[ $2 == groundplane-etcd ]]; then
    if [[ $restarted == 1 && $TEST_CASE == restarted_etcd ]]; then
      printf 'etcd-container|new-start|1|true\n'
    elif [[ $restarted == 1 && $TEST_CASE == stopped_etcd ]]; then
      printf 'etcd-container|old-start|0|false\n'
    else
      printf 'etcd-container|old-start|0|true\n'
    fi
  else
    return 99
  fi
}
'''


class FoundationRestartTest(unittest.TestCase):
    def test_restart_boundary(self):
        # Execute the maintained probe, not a copied version of its assertions.
        function = SCRIPT.read_text().split("restart_controller() {", 1)[1]
        body = function.split("<<'REMOTE'\n", 1)[1].split("\nREMOTE\n", 1)[0]
        for scenario, accepted in (
            ("agent_restart", True),
            ("restarted_etcd", False),
            ("stopped_etcd", False),
            ("changed_agent", False),
        ):
            with self.subTest(scenario=scenario):
                result = subprocess.run(
                    ["bash", "-s", "--", AGENT_ID],
                    input=FAKES + "\n" + body,
                    text=True,
                    capture_output=True,
                    timeout=5,
                    env={**os.environ, "TEST_CASE": scenario, "TEST_AGENT_ID": AGENT_ID},
                    check=False,
                )
                self.assertEqual(result.returncode == 0, accepted, result.stdout + result.stderr)
                if accepted:
                    self.assertIn("before_agent=agent-container|old-start|1", result.stdout)
                    self.assertIn("after_agent=agent-container|new-start|1", result.stdout)
                    self.assertIn("etcd=etcd-container|old-start|0|true", result.stdout)


if __name__ == "__main__":
    unittest.main()
