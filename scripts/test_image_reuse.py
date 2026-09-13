"""Rationale: repeat deployment must not resend an unchanged gigabyte-scale Runner."""
import subprocess
import unittest

import image_transfer


AGENT = "groundplane-agent:deploy-" + "a" * 32
RUNNER = "groundplane-runner:deploy-" + "a" * 32
AGENT_ID = "sha256:" + "b" * 64
RUNNER_ID = "sha256:" + "c" * 64


class ImageReuseTest(unittest.TestCase):
    # Delivery: recorded content-selection commands, not real registry/image behavior.
    # Rationale: transfer missing content only and alias the exact verified existing ID.
    def test_only_absent_content_is_transferred_and_existing_alias_is_exact(self):
        commands = []

        def run(command, **kwargs):
            commands.append(command)
            if command[0] == "docker":
                return subprocess.CompletedProcess(command, 0, AGENT_ID if command[-1] == AGENT else RUNNER_ID)
            if "inspect" in command[-1]:
                if AGENT_ID in command[-1]:
                    return subprocess.CompletedProcess(command, 1, "")
                return subprocess.CompletedProcess(command, 0, RUNNER_ID + "\n")
            return subprocess.CompletedProcess(command, 0, "")

        missing = image_transfer.prepare_images(["ssh", "root@qa"], (AGENT, RUNNER), run=run)
        self.assertEqual(missing, (AGENT,))
        self.assertEqual(commands[-1], ["ssh", "root@qa", "docker tag " + RUNNER_ID + " " + RUNNER])

    # Delivery: content-identity rejection with injected inspection results.
    # Rationale: a successful inspect command cannot authorize different image bytes.
    def test_mismatched_target_identity_cannot_be_reused(self):
        def run(command, **kwargs):
            return subprocess.CompletedProcess(command, 0, AGENT_ID if command[0] == "docker" else RUNNER_ID)

        with self.assertRaisesRegex(ValueError, "identity"):
            image_transfer.prepare_images(["ssh", "root@qa"], (AGENT,), run=run)

    # Delivery: transport-status classification, not an actual SSH outage.
    # Rationale: unreachable target must not be mistaken for missing content to resend.
    def test_ssh_failure_is_not_reported_as_an_absent_image(self):
        def run(command, **kwargs):
            return subprocess.CompletedProcess(command, 0, AGENT_ID) if command[0] == "docker" else \
                subprocess.CompletedProcess(command, 255, "")

        with self.assertRaises(subprocess.CalledProcessError):
            image_transfer.prepare_images(["ssh", "root@qa"], (AGENT,), run=run)

    # Delivery: pre-SSH identifier validation, not remote shell isolation.
    # Rationale: untrusted inspection output cannot enter a target command.
    def test_untrusted_local_identity_is_rejected_before_ssh(self):
        commands = []

        def run(command, **kwargs):
            commands.append(command)
            return subprocess.CompletedProcess(command, 0, "sha256:not-an-identity; false")

        with self.assertRaises(ValueError):
            image_transfer.prepare_images(["ssh", "root@qa"], (AGENT,), run=run)
        self.assertEqual(len(commands), 1)

    # Delivery: transfer admission, not a running image deployment.
    # Rationale: no missing images means no save/load children or empty archive work.
    def test_all_present_images_do_not_spawn_an_empty_archive(self):
        def spawn(*args, **kwargs):
            self.fail("no docker save/load process should be created")

        image_transfer.transfer_images(["ssh", "root@qa"], (), spawn=spawn)


if __name__ == "__main__":
    unittest.main()
