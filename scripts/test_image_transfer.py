"""Rationale: image distribution must not monopolize the QA host's shared data path."""
import io
import subprocess
import unittest

import image_transfer


class Clock:
    def __init__(self):
        self.now = 0
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class ImageTransferTest(unittest.TestCase):
    # Delivery: byte-stream pacing with fake time, not real network continuity.
    # Rationale: transport must preserve every byte while honoring the selected rate.
    def test_stream_is_bounded_and_preserves_exact_image_bytes(self):
        clock = Clock()
        raw = bytes(range(256)) * 3000
        destination = io.BytesIO()
        count = image_transfer.copy_paced(io.BytesIO(raw), destination, 65536, clock.time, clock.sleep)
        self.assertEqual(count, len(raw))
        self.assertEqual(destination.getvalue(), raw)
        self.assertGreaterEqual(clock.now, len(raw) / 65536 - 1)
        self.assertTrue(all(0 < delay <= 1 for delay in clock.sleeps))

    # Delivery: transfer-child cleanup with injected processes, not host process state.
    # Rationale: a broken receiver must leave neither owned transfer child running.
    def test_transfer_failure_terminates_only_owned_children(self):
        class BrokenInput(io.BytesIO):
            def write(self, value):
                raise BrokenPipeError("load exited")

        class Process:
            def __init__(self, source):
                self.stdout = io.BytesIO(b"image bytes") if source else None
                self.stdin = None if source else BrokenInput()
                self.terminated = False
                self.waited = False

            def poll(self):
                return None if not self.terminated else -15

            def terminate(self):
                self.terminated = True

            def wait(self, timeout):
                self.waited = True
                return -15

        processes = []

        def spawn(command, **kwargs):
            process = Process(command[0] == "docker")
            processes.append(process)
            return process

        with self.assertRaises(BrokenPipeError):
            image_transfer.transfer_images(["ssh", "root@qa"], ("agent", "runner"), spawn=spawn)
        self.assertEqual(len(processes), 2)
        self.assertTrue(all(process.terminated and process.waited for process in processes))

    # Delivery: pacing-input validation, not measured throughput.
    # Rationale: invalid rate must reject before consuming or writing image bytes.
    def test_invalid_rate_is_rejected_before_io(self):
        class UnexpectedIO:
            def read(self, _size):
                raise AssertionError("invalid rate read image bytes")

            def write(self, _value):
                raise AssertionError("invalid rate wrote image bytes")

        with self.assertRaises(ValueError):
            image_transfer.copy_paced(UnexpectedIO(), UnexpectedIO(), 0)


if __name__ == "__main__":
    unittest.main()
