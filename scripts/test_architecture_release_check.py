"""Delivery policy proof: existing structural debt cannot hide new violations."""

import json
import unittest

from architecture_release_check import check_report


class ArchitectureReleaseCheckTests(unittest.TestCase):
    def setUp(self):
        self.debt = {"path": "internal/example.go", "rule": "oversized-file-growth",
                     "message": "baseline allows 610 lines but file has 620", "line": 1}

    def test_exact_debt_is_deferred_despite_line_movement(self):
        moved = dict(self.debt, line=20)
        unexpected, count = check_report(1, json.dumps([moved]), [self.debt])
        self.assertFalse(unexpected)
        self.assertEqual(count, 1)

    def test_resolved_debt_does_not_require_retaining_a_violation(self):
        self.assertEqual(check_report(0, "null", [self.debt]), ({}, 0))
        self.assertEqual(check_report(0, "[]", [self.debt]), ({}, 0))

    def test_new_changed_and_duplicate_findings_block(self):
        for finding in (dict(self.debt, path="internal/new.go"),
                        dict(self.debt, message="baseline allows 610 lines but file has 621"),
                        dict(self.debt, rule="unsafe-import")):
            with self.subTest(finding=finding):
                self.assertTrue(check_report(1, json.dumps([finding]), [self.debt])[0])
        self.assertTrue(check_report(1, json.dumps([self.debt] * 2), [self.debt])[0])

    def test_checker_errors_and_malformed_output_block(self):
        for code, output in ((2, "[]"), (1, "null"), (1, ""), (1, "{}"),
                             (0, json.dumps([self.debt])), (1, '[{"path": "x"}]')):
            with self.subTest(code=code, output=output):
                with self.assertRaises(ValueError):
                    check_report(code, output, [self.debt])

    def test_only_test_imports_and_size_debt_can_be_deferred(self):
        test_import = dict(self.debt, path="internal/example_test.go", rule="layer-import")
        self.assertFalse(check_report(1, json.dumps([test_import]), [test_import])[0])
        for finding in (dict(test_import, path="internal/example.go"),
                        dict(self.debt, rule="unsafe-import")):
            with self.subTest(finding=finding):
                with self.assertRaises(ValueError):
                    check_report(1, json.dumps([finding]), [finding])


if __name__ == "__main__":
    unittest.main()
