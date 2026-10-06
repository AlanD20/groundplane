import assert from "node:assert/strict";
import test from "node:test";
import {
  backupScheduleError,
  backupSchedulePreview,
} from "../features/backup/backup-schedule.ts";

// BAK-04: the Console must not promise times different from the Controller.
// Independent UTC expectations cover steps, restricted-day OR, month bounds,
// and the eight-year leap-century gap, rather than testing rendered text.
test("Cron preview follows the same UTC calendar rules as backup dispatch", () => {
  for (const [expression, after, want] of [
    ["15 * * * *", "2026-10-06T02:15:00Z", "2026-10-06T03:15:00.000Z"],
    ["0 */6 * * *", "2026-10-06T05:59:59Z", "2026-10-06T06:00:00.000Z"],
    [
      "5,35 9-17 * * MON-FRI",
      "2026-10-06T17:36:00Z",
      "2026-10-07T09:05:00.000Z",
    ],
    ["0 0 31 * *", "2026-04-30T00:00:00Z", "2026-05-31T00:00:00.000Z"],
    ["0 0 29 FEB *", "2096-03-01T00:00:00Z", "2104-02-29T00:00:00.000Z"],
    ["0 0 1 * MON", "2026-10-06T10:00:00Z", "2026-10-12T00:00:00.000Z"],
    ["0 0 1-31 * MON", "2026-10-06T10:00:00Z", "2026-10-07T00:00:00.000Z"],
    ["0 0 30 FEB MON", "2026-02-01T00:00:00Z", "2026-02-02T00:00:00.000Z"],
  ]) {
    const result = backupSchedulePreview(expression, new Date(after));
    assert.equal(result.error, null, expression);
    assert.equal(result.nextRuns[0].toISOString(), want, expression);
    assert.equal(result.nextRuns.length, 3);
    assert.ok(result.nextRuns[1] > result.nextRuns[0]);
    assert.ok(result.nextRuns[2] > result.nextRuns[1]);
  }
});

// BAK-04: unsupported/old expressions cannot enable an invalid policy locally.
test("Cron preview rejects impossible dates and unsupported syntax", () => {
  for (const value of [
    "*-*-* 03:15:00",
    "Sun *-*-* 03:15:00",
    "0 15 3 * * *",
    "@daily",
    "0 0 30 FEB *",
    "60 * * * *",
    "*/0 * * * *",
    "0 0 * * 7",
    "0 0 L * *",
    "0 0 * * MON#2",
    "1,,2 * * * *",
  ]) {
    assert.notEqual(backupScheduleError(value), null, value);
    assert.notEqual(
      backupSchedulePreview(value, new Date()).error,
      null,
      value,
    );
  }
});
