import assert from "node:assert/strict";
import { test } from "node:test";
import { createTaskDetailRefresh } from "../features/task/task-detail-refresh.ts";

test("Task events during a read queue a single prompt follow-up", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const reads = [];
  const refresh = createTaskDetailRefresh(() => {
    const read = Promise.withResolvers();
    reads.push(read);
    return read.promise;
  }, 2000);

  const initial = refresh.start();
  refresh.invalidate();
  refresh.invalidate();
  t.mock.timers.tick(500);
  assert.equal(
    reads.length,
    1,
    "the event burst must not overlap the first read",
  );

  reads[0].resolve();
  await initial;
  t.mock.timers.tick(200);
  assert.equal(
    reads.length,
    2,
    "the follow-up must beat the fallback interval",
  );
  refresh.invalidate();
  refresh.stop();
  reads[1].resolve();
  await reads[1].promise;
  t.mock.timers.tick(4000);
  assert.equal(
    reads.length,
    2,
    "a closed or finished Task must not keep reading",
  );
});

test("fallback reads continue after a quiet stream and stay off for a captured revision", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let liveReads = 0;
  let capturedReads = 0;
  const live = createTaskDetailRefresh(async () => {
    liveReads++;
  }, 1000);
  const captured = createTaskDetailRefresh(async () => {
    capturedReads++;
  });

  await live.start();
  await captured.start();
  t.mock.timers.tick(1000);
  assert.equal(liveReads, 2);
  assert.equal(capturedReads, 1);
  live.stop();
  captured.stop();
});
