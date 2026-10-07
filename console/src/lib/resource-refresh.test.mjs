import assert from "node:assert/strict";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import {
  ResourceRefreshQueue,
  RefreshSuperseded,
} from "../features/task/resource-refresh-queue.ts";
import {
  observeTerminalTask,
  subscribeTerminalTasks,
} from "../features/task/terminal-observation.ts";
import { mergeEnvironmentProjectLoads } from "../features/environment/workspace-reconciliation.ts";

async function until(predicate) {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await delay(5);
  }
  assert.fail("refresh did not settle");
}

// Mutation completion can arrive from multiple readers after its dialog closes.
test("terminal observation deduplicates readers and includes failed partial effects", () => {
  const seen = [];
  const unsubscribe = subscribeTerminalTasks((task) => seen.push(task.status));
  try {
    const task = { id: "refresh-test", updated_at: "now", status: "running" };
    observeTerminalTask(task);
    observeTerminalTask({ ...task, status: "failed" });
    observeTerminalTask({ ...task, status: "failed" });
    assert.deepEqual(seen, ["failed"]);
  } finally {
    unsubscribe();
  }
});

test("coalesced refresh discards a read superseded by another completion", async () => {
  let release;
  const reads = [],
    applied = [];
  const queue = new ResourceRefreshQueue(
    async (key, isCurrent) => {
      reads.push(key);
      if (reads.length === 1)
        await new Promise((resolve) => {
          release = resolve;
        });
      if (isCurrent()) applied.push(key);
    },
    () => {},
    0,
  );
  try {
    queue.invalidate("environment");
    queue.invalidate("environment");
    await until(() => release);
    queue.invalidate("environment");
    release();
    await until(() => applied.length === 1);
    assert.deepEqual(reads, ["environment", "environment"]);
  } finally {
    queue.close();
  }
});

test("failed refresh remains retryable and closing prevents stale application", async () => {
  let fail = true,
    errors = [],
    applied = false;
  const queue = new ResourceRefreshQueue(
    async (_key, isCurrent) => {
      if (fail) throw new Error("offline");
      await delay(10);
      if (isCurrent()) applied = true;
    },
    (next) => {
      errors = next;
    },
    0,
  );
  queue.invalidate("environment");
  await until(() => errors.length);
  assert.deepEqual(errors, ["offline"]);
  fail = false;
  queue.retry();
  await until(() => applied);
  await until(() => errors.length === 0);
  applied = false;
  queue.invalidate("environment");
  queue.close();
  await delay(20);
  assert.equal(applied, false);
});

test("backing reload uses new data except for concurrent mutations and deleted environments", () => {
  const current = [
    {
      id: "project",
      environments: [
        { id: "a", name: "old" },
        { id: "b", name: "local" },
      ],
    },
  ];
  const loaded = [
    {
      id: "project",
      environments: [
        { id: "a", name: "server" },
        { id: "b", name: "stale" },
        { id: "deleted" },
      ],
    },
  ];
  const result = mergeEnvironmentProjectLoads(
    current,
    loaded,
    new Map(),
    new Map([["b", 1]]),
    (id) => id !== "deleted",
  );
  assert.deepEqual(result[0].environments, [
    { id: "a", name: "server" },
    { id: "b", name: "local" },
  ]);
});

// A write arriving during a read must cause a new read, not a stale commit.
test("a superseded resource read retries without reporting a refresh failure", async () => {
  let reads = 0,
    applied = false,
    errors = [];
  const queue = new ResourceRefreshQueue(
    async (_key, current) => {
      if (++reads === 1) throw new RefreshSuperseded();
      if (current()) applied = true;
    },
    (next) => {
      errors = next;
    },
    0,
  );
  try {
    queue.invalidate("environment");
    await until(() => applied);
    assert.equal(reads, 2);
    assert.deepEqual(errors, []);
  } finally {
    queue.close();
  }
});
