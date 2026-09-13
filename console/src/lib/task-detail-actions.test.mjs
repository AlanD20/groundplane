import assert from 'node:assert/strict'
import test from 'node:test'
import { taskDetailActions } from './task-detail-actions.ts'

// QA: TASK-05, UP-07; local Retry visibility, not activation recovery.
// Rationale: native updates must recover before a fresh explicit update; generic
// Task Retry cannot repeat host activation from a frozen historical request.
test('native Controller updates never expose generic Task Retry', () => {
  for (const status of ['pending', 'running', 'completed', 'failed', 'timed_out', 'aborted']) {
    assert.equal(taskDetailActions({ type: 'update', target: 'controller', status }).retry, false)
  }
  assert.equal(taskDetailActions({ type: 'update', target: 'agt_1', status: 'failed' }).retry, true)
})

// QA: BAK-08, TASK-03, TASK-05; local control visibility, not API refusal.
// Rationale: internal retention cleanup remains observable but cannot expose
// any generic operator mutation regardless of its journal state.
test('backup_prune exposes no operator control in any lifecycle state', () => {
  for (const status of ['pending', 'running', 'completed', 'failed', 'timed_out', 'aborted']) {
    assert.deepEqual(taskDetailActions({ type: 'backup_prune', status }), {
      abort: false,
      cancel: false,
      retry: false,
    })
  }
})

// QA: BAK-05, TASK-05; local Retry intent, not backup resumption or effect safety.
// Rationale: ordinary Backup Tasks follow the generic retry contract; only
// internal prune Tasks are excluded from operator retry controls.
test('terminal backup Tasks expose the generic retry request intent', () => {
  for (const status of ['completed', 'failed', 'timed_out', 'aborted']) {
    assert.deepEqual(taskDetailActions({ type: 'backup', status }), {
      abort: false,
      cancel: false,
      retry: status !== 'completed',
    })
  }
})

// QA: TASK-03, TASK-05; local lifecycle controls, not effect cancellation/retry.
// Rationale: excluding internal prune Tasks must not change the established
// lifecycle controls for ordinary operator-facing Tasks.
test('operator Task controls retain their lifecycle-specific actions', () => {
  assert.deepEqual(taskDetailActions({ type: 'deploy', status: 'pending' }), {
    abort: false,
    cancel: true,
    retry: false,
  })
  assert.deepEqual(taskDetailActions({ type: 'deploy', status: 'running' }), {
    abort: true,
    cancel: false,
    retry: false,
  })
  assert.deepEqual(taskDetailActions({ type: 'deploy', status: 'failed' }), {
    abort: false,
    cancel: false,
    retry: true,
  })
})
