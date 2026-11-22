import assert from 'node:assert/strict'
import test from 'node:test'
import { monitoringHealth } from '../features/monitoring/health-summary.ts'

const observedAt = '2026-11-22T12:00:00.000Z'
const expiresAt = '2026-11-22T12:00:15.000Z'
const now = Date.parse(observedAt) + 1

function service(state, runtimeIntent = 'running') {
  return {
    id: state,
    name: state,
    runtimeIntent,
    observation: {
      state, observedAt, expiresAt, expectedReplicas: 1,
      replicas: { healthy: 0, running: 0, starting: 0, unhealthy: 0, transitional: 0, stopped: 0, failed: 0, [state]: 1 },
      containers: [],
    },
  }
}

// A stale healthy snapshot must not inflate health or reporting coverage.
test('monitoring excludes expired observations and preserves their last sample time', () => {
  const summary = monitoringHealth([service('healthy')], Date.parse(expiresAt))
  assert.equal(summary.healthy, 0)
  assert.equal(summary.reported, 0)
  assert.equal(summary.unavailable, 1)
  assert.equal(summary.rows[0].sampledAt, observedAt)
  assert.equal(summary.rows[0].state, 'unavailable')
})

// Desired stops differ from failed workloads; unhealthchecked runtime is not healthy.
test('monitoring distinguishes intentional stops, unexpected stops and runtime without healthchecks', () => {
  const summary = monitoringHealth([
    service('stopped', 'stopped'), service('absent', 'absent'),
    service('stopped'), service('running'), service('healthy'), service('failed'),
  ], now)
  assert.equal(summary.attention, 2)
  assert.deepEqual(summary.rows.map((row) => row.attention), [false, false, true, false, false, true])
  assert.equal(summary.healthy, 1)
  assert.equal(summary.running, 1)
  assert.equal(summary.reported, 6)
})
