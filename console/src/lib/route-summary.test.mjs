import assert from 'node:assert/strict'
import test from 'node:test'
import { routeSummaryHint } from '../features/environment/route-summary.ts'

// QA: HTTP-01, UI-05; local Route summary, not public reachability or rendering.
// Rationale: exposure alone cannot prove a missing provider or public reachability.
test('Route summary distinguishes empty, applied and mixed exposure', () => {
  assert.equal(routeSummaryHint([]), 'no routes')
  assert.equal(routeSummaryHint([{ exposure: 'public', status: 'served' }]), 'public · provider applied')
  assert.equal(routeSummaryHint([{ exposure: 'internal', status: 'served' }]), 'internal · provider applied')
  assert.equal(routeSummaryHint([
    { exposure: 'public', status: 'served' }, { exposure: 'internal', status: 'served' },
  ]), 'public + internal · provider applied')
})

// QA: HTTP-01, UI-05; local mixed-state summary, not live provider state.
// Rationale: one applied Route must not hide other pending or failed Routes.
test('Route summary counts every non-applied state in a stable order', () => {
  assert.equal(routeSummaryHint([{ exposure: 'public', status: 'unserved' }]), 'public · 1 unserved')
  assert.equal(routeSummaryHint([{ exposure: 'internal', status: 'pending' }]), 'internal · 1 pending')
  assert.equal(routeSummaryHint([{ exposure: 'public', status: 'degraded' }]), 'public · 1 degraded')
  const routes = [
    { exposure: 'public', status: 'served' }, { exposure: 'public', status: 'pending' },
    { exposure: 'public', status: 'unserved' }, { exposure: 'internal', status: 'degraded' },
    { exposure: 'public', status: 'pending' },
  ]
  const expected = 'public + internal · 1 degraded, 2 pending, 1 unserved'
  assert.equal(routeSummaryHint(routes), expected)
  assert.equal(routeSummaryHint([...routes].reverse()), expected)
})
