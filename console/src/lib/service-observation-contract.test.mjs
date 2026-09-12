import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import {
  applyServiceObservations,
  currentServiceObservation,
  environmentRuntimeState,
  serviceObservationFromAPI,
} from '../features/service/service-observation.ts'

const observedAt = '2026-09-12T12:00:00.000Z'
const expiresAt = '2026-09-12T12:00:15.000Z'
const zeroCounts = {
  running: 0,
  healthy: 0,
  starting: 0,
  unhealthy: 0,
  transitional: 0,
  stopped: 0,
  failed: 0,
}

function wireObservation(state, replicas, expectedReplicas = 1) {
  return {
    state,
    observed_at: observedAt,
    expires_at: expiresAt,
    serving_release_id: '01JRELEASE0000000000000000',
    expected_replicas: expectedReplicas,
    replicas: { ...zeroCounts, ...replicas },
  }
}

function service(observation) {
  return { observation }
}

// Rationale: malformed evidence cannot be promoted into runtime health.
test('Service observation accepts only complete state-consistent bounded snapshots', () => {
  const healthy = serviceObservationFromAPI(wireObservation('healthy', { healthy: 2 }, 2))
  assert.equal(healthy.state, 'healthy')
  assert.deepEqual(serviceObservationFromAPI(undefined), { state: 'unavailable' })
  assert.deepEqual(serviceObservationFromAPI({ state: 'unavailable' }), { state: 'unavailable' })
  assert.deepEqual(
    serviceObservationFromAPI({ ...wireObservation('healthy', { healthy: 1 }), expires_at: '2026-09-12T12:00:14.999Z' }),
    { state: 'unavailable' },
  )
  assert.deepEqual(serviceObservationFromAPI(wireObservation('healthy', { running: 1 })), { state: 'unavailable' })
  assert.deepEqual(
    serviceObservationFromAPI(wireObservation('degraded', { unhealthy: 4097 }, 4097)),
    { state: 'unavailable' },
  )
})

// Rationale: a stalled response must not extend the previous snapshot's lifetime.
test('Service observation expires locally at expires_at', () => {
  const observation = serviceObservationFromAPI(wireObservation('running', { running: 1 }))
  assert.deepEqual(currentServiceObservation(observation, Date.parse(observedAt) - 1), { state: 'unavailable' })
  assert.equal(currentServiceObservation(observation, Date.parse(expiresAt) - 1).state, 'running')
  assert.deepEqual(currentServiceObservation(observation, Date.parse(expiresAt)), { state: 'unavailable' })
})

// Rationale: provisioning and desired intent cannot conceal incomplete workload evidence.
test('Environment runtime aggregate never promotes incomplete or missing runtime to healthy', () => {
  const healthy = serviceObservationFromAPI(wireObservation('healthy', { healthy: 1 }))
  const running = serviceObservationFromAPI(wireObservation('running', { running: 1 }))
  const stopped = serviceObservationFromAPI(wireObservation('stopped', { stopped: 1 }))
  const absent = serviceObservationFromAPI(wireObservation('absent', {}, 1))
  const starting = serviceObservationFromAPI(wireObservation('starting', { starting: 1 }))
  const failed = serviceObservationFromAPI(wireObservation('failed', { failed: 1 }))
  const fresh = Date.parse(expiresAt) - 1

  assert.equal(environmentRuntimeState([service(healthy), service(healthy)], fresh), 'healthy')
  assert.equal(environmentRuntimeState([service(healthy), service(running)], fresh), 'running')
  assert.equal(environmentRuntimeState([service(stopped)], fresh), 'stopped')
  assert.equal(environmentRuntimeState([service(absent)], fresh), 'absent')
  assert.equal(environmentRuntimeState([service(starting)], fresh), 'starting')
  assert.equal(environmentRuntimeState([service(failed)], fresh), 'failed')
  assert.equal(environmentRuntimeState([service(stopped), service(healthy)], fresh), 'degraded')
  assert.equal(environmentRuntimeState([service(healthy), service({ state: 'unavailable' })], fresh), 'unavailable')
  assert.equal(environmentRuntimeState([service(healthy)], Date.parse(expiresAt)), 'unavailable')
  assert.equal(environmentRuntimeState([], fresh), 'unavailable')
})

// Rationale: refreshing evidence must not replace desired fields or another Environment.
test('Service observation refresh replaces only matching runtime evidence', () => {
  const healthy = serviceObservationFromAPI(wireObservation('healthy', { healthy: 1 }))
  const selected = { id: 'selected', replicas: 9, runtimeIntent: 'running', observation: { state: 'unavailable' } }
  const missing = { id: 'missing', observation: healthy }
  const unrelated = { id: 'selected', observation: healthy }
  const projects = [{ environments: [
    { id: 'target', services: [selected, missing] },
    { id: 'other', services: [unrelated] },
  ] }]
  applyServiceObservations(projects, 'target', [{ id: 'selected', replicas: 1, observation: healthy }])
  assert.equal(selected.observation, healthy)
  assert.equal(selected.replicas, 9)
  assert.equal(selected.runtimeIntent, 'running')
  assert.deepEqual(missing.observation, { state: 'unavailable' })
  assert.equal(unrelated.observation, healthy)
})

// Rationale: one Agent observation worker requires serial visible refreshes.
test('Console wiring refreshes visible observations serially and no longer stores Service status', async () => {
  const [store, refresh] = await Promise.all([
    readFile(new URL('./store.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../features/service/use-service-observation-refresh.ts', import.meta.url), 'utf8'),
  ])
  assert.match(store, /refreshEnvironmentServices/)
  assert.doesNotMatch(store, /status: 'unknown'/)
  assert.match(refresh, /document\.visibilityState !== 'visible'/)
  assert.match(refresh, /for \(const environmentId of ids\)/)
  assert.doesNotMatch(refresh, /Promise\.all\(ids\.map/)
})
