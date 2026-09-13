import assert from 'node:assert/strict'
import test from 'node:test'
import { scriptFromAPI, scriptCreateToAPI, scriptPatchToAPI } from './script-api.ts'

const image = `setup@sha256:${'a'.repeat(64)}`
const volumeId = 'vol_01ARZ3NDEKTSV4RRFFQ69G5FAV'
const entryId = 'ev_01ARZ3NDEKTSV4RRFFQ69G5FAV'
const base = {
  id: 'scr_one', environment_id: 'env_one', slug: 'prepare', service_id: 'svc_one', service: 'consumer',
  script: 'echo prepare', when: 'pre-deploy', order: 10, origin: 'api', active_generation: 1,
}
const execution = {
  mode: 'explicit', image, user: '0:0',
  volumes: [{ volume_id: volumeId, target: '/etc/tls', read_only: false }], entry_ids: [entryId],
}

// QA: SCRIPT-01, SCRIPT-05; local context decoding, not runner isolation.
// Rationale: API projection preserves complete explicit authority, including
// read-write access, and reports inherited mode without inventing grants.
test('Script Console projection preserves the complete execution context', () => {
  assert.deepEqual(scriptFromAPI({ ...base, execution }).execution, {
    mode: 'explicit', image, user: '0:0',
    volumes: [{ volumeId, target: '/etc/tls', readOnly: false }], entryIds: [entryId],
  })
  assert.deepEqual(scriptFromAPI({ ...base, execution: { mode: 'inherited' } }).execution, { mode: 'inherited' })
})

// QA: SCRIPT-06, UI-05; local malformed-response rejection, not host denial.
// Rationale: the Console must not display ambiguous/malformed access as a valid
// execution choice, even when a response bypassed normal Controller validation.
test('Script Console projection rejects malformed contexts and missing decisions', () => {
  for (const invalid of [
    undefined, null, {}, { mode: 'other' }, { mode: 'inherited', image: '' },
    { ...execution, image: 'setup:latest' }, { ...execution, user: 'root' },
    { ...execution, volumes: null }, { ...execution, entry_ids: null },
    { ...execution, volumes: [{ volume_id: volumeId, target: '/etc/tls' }] },
    { ...execution, volumes: [{ volume_id: volumeId, target: '/etc/tls', read_only: 'false' }] },
    { ...execution, volumes: [{ volume_id: volumeId, target: '/bin', read_only: false }] },
    { ...execution, volumes: [{ volume_id: volumeId, target: '/etc/tls', read_only: false, extra: true }] },
    { ...execution, entry_ids: [entryId, entryId] },
    { ...execution, entry_ids: [null] }, { ...execution, network: 'consumer' },
  ]) assert.throws(() => scriptFromAPI({ ...base, execution: invalid }), /execution/i)
})

// QA: SCRIPT-01; local request encoding, not persisted replacement semantics.
// Rationale: store requests carry complete replacements; patch omission differs
// from a deliberate reset and an explicit context with no resource grants.
test('Console Script mutation projection preserves context replacement and omission', () => {
  const authored = {
    slug: 'prepare', service: 'consumer', body: 'echo prepare', when: 'pre-deploy', order: 0,
    execution: { mode: 'explicit', image, user: '0:0', volumes: [{ volumeId, target: '/etc/tls', readOnly: false }], entryIds: [entryId] },
  }
  assert.deepEqual(scriptCreateToAPI('env_one', 'svc_one', authored), {
    environment_id: 'env_one', service_id: 'svc_one', slug: 'prepare', script: 'echo prepare', when: 'pre-deploy', order: 0, execution,
  })
  assert.deepEqual(scriptPatchToAPI({ order: 0 }), { order: 0 })
  assert.deepEqual(scriptPatchToAPI({ execution: { mode: 'inherited' } }), { execution: { mode: 'inherited' } })
  assert.deepEqual(scriptPatchToAPI({ execution: { ...authored.execution, volumes: [], entryIds: [] } }), {
    execution: { mode: 'explicit', image, user: '0:0', volumes: [], entry_ids: [] },
  })
})
