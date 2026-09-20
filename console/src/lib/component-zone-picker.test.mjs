import assert from 'node:assert/strict'
import test from 'node:test'

import {
  createAndSelectZone,
  mergeOrdinaryZones,
} from '../features/environment/component-zone-picker-state.ts'

const zone = (id, name, ownerKind = 'environment') => ({
  id,
  environmentId: 'env_test',
  name,
  subnet: id === 'net_frontend' ? '10.40.10.0/24' : '10.40.20.0/24',
  internal: false,
  ownerKind,
  ownerId: ownerKind === 'environment' ? 'env_test' : 'prj_backing',
})

// QA: NET-02, CMP-01; local callback and selection, not Controller allocation.
// Rationale: the create response is authoritative; the picker must select its
// exact stable id once and keep the returned Zone while parent hydration lags.
test('successful creation selects and retains the exact returned Zone', async () => {
  const created = zone('net_controller_returned', 'edge')
  const createInputs = []
  const changes = []

  const result = await createAndSelectZone(
    { name: 'edge', subnet: '10.40.20.0/24', internal: false },
    ['net_frontend'],
    async (input) => {
      createInputs.push(input)
      return created
    },
    (ids) => changes.push(ids),
  )

  assert.strictEqual(result, created)
  assert.deepEqual(createInputs, [{ name: 'edge', subnet: '10.40.20.0/24', internal: false }])
  assert.deepEqual(changes, [['net_frontend', 'net_controller_returned']])
  assert.deepEqual(
    mergeOrdinaryZones([zone('net_frontend', 'frontend')], [created]).map(({ id }) => id),
    ['net_frontend', 'net_controller_returned'],
  )
})

// QA: NET-02, UI-03; local rejected-request state, not subnet overlap detection.
// Rationale: a rejected Zone create must not manufacture an id, update the
// Component selection, or leave a phantom option in local picker state.
test('failed creation makes no selection and adds no phantom Zone', async () => {
  let createCalls = 0
  const changes = []

  await assert.rejects(
    createAndSelectZone(
      { name: 'edge', subnet: '10.40.20.0/24', internal: false },
      ['net_frontend'],
      async () => {
        createCalls += 1
        throw new Error('subnet overlaps an existing reservation')
      },
      (ids) => changes.push(ids),
    ),
    /subnet overlaps/,
  )

  assert.equal(createCalls, 1)
  assert.deepEqual(changes, [])
  assert.deepEqual(mergeOrdinaryZones([zone('net_frontend', 'frontend')], []), [
    zone('net_frontend', 'frontend'),
  ])
})

// QA: CMP-01, CMP-03; local option filtering, not API ownership enforcement.
// Rationale: Component Zone choices are ordinary Environment Zones, but their
// names and internal decisions are otherwise arbitrary and must not be inferred.
test('options exclude backing-owned Zones without filtering names or internal state', () => {
  const identity = { ...zone('net_identity', 'identity-private'), internal: true }
  const ordinaryEdge = zone('net_edge', 'edge')
  const backing = zone('net_backing', 'database', 'backing_project')

  assert.deepEqual(mergeOrdinaryZones([identity, ordinaryEdge, backing], []), [identity, ordinaryEdge])
})
