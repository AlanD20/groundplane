import assert from 'node:assert/strict'
import test from 'node:test'

import { applyAuthoritativeEnvironmentScalars } from './environment-authoritative.ts'

test('Environment edit applies every authoritative scalar and preserves hydrated children', () => {
  const zones = [{ id: 'net_existing' }]
  const services = [{ id: 'svc_existing' }]
  const routes = [{ id: 'rte_existing' }]
  const entries = [{ id: 'ent_existing' }]
  const current = {
    id: 'env_old',
    projectId: 'prj_old',
    name: 'old-name',
    networkPool: '10.40.0.0/16',
    networkCapacity: { totalAddresses: 65536, allocatedAddresses: 256, availableAddresses: 65280, zoneCount: 1 },
    status: 'pending',
    provisioningState: 'provisioning',
    createTaskId: 'task_old',
    volumeDir: '/old',
    zones,
    services,
    routes,
    entries,
    release: 'sha-existing',
  }
  const authoritative = {
    id: 'env_authoritative',
    projectId: 'prj_authoritative',
    name: 'authoritative-name',
    networkPool: '10.40.0.0/15',
    networkCapacity: { totalAddresses: 131072, allocatedAddresses: 512, availableAddresses: 130560, zoneCount: 2 },
    status: 'healthy',
    provisioningState: 'ready',
    createTaskId: null,
    volumeDir: '/authoritative',
  }

  const result = applyAuthoritativeEnvironmentScalars(current, authoritative)

  for (const [field, value] of Object.entries(authoritative)) assert.equal(result[field], value)
  assert.strictEqual(result.zones, zones)
  assert.strictEqual(result.services, services)
  assert.strictEqual(result.routes, routes)
  assert.strictEqual(result.entries, entries)
  assert.equal(result.release, 'sha-existing')
})
