import assert from 'node:assert/strict'
import test from 'node:test'
import { entryFromAPI } from './entry-api.ts'

// Rationale: Script selection uses immutable Entry identity metadata, never a
// secret value or a file/variable label guessed to be its Blueprint key.
test('Entry projection retains reconciliation identity and existing metadata', () => {
  const entry = entryFromAPI({
    id: 'ev_one', type: 'file', path: '/etc/tls/seed', uid: 0, gid: 0,
    reconciliation_key: 'tls-seed', secret: true, source: { kind: 'secret_ref', secret_ref: 'sec_one' }, exposure: ['consumer'],
  })
  assert.equal(entry.reconciliationKey, 'tls-seed')
  assert.equal(entry.uid, 0)
  assert.equal(entry.gid, 0)
  assert.deepEqual(entry.source, { kind: 'secret_ref', secretRef: 'sec_one' })
  assert.deepEqual(entry.exposure, ['consumer'])
})
