import assert from 'node:assert/strict'
import test from 'node:test'
import { backingAuthenticationCreateFields } from './valkey-authentication.ts'

// QA: BACK-05, UI-03; local request fields, not form selection or protocol auth.
// Rationale: no Valkey mode may be defaulted; PostgreSQL must not inherit stale
// Valkey-only fields after the operator changes adapters.
test('backing creation requires an explicit valid Valkey mode and omits authentication for PostgreSQL', () => {
  assert.equal(backingAuthenticationCreateFields('valkey', ''), undefined)
  assert.equal(backingAuthenticationCreateFields('valkey', 'invalid'), undefined)
  for (const authentication of ['username_password', 'password', 'none']) {
    assert.deepEqual(backingAuthenticationCreateFields('valkey', authentication), {
      adapter: 'valkey',
      authentication,
    })
  }
  assert.deepEqual(backingAuthenticationCreateFields('postgres', ''), { adapter: 'postgres' })
  assert.deepEqual(backingAuthenticationCreateFields('postgres', 'username_password'), { adapter: 'postgres' })
})
