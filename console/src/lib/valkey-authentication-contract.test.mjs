import assert from 'node:assert/strict'
import test from 'node:test'
import { backingAuthenticationCreateFields } from './valkey-authentication.ts'

// QA: BACK-05, UI-03; local request fields, not form selection or protocol auth.
// Rationale: no Valkey mode may be defaulted; PostgreSQL must not inherit stale
// Valkey-only fields after the operator changes adapters.
test('backing creation requires an explicit valid Valkey mode and omits authentication for PostgreSQL', () => {
  assert.equal(backingAuthenticationCreateFields('valkey:9', ''), undefined)
  assert.equal(backingAuthenticationCreateFields('valkey:9', 'invalid'), undefined)
  for (const authentication of ['username_password', 'password', 'none']) {
    assert.deepEqual(backingAuthenticationCreateFields('valkey:9', authentication), {
      adapter: 'valkey:9',
      authentication,
    })
  }
  assert.deepEqual(backingAuthenticationCreateFields('postgres:16', ''), { adapter: 'postgres:16' })
  assert.deepEqual(backingAuthenticationCreateFields('postgres:16', 'username_password'), { adapter: 'postgres:16' })
})
