import assert from 'node:assert/strict'
import test from 'node:test'

import {
  assertOptionalBackupPolicyKeep,
  isValidBackupPolicyKeep,
} from './backup-policy-contract.ts'

// QA: BAK-01, UI-03; local numeric validation, not policy persistence or retention.
// Rationale: input validation accepts the exact shared public interval and no
// unsafe or fractional JavaScript number.
test('Backup Policy keep accepts the exact public boundaries', () => {
  assert.equal(isValidBackupPolicyKeep(0), false)
  assert.equal(isValidBackupPolicyKeep(1), true)
  assert.equal(isValidBackupPolicyKeep(9007199254740991), true)
  assert.equal(isValidBackupPolicyKeep(9007199254740992), false)
  assert.equal(isValidBackupPolicyKeep(1.5), false)
})

// QA: BAK-01, UI-05; local response validation, not Controller behavior.
// Rationale: Controller responses are rejected before unsafe numeric values
// can enter the Console store and be rounded silently.
test('Backup Policy response validation rejects unsafe integers', () => {
  assert.doesNotThrow(() =>
    assertOptionalBackupPolicyKeep(9007199254740991, 'response keep'),
  )
  assert.throws(
    () => assertOptionalBackupPolicyKeep(9007199254740992, 'response keep'),
    /must be an integer between 1 and 9007199254740991/,
  )
})
