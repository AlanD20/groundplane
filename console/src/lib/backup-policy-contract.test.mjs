import assert from 'node:assert/strict'
import test from 'node:test'
import { readFile } from 'node:fs/promises'

import {
  MAXIMUM_BACKUP_POLICY_KEEP,
  assertOptionalBackupPolicyKeep,
  isValidBackupPolicyKeep,
} from './backup-policy-contract.ts'

// Rationale: input validation accepts the exact shared public interval and no
// unsafe or fractional JavaScript number.
test('Backup Policy keep accepts the exact public boundaries', () => {
  assert.equal(isValidBackupPolicyKeep(0), false)
  assert.equal(isValidBackupPolicyKeep(1), true)
  assert.equal(isValidBackupPolicyKeep(MAXIMUM_BACKUP_POLICY_KEEP), true)
  assert.equal(isValidBackupPolicyKeep(MAXIMUM_BACKUP_POLICY_KEEP + 1), false)
  assert.equal(isValidBackupPolicyKeep(1.5), false)
})

// Rationale: Controller responses are rejected before unsafe numeric values
// can enter the Console store and be rounded silently.
test('Backup Policy response validation rejects unsafe integers', () => {
  assert.doesNotThrow(() =>
    assertOptionalBackupPolicyKeep(MAXIMUM_BACKUP_POLICY_KEEP, 'response keep'),
  )
  assert.throws(
    () => assertOptionalBackupPolicyKeep(MAXIMUM_BACKUP_POLICY_KEEP + 1, 'response keep'),
    /must be an integer between 1 and 9007199254740991/,
  )
})


// Rationale: Recovery Point reads have one documented cursor query; the
// Console must not invent a page-limit contract or emit an empty query string.
test('Recovery Point loads send only the documented cursor query', async () => {
  const store = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')
  const start = store.indexOf('const loadRecoveryPoints = useCallback')
  assert.notEqual(start, -1)
  const recoveryPointLoad = store.slice(start, start + 2500)
  assert.match(recoveryPointLoad, /const query = new URLSearchParams\(\)/)
  assert.match(recoveryPointLoad, /if \(cursor\) query\.set\('cursor', cursor\)/)
  assert.match(recoveryPointLoad, /const suffix = query\.size === 0 \? '' : `\?\$\{query\}`/)
  assert.doesNotMatch(recoveryPointLoad, /new URLSearchParams\(\{ limit: '50' \}\)/)
})


// Rationale: a defensive client must preserve continuation even if a valid
// page contains no visible items, so hidden server-side records cannot strand it.
test("Recovery Point continuation is not nested under the non-empty guard", async () => {
  const page = await readFile(
    new URL("../features/environment/environment-page.tsx", import.meta.url),
    "utf8",
  )
  const start = page.indexOf("Recovery Points")
  assert.notEqual(start, -1)
  const recoveryPoints = page.slice(start, start + 9000)
  assert.match(
    recoveryPoints,
    /{points\.items\.length > 0 && \(\s*<div className="overflow-x-auto[\s\S]*?<\/div>\s*\)}\s*{points\.nextCursor && \(/,
  )
})


// Rationale: C16 remains scaffolded overall, but its landed Recovery Point read
// slice must not regress to the stale claim that all point surfaces are pending.
test("C16 ledger records the landed Recovery Point read slice", async () => {
  const capabilities = await readFile(
    new URL("../../../docs/capabilities.md", import.meta.url),
    "utf8",
  )
  const c16 = capabilities.split("\n").find((line) => line.startsWith("| C16 |"))
  assert.ok(c16)
  assert.match(c16, /Verified Recovery Point listing is now production-composed/)
  assert.doesNotMatch(c16, /Console run, points, restore/)
  assert.doesNotMatch(c16, /Recovery Point API\/CLI\/Console surfaces/)
})
