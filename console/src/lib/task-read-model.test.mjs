import assert from 'node:assert/strict'
import test from 'node:test'
import { taskTypeFromAPI } from './task-read-model.ts'

// Rationale: the Console must reject a widened Task vocabulary rather than
// silently rendering an operation the public contract does not define.
test('Task decoding rejects an unknown type', () => {
  assert.throws(() => taskTypeFromAPI('unknown', 'system'), /unknown Task type/)
})

// Rationale: backup_prune is system-owned internal maintenance, so malformed
// operator provenance must fail before the Task reaches a Console surface.
test('Task decoding rejects operator-owned backup_prune', () => {
  assert.throws(() => taskTypeFromAPI('backup_prune', 'operator'), /non-system actor/)
  assert.equal(taskTypeFromAPI('backup_prune', 'system'), 'backup_prune')
})
