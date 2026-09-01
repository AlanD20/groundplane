import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const lifecycle = await readFile(new URL('./environment-lifecycle.ts', import.meta.url), 'utf8')
const removalState = await readFile(new URL('./environment-removal-state.ts', import.meta.url), 'utf8')
const store = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')

test('stale loads cannot revive a locally successful Environment deletion', () => {
  assert.match(lifecycle, /successfulEnvironmentDeletions = useRef\(new Map<string, string>\(\)\)/)
  assert.match(lifecycle, /if \(successfulEnvironmentDeletions\.current\.has\(environmentId\)\) return false/)
  assert.match(lifecycle, /if \(successfulEnvironmentDeletions\.current\.has\(environment\.id\)\) continue/)
  assert.match(lifecycle, /successfulEnvironmentDeletions\.current\.set\(removal\.resourceId, taskId\)/)
})

test('a missing authoritative deletion Task stops monitoring with recovery state', () => {
  assert.match(removalState, /function isAuthoritativeTaskUnavailable\(error: unknown\)/)
  assert.match(lifecycle, /Controller no longer has this Task; refresh the Environment to recover/)
  assert.match(lifecycle, /recordResourceRemovalError\(\s*taskId,\s*removal,\s*`Unable to observe deletion Task/)
})

test('a synchronous DELETE rejection clears its persisted fence', () => {
  assert.match(removalState, /function isDefinitiveRemovalRequestRejection\(error: unknown\)/)
  assert.match(lifecycle, /if \(isDefinitiveRemovalRequestRejection\(error\)\) \{/)
  assert.match(lifecycle, /resourceRemovalIntents\.current\.delete\(key\)/)
  assert.match(lifecycle, /persistPendingResourceRemovalIntents\(resourceRemovalIntents\.current\)/)
})

test('unknown DELETE outcomes retain the intent and replay the same idempotency key', () => {
  assert.match(lifecycle, /const intent = resourceRemovalIntents\.current\.get\(key\) \?\? \{/)
  assert.match(lifecycle, /deleteResource\(resource, trackedRemoval\.resourceId, intent\.idempotencyKey\)/)
  assert.match(lifecycle, /resourceRemovalIntents\.current\.set\(key, intent\)/)
  assert.match(store, /if \(!isEnvironmentDeletionPending\(envId\)\) assertEnvironmentMutable\(envId, 'Environment deletion'\)/)
})

test('startup recovery replays persisted no-task intents and enters Task observation', () => {
  assert.match(lifecycle, /pendingEnvironmentDeletionReplays\(resourceRemovalIntents\.current, resourceRemovalTasks\.current, pendingResourceRemovals\.current\)/)
  assert.match(lifecycle, /void dispatchResourceRemoval\(removal\)\.catch\(\(\) => undefined\)/)
  assert.match(lifecycle, /\[active, dispatchResourceRemoval, monitorResourceRemoval\]/)
  assert.match(lifecycle, /pendingResourceRemovals\.current\.set\(accepted\.task_id, trackedRemoval\)/)
  assert.match(lifecycle, /monitorResourceRemoval\(accepted\.task_id\)/)
})
