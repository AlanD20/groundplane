import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const sources = Promise.all([
  readFile(new URL('./environment-lifecycle.ts', import.meta.url), 'utf8'),
  readFile(new URL('./store.tsx', import.meta.url), 'utf8'),
  readFile(new URL('./environment-hydration.ts', import.meta.url), 'utf8'),
  readFile(new URL('./environment-task-observation.ts', import.meta.url), 'utf8'),
  readFile(new URL('../features/environment/environment-page.tsx', import.meta.url), 'utf8'),
])

test('deletion dispatch fences the Store before the request and closes open portals', async () => {
  const [lifecycle, store, , , page] = await sources
  assert.match(lifecycle, /persistPendingResourceRemovalIntents\(resourceRemovalIntents\.current\)\s+update\(/)
  assert.match(store, /assertEnvironmentMutable\(envId, 'Environment edit'\)/)
  assert.match(store, /assertEnvironmentMutable\(envId, 'Service mutation'\)/)
  assert.match(page, /DeployControls key=\{deletionInProgress \? 'deletion-fenced' : 'editable'\}/)
  assert.match(page, /fieldset key=\{deletionInProgress \? 'deletion-fenced' : 'editable'\}/)
})

test('retry observes the generation after the Controller accepts it', async () => {
  const [lifecycle] = await sources
  assert.match(lifecycle, /const acceptedRemoval: PendingResourceRemoval = \{ \.\.\.removal, generation: acceptedGeneration \}/)
  assert.match(lifecycle, /resourceRemovalTasks\.current\.set\(resourceRemovalKey\(acceptedRemoval\), accepted\.task_id\)/)
})

test('backing project hydration merges against Environment epochs', async () => {
  const [lifecycle, store, hydration] = await sources
  assert.match(store, /draft\.backingProjects = mergeEnvironmentProjectLoads\(/)
  assert.match(hydration, /shouldPreserve\(environment\.id, snapshotGenerations\.get\(environment\.id\) \?\? 0\)/)
  assert.match(hydration, /deletedEnvironmentIds/)
  assert.match(lifecycle, /shouldPreserveEnvironmentOnLoad/)
})

test('failed create hydrates the authoritative failed Environment projection', async () => {
  const [, store] = await sources
  assert.match(store, /const generation = nextEnvironmentGeneration\(created\.id, 'create'\)/)
  assert.match(store, /if \(created\.provisioningState === 'failed'\)/)
  assert.match(store, /settleEnvironmentMutation\(created\.id, generation, 'failed'\)/)
})

test('create task observation is abortable and provider-scoped', async () => {
  const [, store, , taskObservation] = await sources
  assert.match(taskObservation, /observeEnvironmentTask<Task extends TaskLike>/)
  assert.match(taskObservation, /requestTask\(taskId, signal\)/)
  assert.match(taskObservation, /waitForDelay\(delayMs, signal\)/)
  assert.match(store, /observeEnvironmentTask\(requestEnvironmentTask, accepted\.task_id, providerActive, observationController\.signal\)/)
  assert.match(store, /for \(const controller of environmentTaskControllers\.current\) controller\.abort\(\)/)
})
