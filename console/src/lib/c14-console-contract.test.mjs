import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const read = (path) => readFile(new URL(path, import.meta.url), 'utf8')
const [store, types, ownerList, projectPage, platformPage, taskNavigation, fixtures] = await Promise.all(['./store.tsx', './types.ts', '../features/secrets/reusable-secret-owner-list.tsx', '../features/secrets/project-secrets-page.tsx', '../routes/platform/secrets/page.tsx', './task-navigation.ts', './mock-data.ts'].map(read))

// Rationale: Protects the project-versus-platform owner discriminant and API scope validation.
test('reusable Secret owners are discriminated and API-checked', () => { assert.match(types, /scope: 'project'[\s\S]*projectId: string/); assert.match(types, /scope: 'platform'[\s\S]*projectId\?: never/); assert.match(store, /secret\.scope !== 'project' \|\| secret\.project_id !== expectedScope\.projectId/); assert.match(store, /secret\.scope !== 'platform' \|\| secret\.project_id !== undefined/) })
// Rationale: Protects the invariant that Secret removal executes only the generated authoritative Task.
test('removal follows the generated authoritative Task without local steps', () => { assert.match(store, /operations\['secret\.remove'\]/); assert.doesNotMatch(store, /SecretRemovalCoordinator|secretRemovalProjections|reconcileReusableSecretRemoval/); assert.match(ownerList, /TaskRunnerDialog[\s\S]*steps=\{\[\]\}[\s\S]*onDispatch=.*onRemove\(confirmation\)[\s\S]*onCommit=.*onRefresh/s); assert.doesNotMatch(ownerList, /Reconcile existing materializations|Remove reusable Secret metadata/) })
// Rationale: Protects the no-plaintext-at-rest invariant across fixtures, drawers, and reveal flows.
test('plaintext is neither fixture-backed nor retained by drawers', () => { assert.doesNotMatch(fixtures, /platformSecrets|SENTRY_DSN|REVERB_SECRET/); for (const page of [projectPage, platformPage]) { assert.match(page, /if \(!next\) clearDraft\(\)/); assert.match(page, /catch \(cause\) \{\s*setValue\(''\)/) }; assert.match(ownerList, /<RevealValue loadValue=\{\(\) => onReveal\(secret\)\}/) })
// Rationale: Protects immutable Secret owner validation during Task navigation.
test('Task navigation validates immutable Secret owners', () => { assert.match(taskNavigation, /secret\?\.scope === 'platform'[\s\S]*task\.workspaceType !== 'platform'/); assert.match(taskNavigation, /secret\?\.scope === 'project'[\s\S]*task\.tenantId !== project\.tenantId[\s\S]*task\.projectId !== secret\.projectId/) })
