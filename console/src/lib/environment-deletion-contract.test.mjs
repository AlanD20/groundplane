import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

test('Environment create, rename, and delete use authoritative task and stable-id contracts', async () => {
  const [store, lifecycle, page, platform, storage, fence, ingress] = await Promise.all([
    readFile(new URL('./store.tsx', import.meta.url), 'utf8'),
    readFile(new URL('./environment-lifecycle.ts', import.meta.url), 'utf8'),
    readFile(new URL('../features/environment/environment-page.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../features/platform-component/platform-component-page.tsx', import.meta.url), 'utf8'),
    readFile(new URL('./environment-storage.ts', import.meta.url), 'utf8'),
    readFile(new URL('./environment-deletion-fence.tsx', import.meta.url), 'utf8'),
    readFile(new URL('./environment-platform-ingress.ts', import.meta.url), 'utf8'),
  ])
  assert.match(store, /observeEnvironmentTask\(requestEnvironmentTask, accepted\.task_id, providerActive, observationController\.signal\)/)
  assert.match(store, /idempotencyKey: intent\.idempotencyKey/)
  assert.match(store, /loadEnvironmentMutationIntents/)
  assert.match(store, /environment\.id === task\.target && environment\.projectId === projectId/)
  assert.doesNotMatch(store, /environment\.createTaskId === accepted\.task_id/)
  assert.match(storage, /groundplane-pending-environment-removals/)
  assert.match(store, /task\.status !== 'completed'/)
  assert.match(lifecycle, /environmentGenerations\.current\.get\(removal\.resourceId\)/)
  assert.match(lifecycle, /active\.current = true/)
  assert.match(lifecycle, /active\.current = false/)
  assert.match(lifecycle, /Authoritative Environment list still contains/)
  assert.match(lifecycle, /persistPendingResourceRemovalIntents/)
  assert.match(storage, /loadEnvironmentDeletionFailures/)
  assert.match(storage, /!Array\.isArray\(entry\) \|\| entry\.length !== 2/)
  assert.match(storage, /resourceRemovalKey\(removal\) !== key/)
  assert.match(lifecycle, /task\.type !== 'remove'/)
  assert.match(lifecycle, /environmentMutationOutcomes/)
  assert.match(storage, /loadResourceRemovalRetryIntents/)
  assert.match(store, /provisioningState === 'failed'/)
  assert.match(store, /edited\.id !== envId \|\| edited\.projectId !== project\.id/)
  assert.match(store, /renamed\.id !== envId \|\| renamed\.projectId !== project\.id/)
  assert.match(page, /navigate\(`\/t\/\$\{params\.tenant\}\/\$\{params\.project\}\/\$\{encodeURIComponent\(renamed\.name\)\}`\)/)
  assert.match(page, /getEnvironmentDeletionFailure/)
  assert.match(page, /await store\.deleteEnvironment\(env\.id\)/)
  assert.match(page, /fieldset key=\{deletionInProgress \? 'deletion-fenced' : 'editable'\} disabled=\{deletionInProgress\}/)
  assert.match(page, /routerReady/)
  assert.match(fence, /child mutations are disabled/)
  assert.match(platform, /environmentPlatformIngress/)
  assert.match(ingress, /component\.kind === 'caddy'/)
  assert.match(ingress, /component\.kind === 'cloudflare-tunnel'/)
})
