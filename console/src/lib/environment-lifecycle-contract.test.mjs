import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

// Rationale: Environment rename and deletion are production Controller actions;
// the Console must not regress to fixture mutation or fabricated activity.
test('Environment lifecycle uses generated operations and stable identity paths', async () => {
  const [generated, store, page] = await Promise.all([
    readFile(new URL('./api.generated.ts', import.meta.url), 'utf8'),
    readFile(new URL('./store.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../features/environment/environment-page.tsx', import.meta.url), 'utf8'),
  ])
  assert.match(generated, /environment\.rename/)
  assert.match(generated, /environment\.delete/)
  assert.match(store, /EnvironmentRenameRequest/)
  assert.match(store, /dispatchResourceRemoval\(\{\s*kind: 'environment'/)
  assert.doesNotMatch(store, /if \(e\) e\.name = name/)
  assert.match(page, /secrets\/\.env\.\{env\.id\}/)
  assert.doesNotMatch(page, /Renamed environment .*store\.logActivity/)
})
