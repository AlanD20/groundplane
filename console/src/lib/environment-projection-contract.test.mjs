import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

test('API Environment projection preserves authoritative failed health and does not invent components', async () => {
  const source = await readFile(new URL('./environment-projection.ts', import.meta.url), 'utf8')
  assert.match(source, /provisioningState === 'failed' \? 'failed'/)
  assert.match(source, /components: \[\]/)
  assert.doesNotMatch(source, /createEnvironmentComponents/)
  assert.doesNotMatch(source, /newId\('cmp'/)
})
