import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

test('environment release summaries come from the live release ledger', async () => {
  const store = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')

  assert.match(store, /function projectReleaseSummary\(deploys: DeployRecord\[\]\)/)
  assert.match(store, /release: activeTags\.length === 0 \? 'none' : activeTags\.length === 1 \? activeTags\[0\] : 'mixed'/)
  assert.match(store, /return \{ \.\.\.environment, \.\.\.projectReleaseSummary\(deploys\),/)
  assert.match(store, /Object\.assign\(current, projectReleaseSummary\(deploys\)\)/)
  assert.match(store, /'\/backing-services',\s*201,\s*\{ method: 'POST', body: input \}/s)
  assert.doesNotMatch(store, /'\/backing-services',\s*201,\s*\{ method: 'POST', body: JSON\.stringify\(input\) \}/s)
  assert.doesNotMatch(store, /function applyServiceRelease\(/)
})
