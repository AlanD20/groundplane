import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const page = await readFile(
  new URL('../features/environment/environment-page.tsx', import.meta.url),
  'utf8',
)
const workspace = await readFile(
  new URL('../features/blueprint/blueprint-workspace.tsx', import.meta.url),
  'utf8',
)

test('Blueprint workspace uses the canonical Controller authoring projection', () => {
  assert.match(page, /<BlueprintWorkspace/)
  assert.doesNotMatch(page, /'x-gp-scripts'/)
  assert.match(workspace, /store\.getBlueprint\(/)
  assert.match(workspace, /store\.validateBlueprint\(/)
  assert.match(workspace, /store\.applyBlueprint\(/)
})
