import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const page = await readFile(
  new URL('../features/environment/environment-page.tsx', import.meta.url),
  'utf8',
)

test('desired Blueprint copies only keyed Blueprint-origin Scripts', () => {
  const start = page.indexOf("'x-gp-scripts': Object.fromEntries(")
  assert.notEqual(start, -1)
  const scripts = page.slice(start, start + 600)
  assert.match(scripts, /script\.origin === 'blueprint'/)
  assert.match(scripts, /Boolean\(script\.reconciliationKey\)/)
  assert.match(scripts, /\[script\.reconciliationKey!/)
  assert.doesNotMatch(scripts, /reconciliationKey \?\? .*slug/)
})
