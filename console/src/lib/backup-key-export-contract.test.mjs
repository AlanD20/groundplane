import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const storePath = new URL('./store.tsx', import.meta.url)
const pagePath = new URL('../features/environment/environment-page.tsx', import.meta.url)

// Rationale: browser export must request no-store bytes and release every
// transient download handle after success, failure, or cancellation.
test('backup key export is no-store and destroys transient browser state', async () => {
  const source = await readFile(storePath, 'utf8')
  assert.match(source, /cache:\s*['"]no-store['"]/)
  assert.match(source, /signal,/)
  assert.match(source, /finally\s*{[\s\S]*URL\.revokeObjectURL/)
  assert.match(source, /response\s*=\s*null/)
  assert.match(source, /blob\s*=\s*null/)
  assert.match(source, /objectURL\s*=\s*null/)
  assert.doesNotMatch(source, /useState<(?:Blob|Response|ArrayBuffer|Uint8Array)>/)
})

// Rationale: closing the Environment surface must abort the private-key
// download and avoid surfacing an expected cancellation as an operator error.
test('environment page aborts export on close or unmount and ignores abort errors', async () => {
  const source = await readFile(pagePath, 'utf8')
  assert.match(source, /useRef<AbortController/)
  assert.match(source, /keyExportController\.current\?\.abort\(\)/)
  assert.match(source, /controller\.signal/)
  assert.match(source, /if \(!controller\.signal\.aborted\)/)
})

// Rationale: rotation must derive TaskAccepted from the generated operation and
// must not recreate the wire response with a handwritten task_id shape.
test('backup key rotation uses the generated operation response type', async () => {
  const source = await readFile(storePath, 'utf8')
  assert.match(
    source,
    /type BackupKeyRotateResponse = operations\['backup\.key\.rotate'\]\['responses'\]\[202\]\['content'\]\['application\/json'\]/,
  )
  assert.match(source, /tenantRequest<BackupKeyRotateResponse>/)
  assert.doesNotMatch(source, /tenantRequest<\{\s*task_id:\s*string\s*\}>/)
})
