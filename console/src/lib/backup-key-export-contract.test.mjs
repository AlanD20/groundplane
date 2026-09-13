import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const storePath = new URL('./store.tsx', import.meta.url)

// Delivery: generated wire-type ownership only; not BAK-14 execution or key safety.
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
