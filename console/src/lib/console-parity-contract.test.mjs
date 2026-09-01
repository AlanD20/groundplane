import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const storeSource = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')
const taskRunnerSource = await readFile(
  new URL('../components/common/task-runner-dialog.tsx', import.meta.url),
  'utf8',
)
const serviceFormSource = await readFile(
  new URL('../components/common/service-form-body.tsx', import.meta.url),
  'utf8',
)

test('service form only exposes fields supported by the Service API', () => {
  assert.doesNotMatch(serviceFormSource, /newId\(|envKvs|Environment \(KEY=value\)/)
  assert.doesNotMatch(serviceFormSource, /Env files \(secrets attached to this service only\)/)
  assert.doesNotMatch(serviceFormSource, /environment:\s*envKvs|envFiles:\s*envFiles/)
})

test('task-backed Console actions enforce durable task responses', () => {
  assert.match(storeSource, /function requireTaskId\(response: \{ task_id\?: string \| null \}, operation: string\)/)
  assert.match(storeSource, /runBackup = useCallback\(async \(environmentId: string\): Promise<string> => \{\s*assertEnvironmentMutable\(environmentId, 'Backup run'\)/)
  assert.match(storeSource, /runServiceRuntimeAction:[\s\S]*?const taskId = requireTaskId\(accepted, `Service \$\{action\}`\)/)
  assert.match(storeSource, /runScript:[\s\S]*?return requireTaskId\(accepted, 'Script run'\)/)
  assert.match(storeSource, /runBackingRuntimeAction:[\s\S]*?return requireTaskId\(accepted, `Backing service \$\{action\}`\)/)
  assert.match(storeSource, /requireTaskId\(created, 'Backing service creation'\)/)
})

test('component config no-op responses stay honest in task UI', () => {
  assert.match(storeSource, /updateComponentConfig: \(componentId: string, config: ComponentConfigInput\) => Promise<string \| null>/)
  assert.match(storeSource, /return result\.reconcile_task_id/)
  assert.match(taskRunnerSource, /onDispatch: \(\) => Promise<string \| null>/)
  assert.match(taskRunnerSource, /setTaskStatus\('not_required'\)/)
  assert.match(taskRunnerSource, /No task required/)
})
