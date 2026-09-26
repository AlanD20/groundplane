import assert from 'node:assert/strict'
import test from 'node:test'
import { BlueprintApplyIntent } from '../features/blueprint/apply-intent.ts'

function storage() {
  let value = null
  return {
    getItem: () => value,
    setItem: (_key, next) => { value = next },
    removeItem: () => { value = null },
  }
}

function request() {
  const bytes = Uint8Array.from([0, 1, 2, 255, 45, 45, 13, 10])
  return {
    manifest: {
      root: 'blueprint.yaml', compose_sources: ['blueprint.yaml'], interpolation: {},
      files: [{ path: 'blueprint.yaml', part: 'file-000001', size: bytes.length, sha256: 'a'.repeat(64) }],
    },
    parts: [{ part: 'file-000001', content: new File([bytes], 'blueprint.yaml') }],
  }
}

// QA: UI-04/05. A lost 202 and a page reload must replay the exact protected
// Blueprint request; neither grants permission for a second publication.
test('Blueprint Apply retains exact bytes, revision and key across lost acceptance and reload', async () => {
  const saved = storage()
  const first = new BlueprintApplyIntent(saved)
  const source = request()
  const sendCalls = []
  let loseResponse = true
  const send = async (environmentId, replay, revision, key) => {
    sendCalls.push({ environmentId, revision, key, bytes: [...new Uint8Array(await replay.parts[0].content.arrayBuffer())] })
    if (loseResponse) { loseResponse = false; throw new Error('connection closed') }
    return { task_id: 'task_original' }
  }
  await first.begin('env_1', source, 'revision-1', 'apply-key-00000001')
  await assert.rejects(first.publish(send, () => false), /connection closed/)
  const resumed = new BlueprintApplyIntent(saved)
  assert.deepEqual(resumed.current, {
    environmentId: 'env_1', revision: 'revision-1', key: 'apply-key-00000001', taskId: undefined,
  })
  assert.equal(await resumed.publish(send, () => false), 'task_original')
  assert.deepEqual(sendCalls, [sendCalls[0], sendCalls[0]])
  assert.equal(new BlueprintApplyIntent(saved).current.taskId, 'task_original')
  assert.equal(Object.hasOwn(JSON.parse(saved.getItem()), 'parts'), false)
  assert.equal(Object.hasOwn(JSON.parse(saved.getItem()), 'manifest'), false)
  assert.throws(() => resumed.begin('env_1', source, 'revision-2', 'different-key-0001'), /unresolved/)
  resumed.settle('another_task')
  assert.equal(resumed.current.taskId, 'task_original')
  resumed.settle('task_original')
  assert.equal(new BlueprintApplyIntent(saved).current, null)
})

// QA: UI-04. Definite rejection permits an explicit new Apply; uncertain or
// malformed acceptance keeps the original authority, and storage must succeed
// before the network is touched.
test('Blueprint Apply separates definite rejection from uncertain publication', async () => {
  const saved = storage()
  const intent = new BlueprintApplyIntent(saved)
  await intent.begin('env_1', request(), 'revision-1', 'apply-key-00000001')
  await assert.rejects(intent.publish(async () => { throw new Error('rejected') }, () => true))
  assert.equal(intent.current, null)
  await intent.begin('env_1', request(), 'revision-1', 'apply-key-00000002')
  await assert.rejects(intent.publish(async () => ({}), () => false), /task_id/)
  assert.equal(intent.current.key, 'apply-key-00000002')
  const unavailable = new BlueprintApplyIntent({ ...storage(), setItem() { throw new Error('storage blocked') } })
  await assert.rejects(unavailable.begin('env_1', request(), 'revision-1', 'apply-key-00000003'), /storage blocked/)
  assert.equal(unavailable.current, null)
})
