import assert from 'node:assert/strict'
import test from 'node:test'
import { ControllerUpdateIntent } from '../features/platform-controller/update-intent.ts'

const release = `sha256:${'a'.repeat(64)}`
const key = '01M2482EK5HAACSE0000000001'
function storage() {
  let value = null
  return { getItem: () => value, setItem: (_key, next) => { value = next }, removeItem: () => { value = null } }
}

// QA: UP-11, UI-04; local request-state behavior, not live activation.
// Rationale: lost acceptance and page reload must replay the same protected
// request; neither is permission to create a second native activation Task.
test('native update retains one release and key across uncertain acceptance and reload', async () => {
  const saved = storage()
  const first = new ControllerUpdateIntent(saved)
  const calls = []
  let loseResponse = true
  const send = async (body, idempotencyKey) => {
    calls.push([body, idempotencyKey])
    if (loseResponse) { loseResponse = false; throw new Error('disconnected') }
    return { task_id: 'task_update' }
  }
  first.begin(release, key)
  await assert.rejects(first.publish(send, () => false), /disconnected/)
  const resumed = new ControllerUpdateIntent(saved)
  assert.deepEqual(resumed.current, { release, idempotencyKey: key })
  assert.equal(await resumed.publish(send, () => false), 'task_update')
  assert.deepEqual(calls, [[release, key], [release, key]])
  assert.equal(new ControllerUpdateIntent(saved).current.taskId, 'task_update')
  assert.throws(() => resumed.begin(release, 'different'), /unresolved/)
  resumed.settle('other_task')
  assert.equal(resumed.current.taskId, 'task_update')
  resumed.settle('task_update')
  assert.equal(new ControllerUpdateIntent(saved).current, null)
})

// QA: UP-11, UI-04; local rejection/uncertainty behavior.
// Rationale: a definite rejection permits a new explicit attempt; missing
// storage or malformed acceptance cannot silently discard ambiguous authority.
test('native publication distinguishes rejection from uncertainty', async () => {
  const intent = new ControllerUpdateIntent(storage())
  intent.begin(release, key)
  await assert.rejects(intent.publish(async () => { throw new Error('rejected') }, () => true))
  assert.equal(intent.current, null)
  intent.begin(release, key)
  await assert.rejects(intent.publish(async () => ({}), () => false), /task_id/)
  assert.equal(intent.current.idempotencyKey, key)
  const unavailable = new ControllerUpdateIntent({ ...storage(), setItem() { throw new Error('storage blocked') } })
  assert.throws(() => unavailable.begin(release, key), /storage blocked/)
  assert.equal(unavailable.current, null)
})
