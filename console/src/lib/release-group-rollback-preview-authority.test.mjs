import assert from 'node:assert/strict'
import test from 'node:test'
import {
  acceptRollbackPreview,
  beginRollbackPreviewRequest,
  isRollbackPreviewAccepted,
  isRollbackPreviewRequestCurrent,
  requireRollbackPreview,
  rollbackPreviewScope,
} from '../features/release-group/release-group-rollback-preview-authority.ts'

const response = {
  release_group_id: 'group-a',
  revision: '9223372036854775807',
  sources: [{ service_id: 'service-a', release_id: 'release-a', tag: ' tag-exact ' }],
}

test('accepted rollback previews are immutable and match their exact authority scope', () => {
  const scope = rollbackPreviewScope('env-a', 'group-a', ' tag-exact ', 7)
  const accepted = acceptRollbackPreview(scope, response)

  assert.equal(accepted.response.revision, '9223372036854775807')
  assert.equal(accepted.response.sources[0].tag, ' tag-exact ')
  assert.ok(Object.isFrozen(accepted))
  assert.ok(Object.isFrozen(accepted.scope))
  assert.ok(Object.isFrozen(accepted.response.sources[0]))
  assert.equal(isRollbackPreviewAccepted(accepted, scope), true)
  assert.equal(isRollbackPreviewAccepted(accepted, rollbackPreviewScope('env-b', 'group-a', ' tag-exact ', 7)), false)
  assert.equal(isRollbackPreviewAccepted(accepted, rollbackPreviewScope('env-a', 'group-b', ' tag-exact ', 7)), false)
  assert.equal(isRollbackPreviewAccepted(accepted, rollbackPreviewScope('env-a', 'group-a', undefined, 7)), false)
  assert.equal(isRollbackPreviewAccepted(accepted, rollbackPreviewScope('env-a', 'group-a', '', 7)), false)
  assert.equal(isRollbackPreviewAccepted(accepted, rollbackPreviewScope('env-a', 'group-a', ' tag-exact ', 8)), false)
})

test('dispatch authority rejects the wrong scope without calling the mutation', () => {
  const accepted = acceptRollbackPreview(rollbackPreviewScope('env-a', 'group-a', undefined, 3), response)
  let mutationCalls = 0
  const dispatch = (scope) => {
    const current = requireRollbackPreview(accepted, scope)
    mutationCalls += 1
    return current.response.revision
  }

  assert.throws(() => dispatch(rollbackPreviewScope('env-a', 'group-b', undefined, 3)), /current rollback preview/)
  assert.equal(mutationCalls, 0)
  assert.equal(dispatch(rollbackPreviewScope('env-a', 'group-a', undefined, 3)), '9223372036854775807')
  assert.equal(mutationCalls, 1)
})

test('late deferred preview results cannot regain ownership after scope or generation changes', async () => {
  const requestedScope = rollbackPreviewScope('env-a', 'group-a', 'v1', 1)
  const request = beginRollbackPreviewRequest(requestedScope, 10)
  let resolve
  const deferred = new Promise((done) => { resolve = done })

  const settled = deferred.then((value) => ({
    ownedAfterEditBack: isRollbackPreviewRequestCurrent(request, requestedScope, 11),
    ownedAfterReopen: isRollbackPreviewRequestCurrent(request, rollbackPreviewScope('env-a', 'group-a', 'v1', 2), 10),
    value,
  }))
  resolve(response)

  assert.deepEqual(await settled, {
    ownedAfterEditBack: false,
    ownedAfterReopen: false,
    value: response,
  })

  let reject
  const deferredError = new Promise((_resolve, fail) => { reject = fail })
  const ignoredError = deferredError.catch((error) => ({
    ownedAfterReopen: isRollbackPreviewRequestCurrent(request, rollbackPreviewScope('env-a', 'group-a', 'v1', 2), 10),
    message: error.message,
  }))
  reject(new Error('late failure'))
  assert.deepEqual(await ignoredError, { ownedAfterReopen: false, message: 'late failure' })
})
