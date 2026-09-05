import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const storeSource = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')
const operationsSource = await readFile(
  new URL('../features/release-group/release-group-operation-dialogs.tsx', import.meta.url),
  'utf8',
)
const detailSource = await readFile(
  new URL('../features/release-group/release-group-detail-page.tsx', import.meta.url),
  'utf8',
)

test('C20 Release Group operations dispatch durable Controller Tasks', () => {
  assert.match(operationsSource, /onDispatch=\{\(\) => store\.deployReleaseGroup/)
  assert.match(operationsSource, /onDispatch=\{async \(\) =>/)
  assert.match(detailSource, /onDispatch=\{\(\) => store\.removeReleaseGroup/)
  assert.doesNotMatch(operationsSource, /onCommit=\{\(\) => store\.(?:deploy|rollback)ReleaseGroup/)
  assert.doesNotMatch(detailSource, /onCommit=\{async \(\) =>[^}]*removeReleaseGroup/s)
})

test('C20 successful Tasks refresh authoritative release projections', () => {
  assert.match(storeSource, /refreshEnvironmentReleases: \(environmentId: string, signal\?: AbortSignal\) => Promise<void>/)
  assert.match(storeSource, /const refreshEnvironmentReleases = useCallback\(async[\s\S]*listAllReleases\([\s\S]*listAllReleaseGroups\(/)
  assert.match(storeSource, /current\.deploys = deploys[\s\S]*current\.releaseGroups = releaseGroups[\s\S]*refreshReleaseGroupTags\(current\)/)
  assert.equal(operationsSource.match(/store\.refreshEnvironmentReleases\(env\.id\)/g)?.length, 2)
  assert.match(detailSource, /refreshEnvironmentReleases\(environment\.id\)[\s\S]*finally\(\(\) => navigate\(listPath/)
})

test('C20 Release Group rollback accepts an optional member tag override', () => {
  assert.match(operationsSource, /<Label htmlFor="release-group-rollback-tag">Image tag \(optional\)<\/Label>/)
  assert.match(operationsSource, /Leave blank to use each member's previous successful served release\./)
  assert.match(operationsSource, /const requestedScope = rollbackPreviewScope\(env\.id, group\.id, tag === '' \? undefined : tag, dialogSession\.current\)/)
  assert.match(operationsSource, /store\.previewReleaseGroupRollback\(request\.scope\.environmentId, request\.scope\.groupId, requestedTag\)/)
  assert.match(operationsSource, /store\.rollbackReleaseGroup\(env\.id, group\.id, tag === '' \? undefined : tag, authority\.response\.revision\)/)
  assert.doesNotMatch(operationsSource, /resolveReleaseGroupRollbackTargets|status === 'superseded'/)
  assert.match(storeSource, /previewReleaseGroupRollback: \(envId: string, groupId: string, tag\?: string\)/)
  assert.match(storeSource, /preview_revision: previewRevision/)
})

test('C20 Release Group rollback previews only on explicit request and presents authoritative source identities', () => {
  assert.match(operationsSource, /<Button[^>]*onClick=\{\(\) => \{ void loadPreview\(\) \}\}[^>]*>Preview<\/Button>/s)
  assert.doesNotMatch(operationsSource, /if \(!open\) return[\s\S]{0,300}previewReleaseGroupRollback/)
  assert.match(operationsSource, /requestGeneration\.current \+= 1/)
  assert.match(operationsSource, /isRollbackPreviewAccepted\(acceptedPreview, currentScope\)/)
  assert.match(operationsSource, /requireRollbackPreview\(acceptedPreview, dispatchScope\)/)
  assert.equal(operationsSource.match(/if \(!requestIsCurrent\(\)\) return/g)?.length, 2)
  assert.match(operationsSource, /<dt[^>]*>Service<\/dt>[\s\S]*source\.service_id/)
  assert.match(operationsSource, /<dt[^>]*>Release<\/dt>[\s\S]*source\.release_id/)
  assert.match(operationsSource, /<dt[^>]*>Tag<\/dt>[\s\S]*source\.tag/)
})
