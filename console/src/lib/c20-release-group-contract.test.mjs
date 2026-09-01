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
  assert.match(operationsSource, /onDispatch=\{\(\) => store\.rollbackReleaseGroup/)
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
