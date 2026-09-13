import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'
import { backingAuthenticationCreateFields } from './valkey-authentication.ts'

const typesSource = await readFile(new URL('./types.ts', import.meta.url), 'utf8')
const storeSource = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')
const fixturesSource = await readFile(new URL('./mock-data.ts', import.meta.url), 'utf8')
const listSource = await readFile(new URL('../routes/platform/backing-services/page.tsx', import.meta.url), 'utf8')
const detailSource = await readFile(new URL('../features/backing-service/backing-service-page.tsx', import.meta.url), 'utf8')
const environmentSource = await readFile(new URL('../features/environment/environment-page.tsx', import.meta.url), 'utf8')

test('Valkey authentication is projected from the backing facade into Console service state', () => {
  assert.match(typesSource, /export type ValkeyAuthentication = 'username_password' \| 'password' \| 'none'/)
  assert.match(typesSource, /authentication\?: string/)
  assert.match(storeSource, /authentication: facade\.authentication/)
  assert.match(fixturesSource, /adapter: 'valkey:9',[\s\S]*?authentication: 'username_password'/)
})

test('backing creation requires an explicit valid Valkey mode and omits authentication for PostgreSQL', () => {
  assert.equal(backingAuthenticationCreateFields('valkey:9', ''), undefined)
  assert.equal(backingAuthenticationCreateFields('valkey:9', 'invalid'), undefined)
  for (const authentication of ['username_password', 'password', 'none']) {
    assert.deepEqual(backingAuthenticationCreateFields('valkey:9', authentication), {
      adapter: 'valkey:9',
      authentication,
    })
  }
  assert.deepEqual(backingAuthenticationCreateFields('postgres:16', ''), { adapter: 'postgres:16' })
  assert.deepEqual(backingAuthenticationCreateFields('postgres:16', 'username_password'), { adapter: 'postgres:16' })

  assert.match(listSource, /Authentication/)
  assert.match(listSource, /\{adapter === 'valkey:9' && \([\s\S]*?<Label htmlFor="backing-authentication">Authentication<\/Label>/)
  assert.match(listSource, /useState<ValkeyAuthenticationSelection>\(''\)/)
  assert.match(listSource, /<option value="" disabled>Select authentication<\/option>/)
  assert.match(listSource, /const authenticationFields = backingAuthenticationCreateFields\(adapter, authentication\)/)
  assert.match(listSource, /if \(!authenticationFields\)[\s\S]*?return/)
  assert.match(listSource, /await store\.addBackingProject\([\s\S]*?\.\.\.authenticationFields/)
  assert.match(listSource, /disabled=\{creating \|\| !authenticationFields/)
  assert.match(listSource, /setAdapter\([\s\S]{0,160}setAuthentication\(''\)/)
  assert.match(listSource, /resetBackingIdentity\(\)[\s\S]{0,160}setAuthentication\(''\)/)
  assert.match(listSource, /Any client that can reach this backing service can access it without authentication\./)
  assert.doesNotMatch(listSource, /\(default\)/)
})

test('backing details and Attach inherit mode-specific facts and provisioning without an auth selector', () => {
  assert.match(detailSource, /Authentication/)
  assert.match(detailSource, /valkeyAuthenticationDetails/)
  assert.match(detailSource, /authenticationUnavailable \? \([\s\S]*?Mode-specific provisioning is unavailable/)
  assert.match(environmentSource, /inherits the backing instance/)
  assert.match(environmentSource, /valkeyAuthenticationDetails/)
  assert.match(environmentSource, /authenticationUnavailable[\s\S]*?\? \[\]/)
  assert.doesNotMatch(environmentSource, /Label>Authentication|Label htmlFor="attach-authentication"/)
})
