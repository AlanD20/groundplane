import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

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

test('backing creation sends authentication only for Valkey and warns before no-auth creation', () => {
  assert.match(listSource, /Authentication/)
  assert.match(listSource, /adapter === 'valkey:9'[\s\S]*?authentication/)
  assert.match(listSource, /Any client that can reach this backing service can access it without authentication\./)
  assert.doesNotMatch(listSource, /adapter,[\s\S]{0,120}authentication,[\s\S]{0,120}network_pool/)
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
