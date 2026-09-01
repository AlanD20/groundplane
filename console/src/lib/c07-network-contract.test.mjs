import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const storeSource = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')
const environmentPageSource = await readFile(
  new URL('../features/environment/environment-page.tsx', import.meta.url),
  'utf8',
)
const apiCLI = await readFile(new URL('../../../docs/api-cli.md', import.meta.url), 'utf8')
const routerStart = environmentPageSource.indexOf('function RouterCard(')
const routerEnd = environmentPageSource.indexOf('// ---- Deploys ----', routerStart)
const routerCardSource = environmentPageSource.slice(routerStart, routerEnd)

test('C07 Console detail actions read Zone and Route records from the Controller', () => {
  assert.match(storeSource, /getZone: async \(zoneId\).*tenantRequest<ZoneShowResponse>/s)
  assert.match(storeSource, /getRoute: async \(routeId\).*tenantRequest<RouteShowResponse>/s)
  assert.match(environmentPageSource, /data-action-id="zone\.show"/)
  assert.match(environmentPageSource, /data-action-id="route\.show"/)
})

test('C07 Console reports Route validation failures and blocks unsafe path tokens', () => {
  assert.match(environmentPageSource, /setSubmitError\(error instanceof Error/)
  assert.match(environmentPageSource, /Route paths contain a character that cannot be rendered safely/)
  assert.match(environmentPageSource, /Percent escapes must use exactly two hexadecimal digits/)
  assert.match(environmentPageSource, /role="alert"/)
})

test('C07 Router delegates live controls to the C12 and C14 Component API', () => {
  assert.match(routerCardSource, /store\.setComponentEnabled/)
  assert.match(routerCardSource, /store\.reconcileEnvironmentComponent/)
  assert.match(routerCardSource, /store\.updateComponentConfig/)
  assert.match(storeSource, /tenantRequest<ComponentTaskAccepted>/)
  assert.doesNotMatch(routerCardSource, /addSecret|<Switch|<Textarea/)
  assert.doesNotMatch(routerCardSource, /\btokenRef\b|env\.router\.hostnames|Create the secret/)
})

test('the locked CLI tree includes the Zone removal impact command', () => {
  assert.match(apiCLI, /zone\s+list \| show \| removal-impact \| add \| remove/)
})
