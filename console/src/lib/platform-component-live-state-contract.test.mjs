import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

test('CoreDNS presentation does not manufacture fields or forwarder identities', async () => {
  const hydration = await readFile(new URL('./platform-component-hydration.ts', import.meta.url), 'utf8')
  const page = await readFile(new URL('../features/platform-component/platform-component-page.tsx', import.meta.url), 'utf8')
  const environmentPage = await readFile(new URL('../features/environment/environment-page.tsx', import.meta.url), 'utf8')
  const store = await readFile(new URL('./store.tsx', import.meta.url), 'utf8')
  const types = await readFile(new URL('./types.ts', import.meta.url), 'utf8')
  const surfaces = await Promise.all([
    readFile(new URL('../features/platform-components/platform-components-page.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../App.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../components/shell/topbar.tsx', import.meta.url), 'utf8'),
    readFile(new URL('../features/platform-overview/platform-overview-page.tsx', import.meta.url), 'utf8'),
  ])

  assert.doesNotMatch(hydration, /127\.0\.0\.1:53|staticEntries\s*:|corefileRev\s*:|reloaded\s*:/)
  assert.doesNotMatch(hydration, /id:\s*`\$\{value\.domain\}/)
  assert.doesNotMatch(page, /127\.0\.0\.1:53|10\.20\.10\.2|staticEntries|corefileRev|reloaded|renderCorefile/)
	assert.match(store, /component-config\.show|managed_files/)
	assert.match(page, /Controller-rendered Corefile/)
	assert.match(page, /readOnly/)
	assert.match(page, /navigator\.clipboard\.writeText/)
	assert.match(page, /managedConfigLoading/)
	assert.match(page, /managedConfigError/)
  assert.match(page, /\[componentId, refreshComponentConfig\]/)
  assert.doesNotMatch(page, /\[component,\s*refreshComponentConfig\]/)
  assert.match(page, /onRemoveForwarder/)
  assert.doesNotMatch(page, /Forwarder removal is unavailable|stable forwarder identity/)
  assert.match(page, /disabled=\{saving \|\| !configured\}/)
  assert.match(page, /Save a complete resolver configuration before enabling CoreDNS/)
  assert.doesNotMatch(page, /function CoreDnsUnavailable/)
  assert.match(types, /config: \{ zone_ids: string\[\]; caddyfile_template\?: string \} \| null/)
  assert.match(types, /config: \{ zone_ids: string\[\]; secret_id: string \} \| null/)
  assert.doesNotMatch(store, /caddyfile_template[^\n]*\? config\.caddyfile_template : ''/)
  assert.doesNotMatch(store, /secret_id[^\n]*\? config\.secret_id : ''/)
  assert.match(hydration, /Controller returned invalid CoreDNS Component configuration/)
  assert.doesNotMatch(hydration, /value\.resolvers \?\? \[\]/)
  assert.match(environmentPage, /caddyTemplateMarkerCount !== 1/)
  assert.doesNotMatch(surfaces[0], /127\.0\.0\.1:53/)
  for (const surface of surfaces) assert.doesNotMatch(surface, /\/platform\/(?:tenants|projects)/)
})
