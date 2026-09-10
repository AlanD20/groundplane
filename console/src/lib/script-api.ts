import type { operations } from './api.generated'
import type { Script } from './script-types'

export type ScriptCreateRequest = operations['script.create']['requestBody']['content']['application/json']
export type ScriptCreateResponse = operations['script.create']['responses'][201]['content']['application/json']
export type ScriptEditRequest = operations['script.edit']['requestBody']['content']['application/json']
export type ScriptEditResponse = operations['script.edit']['responses'][200]['content']['application/json']

const scriptHooks: Script['when'][] = [
  'manual',
  'pre-deploy',
  'post-deploy',
  'pre-rollback',
  'post-rollback',
  'on-failure',
]

export function scriptFromAPI(script: ScriptCreateResponse | ScriptEditResponse): Script {
  if (!scriptHooks.includes(script.when as Script['when'])) {
    throw new Error(`Controller returned unknown Script hook ${script.when}`)
  }
  if (!Number.isInteger(script.order) || script.order < 0 || script.order > 65535) {
    throw new Error('Controller returned an invalid Script order')
  }
  return {
    id: script.id,
    environmentId: script.environment_id,
    slug: script.slug,
    serviceId: script.service_id,
    service: script.service,
    body: script.script,
    order: script.order,
    when: script.when as Script['when'],
    origin: script.origin,
    reconciliationKey: script.reconciliation_key,
    activeGeneration: script.active_generation,
  }
}
