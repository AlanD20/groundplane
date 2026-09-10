export type ScriptHook =
  | 'manual'
  | 'pre-deploy'
  | 'post-deploy'
  | 'pre-rollback'
  | 'post-rollback'
  | 'on-failure'

export type Script = {
  id: string
  environmentId: string
  slug: string
  serviceId: string
  service: string
  when: ScriptHook
  order: number
  body: string
  origin: 'api' | 'blueprint'
  reconciliationKey?: string
  activeGeneration: number
}
