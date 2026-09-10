export type EnvironmentEntrySource =
  | { kind: 'literal'; literal?: string }
  | { kind: 'secret_ref'; secretRef: string }
  | { kind: 'fact'; attachId: string; grantAttachId?: string; fact: string }

export type EnvironmentEntry = {
  id: string
  type: 'env' | 'file'
  reconciliationKey?: string
  key?: string
  path?: string
  uid?: number
  gid?: number
  source: EnvironmentEntrySource
  exposure: string[]
  secret: boolean
}
