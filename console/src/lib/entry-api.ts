import type { operations } from './api.generated'
import type { EnvironmentEntry } from './entry-types'

type EntryResponse = operations['entry.edit']['responses'][200]['content']['application/json']

export function entryFromAPI(entry: EntryResponse): EnvironmentEntry {
  if (entry.type !== 'env' && entry.type !== 'file') {
    throw new Error(`Controller returned unknown Entry type ${entry.type}`)
  }
  if (!entry.exposure || entry.exposure.length === 0) {
    throw new Error(`Controller returned Entry ${entry.id} without exposure`)
  }
  let source: EnvironmentEntry['source']
  switch (entry.source.kind) {
    case 'literal':
      source = { kind: 'literal', literal: entry.source.literal }
      break
    case 'secret_ref':
      if (!entry.source.secret_ref) throw new Error(`Controller returned invalid secret_ref Entry ${entry.id}`)
      source = { kind: 'secret_ref', secretRef: entry.source.secret_ref }
      break
    case 'fact':
      if (!entry.source.attach_id || !entry.source.fact) {
        throw new Error(`Controller returned invalid fact Entry ${entry.id}`)
      }
      source = {
        kind: 'fact',
        attachId: entry.source.attach_id,
        grantAttachId: entry.source.grant_attach_id,
        fact: entry.source.fact,
      }
      break
    default:
      throw new Error(`Controller returned unknown Entry source ${entry.source.kind}`)
  }
  return {
    id: entry.id,
    type: entry.type,
    reconciliationKey: entry.reconciliation_key,
    key: entry.key,
    path: entry.path,
    uid: entry.uid,
    gid: entry.gid,
    source,
    exposure: [...entry.exposure],
    secret: entry.secret,
  }
}
