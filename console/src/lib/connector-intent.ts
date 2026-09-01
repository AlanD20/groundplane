import { newULID } from './utils'

export type ConnectorMutationIntent = Readonly<{
  idempotencyKey: string
}>

export function newConnectorMutationIntent(): ConnectorMutationIntent {
  return { idempotencyKey: newULID() }
}
