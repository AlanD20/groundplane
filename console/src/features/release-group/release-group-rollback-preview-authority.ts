export type RollbackPreviewScope = Readonly<{
  environmentId: string
  groupId: string
  tagPresent: boolean
  tagValue: string
  dialogSession: number
}>

export type RollbackPreviewResponse = Readonly<{
  release_group_id: string
  revision: string
  sources: ReadonlyArray<Readonly<{
    service_id: string
    release_id: string
    tag: string
  }>>
}>

type RollbackPreviewResponseInput = Omit<RollbackPreviewResponse, 'sources'> & Readonly<{
  sources: RollbackPreviewResponse['sources'] | null
}>

export type AcceptedRollbackPreview = Readonly<{
  scope: RollbackPreviewScope
  response: RollbackPreviewResponse
}>

export type RollbackPreviewRequest = Readonly<{
  scope: RollbackPreviewScope
  generation: number
}>

export function rollbackPreviewScope(
  environmentId: string,
  groupId: string,
  tag: string | undefined,
  dialogSession: number,
): RollbackPreviewScope {
  return Object.freeze({
    environmentId,
    groupId,
    tagPresent: tag !== undefined,
    tagValue: tag ?? '',
    dialogSession,
  })
}

function scopesEqual(left: RollbackPreviewScope, right: RollbackPreviewScope) {
  return left.environmentId === right.environmentId
    && left.groupId === right.groupId
    && left.tagPresent === right.tagPresent
    && left.tagValue === right.tagValue
    && left.dialogSession === right.dialogSession
}

export function acceptRollbackPreview(
  scope: RollbackPreviewScope,
  response: RollbackPreviewResponseInput,
): AcceptedRollbackPreview {
  const immutableResponse = Object.freeze({
    release_group_id: response.release_group_id,
    revision: response.revision,
    sources: Object.freeze((response.sources ?? []).map((source) => Object.freeze({ ...source }))),
  })
  return Object.freeze({ scope: Object.freeze({ ...scope }), response: immutableResponse })
}

export function isRollbackPreviewAccepted(
  accepted: AcceptedRollbackPreview | null,
  currentScope: RollbackPreviewScope,
): accepted is AcceptedRollbackPreview {
  return accepted !== null && scopesEqual(accepted.scope, currentScope)
}

export function requireRollbackPreview(
  accepted: AcceptedRollbackPreview | null,
  currentScope: RollbackPreviewScope,
) {
  if (!isRollbackPreviewAccepted(accepted, currentScope)) {
    throw new Error('Review the current rollback preview before dispatch')
  }
  return accepted
}

export function beginRollbackPreviewRequest(
  scope: RollbackPreviewScope,
  generation: number,
): RollbackPreviewRequest {
  return Object.freeze({ scope, generation })
}

export function isRollbackPreviewRequestCurrent(
  request: RollbackPreviewRequest,
  currentScope: RollbackPreviewScope,
  currentGeneration: number,
) {
  return request.generation === currentGeneration && scopesEqual(request.scope, currentScope)
}
