import type { Environment } from './types'

export type AuthoritativeEnvironmentScalars = Pick<
  Environment,
  | 'id'
  | 'projectId'
  | 'name'
  | 'networkPool'
  | 'networkCapacity'
  | 'status'
  | 'provisioningState'
  | 'createTaskId'
  | 'deletionTaskId'
  | 'volumeDir'
>

// Apply only fields represented by the Controller Environment document. Child
// collections are hydrated by their own list operations and must survive a
// synchronous Environment mutation response.
export function applyAuthoritativeEnvironmentScalars(
  current: Environment,
  authoritative: AuthoritativeEnvironmentScalars,
): Environment {
  current.id = authoritative.id
  current.projectId = authoritative.projectId
  current.name = authoritative.name
  current.networkPool = authoritative.networkPool
  current.networkCapacity = authoritative.networkCapacity
  current.status = authoritative.status
  current.provisioningState = authoritative.provisioningState
  current.createTaskId = authoritative.createTaskId
  current.deletionTaskId = authoritative.deletionTaskId
  current.volumeDir = authoritative.volumeDir
  return current
}
