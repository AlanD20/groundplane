import type { Environment, Project } from './types'

export function environmentDeletionGuard(
  projects: Project[],
  environmentId: string,
  pending: (environmentId: string) => boolean,
) {
  const environment: Environment | undefined = projects
    .flatMap((project) => project.environments ?? [])
    .find((candidate) => candidate.id === environmentId)
  return environment?.deletionTaskId || pending(environmentId)
}
