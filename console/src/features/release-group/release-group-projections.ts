import type { Environment, ReleaseGroup, ReleaseGroupOnFailure } from '@/lib/types'

export type ReleaseGroupRollbackTarget = { service: string; tag?: string }

export function releaseGroupPath(tenant: string, project: string, environment: string, groupId: string) {
  return `/t/${tenant}/${project}/${environment}/release-groups/${groupId}`
}

export function releaseGroupPolicyLabel(policy: ReleaseGroupOnFailure) {
  return policy === 'switch_back' ? 'Switch back completed members' : 'Leave completed members active'
}

export function activeMemberTag(environment: Environment, service: string) {
  return environment.deploys.find((record) => record.service === service && record.status === 'active')?.tag
}

export function releaseGroupTag(environment: Environment, group: ReleaseGroup) {
  if (group.tag) return group.tag
  const tags = group.order.map((service) => activeMemberTag(environment, service))
  if (tags.every(Boolean) && new Set(tags).size === 1) return tags[0]!
  return tags.some(Boolean) ? 'mixed member tags' : 'not deployed'
}

export function resolveReleaseGroupRollbackTargets(
  environment: Environment,
  group: ReleaseGroup,
): ReleaseGroupRollbackTarget[] {
  return group.order.map((service) => {
    const activeTag = activeMemberTag(environment, service)
    const previous = environment.deploys.find(
      (record) => record.service === service && record.status === 'superseded' && record.tag !== activeTag,
    )
    return { service, tag: previous?.tag }
  })
}
