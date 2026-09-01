import type { operations } from './api.generated'
import type { HealthState, PlatformComponent, PlatformInfra } from './types'

type ComponentPageResponse = operations['component.list']['responses'][200]['content']['application/json']
export type ComponentResponse = NonNullable<ComponentPageResponse['items']>[number]
type PlatformComponentKind = PlatformComponent['kind']

const coreDNSPresentation: Omit<PlatformComponent, 'id' | 'status'> = {
  name: 'CoreDNS',
  kind: 'coredns',
  image: 'unavailable',
  version: 'unavailable',
  runtime: 'container · host network',
  hostNetwork: true,
  mounts: [],
  notes: [],
}

export type PlatformComponentHydration = {
  components: PlatformComponent[]
  dns?: PlatformInfra['dns']
}

function componentHealthState(status: string | undefined): HealthState {
  switch (status) {
    case 'disabled': return 'stopped'
    case 'healthy': return 'healthy'
    case 'degraded': return 'degraded'
    case 'pending': return 'pending'
    default: return 'unknown'
  }
}

function platformKind(value: string): PlatformComponentKind {
  if (value === 'coredns') return value
  throw new Error(`Controller returned unknown Platform Component kind ${value}`)
}

export function platformComponentFromAPI(item: ComponentResponse): PlatformComponent {
  if (item.owner !== 'platform' || item.owner_id) {
    throw new Error('Controller returned a Component outside the Platform owner scope')
  }
  const kind = platformKind(item.kind)
  return {
    ...coreDNSPresentation,
    id: item.id,
    status: componentHealthState(item.status),
  }
}

export function platformDNSFromComponent(item: ComponentResponse): PlatformInfra['dns'] {
  const config = item.config
  if (!config) {
    return { enabled: item.enabled }
  }
  if (
    !('upstream_auto' in config) || typeof config.upstream_auto !== 'boolean' ||
    !('upstream_resolvers' in config) || !Array.isArray(config.upstream_resolvers) || config.upstream_resolvers.some((value) => typeof value !== 'string') ||
    !('tailnet_delegation' in config) || typeof config.tailnet_delegation !== 'boolean' ||
    !('forwarders' in config) || !Array.isArray(config.forwarders) || config.forwarders.some((value) => (
      typeof value.domain !== 'string' || !Array.isArray(value.resolvers) || value.resolvers.some((resolver) => typeof resolver !== 'string')
    ))
  ) {
    throw new Error('Controller returned invalid CoreDNS Component configuration')
  }
  const forwarders = config.forwarders.map((value) => ({
      domain: value.domain,
      upstream: value.resolvers.join(' '),
  }))
  return {
    enabled: item.enabled,
    upstream: config.upstream_resolvers.join(' '),
    upstreamAuto: config.upstream_auto,
    tailnetDelegation: config.tailnet_delegation,
    forwarders,
  }
}

export function hydratePlatformComponents(items: ComponentResponse[]): PlatformComponentHydration {
  const components = items.map(platformComponentFromAPI)
  const coredns = items.find((item) => item.kind === 'coredns')
  return {
    components,
    ...(coredns ? { dns: platformDNSFromComponent(coredns) } : {}),
  }
}
