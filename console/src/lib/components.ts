import type {
  CaddyComponent,
  CloudflareTunnelComponent,
  Environment,
  EnvironmentComponent,
} from './types'

type ComponentSeed = {
  caddy?: {
    enabled?: boolean
    zoneId?: string
    caddyfile_template?: string
    pinnedIPv4?: string
  }
  tunnel?: {
    enabled?: boolean
    secret_id?: string
  }
}

export function createEnvironmentComponents(
  environmentId: string,
  caddyId: string,
  tunnelId: string,
  seed: ComponentSeed = {},
): EnvironmentComponent[] {
  const caddyEnabled = seed.caddy?.enabled ?? false
  const tunnelEnabled = seed.tunnel?.enabled ?? false

  return [
    {
      id: caddyId,
      kind: 'caddy',
      owner: 'environment',
      ownerRef: environmentId,
      enabled: caddyEnabled,
      status: caddyEnabled ? 'healthy' : 'stopped',
      dependencies: [],
      generatedServices: caddyEnabled ? [`svc_${caddyId.slice(4)}`] : [],
      config: { zone_id: seed.caddy?.zoneId ?? '', caddyfile_template: seed.caddy?.caddyfile_template ?? '' },
      state: { pinnedIPv4: seed.caddy?.pinnedIPv4 },
    },
    {
      id: tunnelId,
      kind: 'cloudflare-tunnel',
      owner: 'environment',
      ownerRef: environmentId,
      enabled: tunnelEnabled,
      status: tunnelEnabled ? 'healthy' : 'stopped',
      dependencies: [],
      generatedServices: tunnelEnabled ? [`svc_${tunnelId.slice(4)}`] : [],
      config: { secret_id: seed.tunnel?.secret_id ?? '' },
      state: {},
    },
  ]
}

export type RouterProjection = Readonly<{
  caddy: CaddyComponent
  tunnel: CloudflareTunnelComponent
}>

export function routerProjection(environment: Pick<Environment, 'id' | 'components'>): RouterProjection {
  const caddy = environment.components.find((component): component is CaddyComponent => component.kind === 'caddy')
  const tunnel = environment.components.find(
    (component): component is CloudflareTunnelComponent => component.kind === 'cloudflare-tunnel',
  )

  if (!caddy || !tunnel) {
    throw new Error(`environment ${environment.id} is missing its ingress component records`)
  }

  return { caddy, tunnel }
}
