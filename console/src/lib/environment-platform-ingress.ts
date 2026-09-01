import type { Project } from './types'
import { routerProjection } from './components'

function hasCompleteRouterProjection(project: NonNullable<Project['environments']>[number]) {
  return project.components.some((component) => component.kind === 'caddy') &&
    project.components.some((component) => component.kind === 'cloudflare-tunnel')
}

export function environmentPlatformIngress(projects: Project[]) {
  let unavailable = 0
  const hostnames: { host: string; env: string }[] = []
  for (const project of projects) {
    for (const environment of project.environments ?? []) {
      if (!hasCompleteRouterProjection(environment)) {
        unavailable++
        continue
      }
      const router = routerProjection(environment)
      if (router.caddy.enabled && router.tunnel.enabled) {
        hostnames.push(...environment.routes.filter((route) => route.exposure === 'public').map((route) => ({
          host: route.host,
          env: `${project.slug}/${environment.name}`,
        })))
      }
    }
  }
  return { unavailable, hostnames }
}
