import type { Route } from '@/lib/types'

export function routeSummaryHint(routes: readonly Pick<Route, 'exposure' | 'status'>[]): string {
  if (routes.length === 0) return 'no routes'
  const hasPublic = routes.some((route) => route.exposure === 'public')
  const hasInternal = routes.some((route) => route.exposure === 'internal')
  const exposure = hasPublic ? (hasInternal ? 'public + internal' : 'public') : 'internal'
  const states = (['degraded', 'pending', 'unserved'] as const).flatMap((state) => {
    const count = routes.filter((route) => route.status === state).length
    return count === 0 ? [] : [`${count} ${state}`]
  })
  return `${exposure} · ${states.length > 0 ? states.join(', ') : 'provider applied'}`
}
