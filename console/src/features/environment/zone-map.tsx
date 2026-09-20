import { useRef, useState, type ReactNode } from 'react'
import type { Environment, Zone } from '@/lib/types'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { ServicesList } from './services-list'
import { ServiceDetailsDrawer } from './service-details-drawer'

export type ZoneSelection = { selectedId: string | null; onSelect: (id: string) => void }

export function ZoneMap({
  env,
  actions,
  renderZone,
  renderUnzoned,
}: {
  env: Environment
  actions: ReactNode
  renderZone: (zone: Zone, selection: ZoneSelection) => ReactNode
  renderUnzoned: (selection: ZoneSelection) => ReactNode
}) {
  const [view, setView] = useState<'map' | 'list'>('map')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [details, setDetails] = useState(false)
  const board = useRef<HTMLDivElement>(null)
  const selected = env.services.find((service) => service.id === selectedId)
  const selection: ZoneSelection = {
    selectedId: selected?.id ?? null,
    onSelect: (id) => setSelectedId((current) => (current === id ? null : id)),
  }

  function jump(id: string) {
    const container = board.current
    const target = Array.from(container?.children ?? []).find((child) => child.getAttribute('data-zone-id') === id)
    if (container && target instanceof HTMLElement) {
      container.scrollTo({
        left: target.offsetLeft,
        behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'instant' : 'smooth',
      })
      target.focus({ preventScroll: true })
    }
  }

  return (
    <section className="flex min-w-0 flex-col gap-4" aria-label="Network topology">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-sm font-semibold">Network zones</h2>
          <p className="mt-1 text-xs text-muted-foreground">
            {env.services.length} services, {env.zones.length} zones. Repeated cards represent the same Service.
          </p>
        </div>
        <div className="flex flex-wrap gap-2">
          {actions}
          <div className="flex rounded-lg border border-border bg-card p-1">
            <Button
              variant={view === 'map' ? 'secondary' : 'ghost'}
              size="sm"
              aria-pressed={view === 'map'}
              onClick={() => setView('map')}
            >
              Map
            </Button>
            <Button
              variant={view === 'list' ? 'secondary' : 'ghost'}
              size="sm"
              aria-pressed={view === 'list'}
              onClick={() => setView('list')}
            >
              List
            </Button>
          </div>
        </div>
      </div>
      {view === 'map' ? (
        <>
          <nav className="flex flex-wrap gap-1" aria-label="Jump to zone">
            {env.zones.map((zone) => (
              <Button key={zone.id} size="xs" variant="ghost" onClick={() => jump(zone.id)}>
                {zone.name}
              </Button>
            ))}
            <Button size="xs" variant="ghost" onClick={() => jump('unzoned')}>
              No zone
            </Button>
          </nav>
          <div
            ref={board}
            tabIndex={0}
            aria-label="Network zones, scroll horizontally"
            className="relative flex snap-x snap-proximity gap-4 overflow-x-auto pb-3"
          >
            {env.zones.map((zone) => (
              <div
                key={zone.id}
                data-zone-id={zone.id}
                tabIndex={-1}
                className="min-w-[270px] flex-1 snap-start rounded-xl"
              >
                {renderZone(zone, selection)}
              </div>
            ))}
            <div data-zone-id="unzoned" tabIndex={-1} className="min-w-[270px] flex-1 snap-start rounded-xl">
              {renderUnzoned(selection)}
            </div>
          </div>
          <div
            className="flex min-h-16 flex-wrap items-center gap-3 rounded-xl border border-border bg-card p-4 text-xs"
            aria-live="polite"
          >
            {selected ? (
              <>
                <strong>{selected.name}</strong>
                {selected.zones.map((zone) => (
                  <Badge key={zone} variant="outline">
                    {zone}
                  </Badge>
                ))}
                {selected.zones.length === 0 && <Badge variant="outline">No zone</Badge>}
                <div className="ml-auto flex gap-2">
                  <Button size="sm" variant="outline" onClick={() => setDetails(true)}>
                    Service details
                  </Button>
                  <Button size="sm" variant="ghost" onClick={() => setSelectedId(null)}>
                    Clear
                  </Button>
                </div>
              </>
            ) : (
              <span className="text-muted-foreground">
                Select a Service to highlight every zone it joins. Click again to clear.
              </span>
            )}
          </div>
          <p className="text-xs text-muted-foreground">
            Attach chips link to backing services. Network membership is not a traffic measurement.
          </p>
        </>
      ) : (
        <ServicesList env={env} />
      )}
      {selected && <ServiceDetailsDrawer env={env} service={selected} open={details} onOpenChange={setDetails} />}
    </section>
  )
}
