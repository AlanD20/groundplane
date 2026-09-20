'use client'

import { useEffect } from 'react'
import { Link } from 'react-router-dom'
import {
  Activity,
  ArrowRight,
  Boxes,
  Building2,
  Cpu,
  Database,
  HardDrive,
  Layers,
  MemoryStick,
  RefreshCw,
} from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { StatCard } from '@/components/common/stat-card'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge, StatusDot } from '@/components/common/status-badge'
import { TaskJournalItem } from '@/components/common/task-journal-item'
import { Button } from '@/components/ui/button'
import type { Environment, HealthState, TaskJournalScope } from '@/lib/types'
import { environmentRuntimeState, serviceObservationState } from '@/features/service/service-observation'
import { useVisibleServiceObservations } from '@/features/service/use-service-observation-refresh'

const platformTaskScope: TaskJournalScope = { kind: 'workspace', workspace: 'platform' }

function worst(states: HealthState[]): HealthState {
  const order: HealthState[] = ['failed', 'degraded', 'pending', 'stopped', 'unknown', 'healthy']
  for (const s of order) if (states.includes(s)) return s
  return 'unknown'
}

export default function PlatformOverviewPage() {
  const store = useStore()
  const { tenants, tenantProjects, backingProjects, host } = store
  const platformJournal = store.getTaskJournal(platformTaskScope)

  useEffect(() => {
    void store.loadTaskJournal('activity', platformTaskScope).catch(() => undefined)
  }, [store.loadTaskJournal])

  const allEnvs: { env: Environment; project: string; tenant: string }[] = tenantProjects.flatMap((project) => {
    const tenant = tenants.find((candidate) => candidate.id === project.tenantId)
    if (!tenant) return []

    return (project.environments ?? []).map((env) => ({
      env,
      project: project.slug,
      tenant: tenant.slug,
    }))
  })
  const visibleEnvironments = [
    ...allEnvs.map(({ env }) => env),
    ...backingProjects.flatMap((project) => project.environments?.slice(0, 1) ?? []),
  ]
  const observationRefresh = useVisibleServiceObservations({
    environmentIds: visibleEnvironments.map((environment) => environment.id),
    observations: visibleEnvironments.flatMap((environment) => environment.services.map((service) => service.observation)),
    refreshEnvironment: store.refreshEnvironmentServices,
  })
  const totalServices = allEnvs.reduce((n, e) => n + e.env.services.length, 0)
  const serviceStates = allEnvs.flatMap(({ env }) => env.services.map((service) => serviceObservationState(service.observation, observationRefresh.now)))
  const unavailableServices = serviceStates.filter((state) => state === 'unavailable').length
  const incompleteServices = serviceStates.filter((state) => state !== 'healthy' && state !== 'running' && state !== 'unavailable').length
  const platformHealth = worst([
    ...backingProjects.flatMap((g) => (g.status ? [g.status] : [])),
    ...allEnvs.map((e) => e.env.status),
  ])

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow="Platform"
        title="Platform overview"
        description="Every tenant, shared datastore, and host resource on this control plane, rolled up into one desired-state view."
        actions={
          <span className="flex items-center gap-2 rounded-lg border border-border bg-surface px-3 py-1.5 text-sm">
            <StatusDot status={platformHealth} />
            <span className="font-medium capitalize">{platformHealth}</span>
            <span className="text-muted-foreground">provisioning</span>
          </span>
        }
      />

      {observationRefresh.refreshError && (
        <p role="alert" className="text-sm text-destructive">
          Runtime refresh failed; evidence will expire locally. {observationRefresh.refreshError}
        </p>
      )}

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard icon={<Boxes />} label="Tenants" value={tenants.length} hint={`${tenantProjects.length} projects`} />
        <StatCard icon={<Layers />} label="Environments" value={allEnvs.length} hint="staging + production" />
        <StatCard
          icon={<Cpu />}
          label="Services"
          value={totalServices}
          hint={totalServices === 0 ? 'No Service observations loaded' : unavailableServices ? `${unavailableServices} runtime state${unavailableServices === 1 ? '' : 's'} unavailable` : incompleteServices ? `${incompleteServices} incomplete runtime${incompleteServices === 1 ? '' : 's'}` : 'all healthy or running unchecked'}
          tone={totalServices === 0 ? undefined : incompleteServices || unavailableServices ? 'warning' : 'success'}
        />
        <StatCard icon={<Database />} label="Backing services" value={backingProjects.length} hint="attachable datastores" />
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="flex flex-col gap-6 lg:col-span-2">
          {/* Tenants */}
          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Building2 className="size-4 text-muted-foreground" />
                Tenants
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              {tenants.map((t) => {
                const projects = tenantProjects.filter((p) => p.tenantId === t.id)
                return (
                  <Link
                    key={t.id}
                    to={`/t/${t.slug}`}
                    className="group flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5 transition-colors hover:border-ring/50 hover:bg-muted"
                  >
                    <div className="flex min-w-0 items-center gap-3">
                      <span className="flex size-6 shrink-0 items-center justify-center rounded-md bg-secondary text-secondary-foreground [&_svg]:size-3.5">
                        <Building2 />
                      </span>
                      <div className="flex min-w-0 flex-col">
                        <span className="truncate text-sm font-medium">{t.name}</span>
                        <span className="truncate font-mono text-xs text-muted-foreground">{t.description}</span>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <span className="hidden shrink-0 whitespace-nowrap text-xs text-muted-foreground sm:block">
                        {projects.length} projects
                      </span>
                      <ArrowRight className="size-4 text-muted-foreground/40 transition-transform group-hover:translate-x-0.5 group-hover:text-foreground" />
                    </div>
                  </Link>
                )
              })}
            </CardContent>
          </Card>

          {/* Projects */}
          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Boxes className="size-4 text-muted-foreground" />
                Projects
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              {tenantProjects.map((p) => {
                const tenant = tenants.find((x) => x.id === p.tenantId)
                const envs = p.environments ?? []
                const svcCount = envs.reduce((n, e) => n + e.services.length, 0)
                return (
                  <Link
                    key={p.id}
                    to={`/t/${tenant?.slug ?? p.tenantId}/${p.slug}`}
                    className="group flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5 transition-colors hover:border-ring/50 hover:bg-muted"
                  >
                    <div className="flex min-w-0 items-center gap-3">
                      {p.status && <StatusDot status={p.status} />}
                      <div className="flex min-w-0 flex-col">
                        <span className="truncate text-sm font-medium">
                          {p.name}
                          {tenant && (
                            <span className="ml-1.5 rounded bg-secondary px-1.5 py-0.5 font-mono text-[11px] font-normal text-secondary-foreground">
                              {tenant.slug}
                            </span>
                          )}
                        </span>
                        <span className="font-mono text-xs text-muted-foreground">
                          {envs.length} environments · {svcCount} services
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      {p.createdAt && <span className="hidden shrink-0 whitespace-nowrap text-xs text-muted-foreground sm:block">{p.createdAt}</span>}
                      <ArrowRight className="size-4 text-muted-foreground/40 transition-transform group-hover:translate-x-0.5 group-hover:text-foreground" />
                    </div>
                  </Link>
                )
              })}
            </CardContent>
          </Card>

          {/* Backing services */}
          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Database className="size-4 text-muted-foreground" />
                Backing services
              </CardTitle>
              <Link
                to="/platform/backing-services"
                className="flex items-center gap-1 text-sm text-muted-foreground transition-colors hover:text-foreground"
              >
                Manage <ArrowRight className="size-3.5" />
              </Link>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              {backingProjects.map((g) => (
                <Link
                  key={g.id}
                  to={`/platform/backing-services/${g.id}`}
                  className="group flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5 transition-colors hover:border-ring/50 hover:bg-muted"
                >
                  <div className="flex items-center gap-3">
                    {g.environments?.[0] && <StatusDot status={environmentRuntimeState(g.environments[0].services, observationRefresh.now)} />}
                    <div className="flex flex-col">
                      <span className="text-sm font-medium">{g.name}</span>
                      <span className="font-mono text-xs text-muted-foreground">{(g.environments?.[0]?.services[0]?.image ?? g.id)}</span>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <div className="hidden text-right sm:block">
                      <div className="text-sm font-medium">{g.consumers?.length ?? 0}</div>
                      <div className="text-xs text-muted-foreground">consumers</div>
                    </div>
                    <ArrowRight className="size-4 text-muted-foreground/40 transition-transform group-hover:translate-x-0.5 group-hover:text-foreground" />
                  </div>
                </Link>
              ))}
            </CardContent>
          </Card>

          {/* Environments across tenants */}
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <Layers className="size-4 text-muted-foreground" />
                Environments
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              {allEnvs.map(({ env, project, tenant }) => (
                <Link
                  key={env.id}
                  to={`/t/${tenant}/${project}/${env.name}`}
                  className="group flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5 transition-colors hover:border-ring/50 hover:bg-muted"
                >
                  <div className="flex min-w-0 items-center gap-3">
                    <StatusDot status={environmentRuntimeState(env.services, observationRefresh.now)} />
                    <div className="flex min-w-0 flex-col">
                      <span className="truncate text-sm font-medium">
                        {tenant}/{project}
                        <span className="ml-1.5 rounded bg-secondary px-1.5 py-0.5 font-mono text-[11px] font-normal text-secondary-foreground">
                          {env.name}
                        </span>
                        <StatusBadge status={env.status} label={`Provisioning ${env.provisioningState}`} className="ml-1.5 align-middle" />
                        <StatusBadge status={environmentRuntimeState(env.services, observationRefresh.now)} label={`Runtime ${environmentRuntimeState(env.services, observationRefresh.now)}`} className="ml-1.5 align-middle" />
                      </span>
                      <span className="font-mono text-xs text-muted-foreground">
                        {env.services.length} services · {env.release}
                      </span>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <span className="hidden shrink-0 whitespace-nowrap text-xs text-muted-foreground sm:block">
                      {env.lastDeployAt}
                    </span>
                  </div>
                </Link>
              ))}
            </CardContent>
          </Card>
        </div>

        {/* Right column */}
        <div className="flex flex-col gap-6">
          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Activity className="size-4 text-muted-foreground" />
                Platform activity
              </CardTitle>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={platformJournal.loading || platformJournal.loadingMore}
                  onClick={() => void store.loadTaskJournal('activity', platformTaskScope).catch(() => undefined)}
                  aria-label="Refresh Platform activity"
                >
                  <RefreshCw className="size-4" />
                </Button>
                <Link
                  to="/platform/activity"
                  className="flex items-center gap-1 text-sm text-muted-foreground transition-colors hover:text-foreground"
                >
                  View all <ArrowRight className="size-3.5" />
                </Link>
              </div>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              {platformJournal.loading && <p role="status" className="text-xs text-muted-foreground">refreshing Platform activity…</p>}
              {platformJournal.loadError && (
                <div role="alert" className="flex items-center justify-between gap-2 text-xs text-destructive">
                  <span>{platformJournal.loadError}</span>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={platformJournal.loading || platformJournal.loadingMore}
                    onClick={() => void store.loadTaskJournal('activity', platformTaskScope, platformJournal.failedCursor ?? undefined).catch(() => undefined)}
                  >
                    Retry
                  </Button>
                </div>
              )}
              {platformJournal.entries.slice(0, 6).map((entry) => (
                <TaskJournalItem key={entry.id} entry={entry} scope={platformTaskScope} />
              ))}
              {platformJournal.loaded && !platformJournal.loadError && platformJournal.entries.length === 0 && (
                <p className="py-4 text-center text-xs text-muted-foreground">no Platform tasks yet</p>
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex-row items-center justify-between">
              <CardTitle className="flex items-center gap-2">
                <Cpu className="size-4 text-muted-foreground" />
                Host
              </CardTitle>
              <Link
                to="/platform/host"
                className="flex items-center gap-1 text-sm text-muted-foreground transition-colors hover:text-foreground"
              >
                Details <ArrowRight className="size-3.5" />
              </Link>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              {host ? (
                <>
                  <div className="flex items-center justify-between text-sm">
                    <span className="font-mono text-muted-foreground">{host.hostname}</span>
                    <span className="text-muted-foreground">{host.arch} · {host.uptime}</span>
                  </div>
                  <Meter icon={<Cpu />} label="CPU load" pct={host.cpu.load} detail={`${host.cpu.cores} cores`} />
                  <Meter icon={<MemoryStick />} label="Memory" pct={host.memory.usedPct} detail={`${host.memory.used} / ${host.memory.total}`} />
                  <Meter icon={<HardDrive />} label="Disk" pct={host.disk.usedPct} detail={`${host.disk.used} / ${host.disk.total}`} />
                </>
              ) : (
                <p className="text-sm text-muted-foreground">Live Host health is unavailable.</p>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  )
}

function Meter({ icon, label, pct, detail }: { icon: React.ReactNode; label: string; pct: number; detail: string }) {
  const tone = pct >= 85 ? 'bg-destructive' : pct >= 70 ? 'bg-warning' : 'bg-primary'
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between text-xs">
        <span className="flex items-center gap-1.5 text-muted-foreground [&_svg]:size-3.5">
          {icon}
          {label}
        </span>
        <span className="font-mono text-muted-foreground">{detail}</span>
      </div>
      <div className="h-1.5 overflow-hidden rounded-full bg-secondary">
        <div className={`h-full rounded-full ${tone}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}
