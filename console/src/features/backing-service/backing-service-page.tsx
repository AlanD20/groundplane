import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/components/ui/table'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useRequiredParams } from '@/lib/router'
import { ArrowLeft, Boxes, Database, FileCode2, Power, PowerOff, RefreshCw, RotateCw, Server, Trash2 } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { StatCard } from '@/components/common/stat-card'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsList, TabsTab, TabsPanel } from '@/components/ui/tabs'
import { RevealValue } from '@/components/common/reveal-value'
import { CopyButton } from '@/components/common/copy-button'
import { EmptyState } from '@/components/common/empty-state'
import { toYAML } from '@/lib/yaml'
import type { ConsumerLink, Project } from '@/lib/types'
import { valkeyAuthenticationDetails } from '@/lib/valkey-authentication'
import { Row, ServiceTab } from './service-tab'
import { Drawer } from '@/components/ui/drawer'
import { ServiceFormBody } from '@/components/common/service-form-body'
import { serviceObservationState } from '@/features/service/service-observation'
import { useVisibleServiceObservations } from '@/features/service/use-service-observation-refresh'

type PlatformTab = 'service' | 'connections' | 'state' | 'backups'

export default function BackingServiceDetailPage() {
  const params = useRequiredParams('id')
  const store = useStore()
  const g = store.getBackingProject(params.id)
  const env = g?.environments?.[0]
  const svc = env?.services[0]
	const [tab, setTab] = useState<PlatformTab>('service')
	const [pendingAction, setPendingAction] = useState<'start' | 'stop' | 'destroy' | null>(null)
	const [actionError, setActionError] = useState('')
  const [editOpen, setEditOpen] = useState(false)
  const observationRefresh = useVisibleServiceObservations({
    environmentIds: env ? [env.id] : [],
    observations: svc ? [svc.observation] : [],
    refreshEnvironment: store.refreshEnvironmentServices,
  })

  if (!g || !env || !svc) {
    return (
      <EmptyState
        icon={<Database />}
        title="Backing service not found"
        description={`${params.id} does not exist.`}
        action={
          <Link to="/platform/backing-services">
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to backing services
            </Button>
          </Link>
        }
      />
    )
  }

  const running = svc.runtimeIntent === 'running'
  const runtimeState = serviceObservationState(svc.observation, observationRefresh.now)
  const adapter = store.adapters.find((a) => a.key === svc.adapter)
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication)
  const port = adapter?.urlScheme === 'redis' ? 6379 : adapter?.urlScheme === 'pgsql' ? 5432 : undefined
  // Backups are per consumer: count the attach-backed sources across all
  // environments that attach this backing project.
  const consumerBackupCount = store.tenantProjects.reduce(
    (n, p) =>
      n +
      (p.environments ?? []).reduce(
        (m, e) => m + (e.backup?.sources ?? []).filter((source) => source.kind === 'attach' && e.attaches.find((attach) => attach.id === source.ref)?.projectId === g.id).length,
        0,
      ),
    0,
  )
  const statusTone: 'success' | 'warning' | 'danger' | 'default' =
    runtimeState === 'healthy'
      ? 'success'
      : runtimeState === 'degraded' || runtimeState === 'starting' || runtimeState === 'running'
        ? 'warning'
        : runtimeState === 'failed'
          ? 'danger'
			: 'default'
	const runLifecycle = async (action: 'start' | 'stop' | 'destroy') => {
		setPendingAction(action)
		setActionError('')
		try {
			await store.runBackingRuntimeAction(g.id, action)
		} catch (error) {
			setActionError(error instanceof Error ? error.message : 'Backing-service lifecycle request failed')
		} finally {
			setPendingAction(null)
		}
	}

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={
          <>
            {g.name}
            <MetaPill icon={<StatusDot status={env.status} />} tone={env.provisioningState === 'failed' ? 'danger' : env.provisioningState === 'provisioning' ? 'warning' : 'success'} className="ml-2 align-middle capitalize">
              provisioning {env.provisioningState}
            </MetaPill>
            <MetaPill icon={<StatusDot status={runtimeState} />} tone={statusTone} className="ml-2 align-middle capitalize">
              runtime {runtimeState}
            </MetaPill>
          </>
        }
        description={g.description}
        icon={<Database />}
        meta={
          <>
            <MetaPill icon={<Database />}>
              {adapter?.label ?? svc.adapter} · {svc.image}
            </MetaPill>
            <MetaPill icon={<Server />}>
              {svc.serviceName}{port ? `:${port}` : ''}
            </MetaPill>
            {authenticationDetails && <MetaPill>{authenticationDetails.label} authentication</MetaPill>}
            <MetaPill icon={<Boxes />}>{env.name} environment</MetaPill>
          </>
        }
			actions={
				<>
					<Button variant="outline" onClick={() => setEditOpen(true)}>Edit service</Button>
					{running ? (
						<Button variant="outline" disabled={pendingAction !== null} onClick={() => void runLifecycle('stop')}>
							<PowerOff className="size-4" /> Stop
						</Button>
					) : (
						<Button disabled={pendingAction !== null} onClick={() => void runLifecycle('start')}>
							<Power className="size-4" /> Start
						</Button>
					)}
					<Button variant="destructive" disabled={pendingAction !== null} onClick={() => void runLifecycle('destroy')} title="Remove runtime and retain durable data">
						<Trash2 className="size-4" /> Destroy
					</Button>
				</>
			}
		/>
		{actionError && <p role="alert" className="text-sm text-destructive">{actionError}</p>}
      <Drawer open={editOpen} onOpenChange={setEditOpen}>
        {editOpen && <ServiceFormBody env={env} workspace="platform" initial={svc} onClose={() => setEditOpen(false)} />}
      </Drawer>
		{observationRefresh.refreshError && <p role="alert" className="text-sm text-destructive">Runtime refresh failed; evidence will expire locally. {observationRefresh.refreshError}</p>}

      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard icon={<Boxes />} label="Consumers" value={g.consumers?.length ?? 0} hint="environments attached" />
        <StatCard icon={<Database />} label="Adapter" value={adapter?.label ?? '—'} hint={`adapter ${svc.adapter}`} />
        <StatCard icon={<Server />} label="Service" value={svc.serviceName ?? '—'} hint={port ? `port ${port}` : 'operator-selected image'} />
        <StatCard icon={<RefreshCw />} label="Consumer backups" value={consumerBackupCount} hint="attach sources across all consumers" />
      </div>

      <Tabs value={tab} onValueChange={(v) => setTab(v as PlatformTab)}>
        <TabsList>
          <TabsTab value="service">Service</TabsTab>
          <TabsTab value="connections">Connections</TabsTab>
          <TabsTab value="state">Desired state</TabsTab>
          <TabsTab value="backups">Backups</TabsTab>
        </TabsList>

        <TabsPanel value="service" className="mt-6 flex flex-col gap-6">
          <ServiceTab env={env} svc={svc} now={observationRefresh.now} />
        </TabsPanel>
        <TabsPanel value="connections" className="mt-6 flex flex-col gap-6">
          <ConnectionsTab g={g} env={env} svc={svc} />
        </TabsPanel>
        <TabsPanel value="state" className="mt-6 flex flex-col gap-4">
          <DesiredStateTab g={g} env={env} svc={svc} />
        </TabsPanel>
        <TabsPanel value="backups" className="mt-6 flex flex-col gap-4">
          <BackupsTab g={g} env={env} svc={svc} />
        </TabsPanel>
      </Tabs>
    </div>
  )
}

// ---- Connections ----

function ConnectionsTab({ g, env, svc }: { g: Project; env: NonNullable<Project['environments']>[number]; svc: NonNullable<Project['environments']>[number]['services'][number] }) {
  const store = useStore()
  const adapter = store.adapters.find((a) => a.key === svc.adapter)
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication)
  const authenticationUnavailable = svc.adapter === 'valkey:9' && !authenticationDetails
  const exposedFacts = authenticationDetails
    ? authenticationDetails.factSuffixes.map((suffix) => `${svc.prefix ?? adapter?.prefix}_${suffix}`)
    : authenticationUnavailable ? [] : adapter?.envVars ?? []
  const provision = authenticationDetails?.provision ?? (authenticationUnavailable ? [] : adapter?.provision ?? [])
  const exposesRole = adapter?.requires.role && (svc.adapter !== 'valkey:9' || svc.authentication === 'username_password')

  return (
    <>
      {adapter && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <RotateCw className="size-4 text-muted-foreground" /> Adapter · {svc.adapter}
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3">
            {adapter?.custom ? (
              <p className="text-xs text-muted-foreground">
                <span className="font-medium text-foreground">Custom container managed by Groundplane.</span>{' '}
                {svc.hooks?.attach ? 'Each new Attach runs your provisioning command and publishes its declared facts after success.' : 'Attaching connects a consumer to the backing network without provisioning.'}{' '}
                Hooks are configured under Edit service. Custom services have no managed grants or backups.
              </p>
            ) : authenticationUnavailable ? (
              <p role="alert" className="text-xs text-destructive">
                The Controller did not return this Valkey backing instance&apos;s authentication mode. Refresh before using connection guidance.
              </p>
            ) : authenticationDetails ? (
              <p className="text-xs text-muted-foreground">
                Every Attach <span className="font-medium text-foreground">inherits the backing instance&apos;s immutable {authenticationDetails.label.toLowerCase()} authentication mode</span>; there is no per-Attach authentication selector.{' '}
                {authenticationDetails.summary} Attaching always joins this backing network and publishes the mode&apos;s facts; nothing is injected automatically.
              </p>
            ) : (
              <p className="text-xs text-muted-foreground">
                The Controller dispatches to this adapter to auto-provision consumers. These operations run on attach,
                rotate, and repair — each parameter is filled from the attach facts and the generated password. Attaching
                exposes facts (prefix <span className="font-mono">{svc.prefix ?? adapter?.prefix}</span>), e.g.{' '}
                <span className="font-mono">{svc.prefix ?? adapter?.prefix}_URL</span> — you create env vars from them;
                nothing is injected automatically.
              </p>
            )}
            <div className="flex flex-col gap-1.5 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Network</span>
                <span className="font-mono text-xs">
                  owns/joins: {env.zones.map((z) => `${z.name} · ${z.subnet}${z.internal ? ' · internal' : ''}`).join(', ') || '—'}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Healthcheck</span>
                <span className="font-mono text-xs">
                  {svc.healthcheck ? `${svc.healthcheck.kind} ${svc.healthcheck.target} · every ${svc.healthcheck.interval}` : 'none'}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-muted-foreground">Volume</span>
                <span className="font-mono text-xs">
                  {svc.mounts.find((m) => m.type === 'volume')?.volume ?? 'data'}
                </span>
              </div>
            </div>
            {!adapter?.custom && (
              <>
                <div className="flex flex-wrap gap-1.5">
                  {exposedFacts.map((v) => (
                    <span key={v} className="rounded-full bg-primary/10 px-2.5 py-1 font-mono text-xs text-primary">
                      {v}
                    </span>
                  ))}
                </div>
                <div className="flex flex-col gap-1 border-t border-border pt-3">
                  {authenticationUnavailable ? (
                    <p className="text-xs text-muted-foreground">Mode-specific provisioning is unavailable until the Controller returns the authentication mode.</p>
                  ) : provision.length === 0 && (
                    <p className="text-xs text-muted-foreground">No credential provisioning steps. Attach manages network membership and fact ownership only.</p>
                  )}
                  {provision.map((op) => (
                    <div key={op.op} className="flex items-baseline gap-2.5 text-xs">
                      <span className="size-1.5 shrink-0 translate-y-[-2px] rounded-full bg-success" />
                      <span className="w-36 shrink-0 font-mono text-primary">{op.op}</span>
                      <span className="break-all font-mono text-muted-foreground">{op.detail}</span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Database className="size-4 text-muted-foreground" /> Consumers
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-2.5">
          {(g.consumers ?? []).length === 0 && (
            <p className="text-xs text-muted-foreground">
              No environments attached yet — attachment opens once this backing service is running.
            </p>
          )}
          {(g.consumers ?? []).map((c) => (
            <div key={`${c.environment}-${c.service}-${c.attachId}`} className="rounded-xl border border-border bg-card p-4">
              <div className="flex items-center justify-between">
                <span className="font-mono text-sm">
                  {c.project} / {c.environment} <span className="text-muted-foreground">· {c.service}</span>
                </span>
                {!adapter?.custom && (
                  <div className="flex items-center gap-1">
                    <ConsumerConnectionActions consumer={c} authentication={svc.authentication} />
                  </div>
                )}
              </div>
              {adapter?.custom ? (
                <div className="mt-2 flex flex-col gap-1.5 text-sm">
                  <Row label="Access" value={svc.hooks?.attach ? 'network access and custom provisioning' : 'network-only, no provisioning'} mono />
                  <Row label="Declared facts" value={svc.hooks?.facts?.map((fact) => fact.key).join(', ') || 'none'} mono />
                  <Row label="Reach at" value={`${svc.serviceName ?? svc.name} on the network`} mono />
                </div>
              ) : (
                <div className="mt-2 flex flex-col gap-1.5 text-sm">
                  <Row label="Service" value={c.service} mono />
                  {adapter?.requires.database && <Row label="Database" value={c.database} mono />}
                  {exposesRole && <Row label="Role" value={c.role} mono />}
                  <Row label="Host" value={`${svc.serviceName}:${adapter?.urlScheme === 'redis' ? 6379 : 5432}`} mono />
                  <Row
                    label="Connection"
                    value={c.connectionFactKey ? 'available through explicit reveal' : 'unavailable'}
                    mono
                    masked={svc.authentication !== 'none'}
                  />
                </div>
              )}
              {!adapter?.custom && (
                <details className="mt-2">
                  <summary className="cursor-pointer text-xs text-primary">
                    procedure the Agent runs · {provision.length} steps
                  </summary>
                  <div className="mt-2 flex flex-col gap-1 border-t border-border pt-2">
                    {authenticationUnavailable ? (
                      <p className="text-xs text-muted-foreground">Mode-specific provisioning is unavailable until the Controller returns the authentication mode.</p>
                    ) : provision.length === 0 && (
                      <p className="text-xs text-muted-foreground">No credential procedure; the Attach joins the network and publishes credential-free facts.</p>
                    )}
                    {provision.map((op) => (
                      <div key={op.op} className="flex items-baseline gap-2.5 text-xs">
                        <span className="size-1.5 shrink-0 translate-y-[-2px] rounded-full bg-success" />
                        <span className="w-32 shrink-0 font-mono text-primary">{op.op}</span>
                        <span className="break-all font-mono text-muted-foreground">
                          {op.detail.replaceAll('<db>', c.database).replaceAll('<role>', c.role).replaceAll('<generated>', '••••••')}
                        </span>
                      </div>
                    ))}
                  </div>
                </details>
              )}
            </div>
          ))}
        </CardContent>
      </Card>
    </>
  )
}

// ---- Desired state ----

function DesiredStateTab({ g, env, svc }: { g: Project; env: NonNullable<Project['environments']>[number]; svc: NonNullable<Project['environments']>[number]['services'][number] }) {
  const store = useStore()
  const adapter = store.adapters.find((a) => a.key === svc.adapter)
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication)
  const authenticationUnavailable = svc.adapter === 'valkey:9' && !authenticationDetails
  const doc: Record<string, unknown> = {
    kind: 'backing',
    schema: 1,
    metadata: {
      name: svc.serviceName,
      label: g.name,
    },
    backing: {
      name: svc.serviceName,
      adapter: svc.adapter,
      ...(svc.adapter === 'valkey:9' ? { authentication: svc.authentication } : {}),
      image: svc.image,
      prefix: svc.prefix ?? adapter?.prefix,
      host: svc.serviceName,
      port: adapter?.urlScheme === 'redis' ? 6379 : adapter?.urlScheme === 'pgsql' ? 5432 : undefined,
      zones: env.zones.map((z) => ({ name: z.name, subnet: z.subnet, internal: z.internal })),
      custom: adapter?.custom ?? false,
      hooks: svc.hooks,
      provision: authenticationDetails?.provision ?? (authenticationUnavailable ? [] : adapter?.provision ?? []),
      exposes: adapter?.custom ? (svc.hooks?.facts ?? []).map((fact) => fact.key) : authenticationDetails
        ? authenticationDetails.factSuffixes.map((suffix) => `${svc.prefix ?? adapter?.prefix}_${suffix}`)
        : authenticationUnavailable ? [] : adapter?.envVars ?? [],
      healthcheck: svc.healthcheck ? { kind: svc.healthcheck.kind, target: svc.healthcheck.target } : undefined,
      volume: adapter?.custom ? undefined : 'volumes/data',
    },
    consumers: (g.consumers ?? []).map((c) => ({ project: c.project, environment: c.environment, service: c.service, database: c.database, role: c.role })),
  }
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <FileCode2 className="size-4 text-muted-foreground" /> Desired state · backing/{g.id}.yaml
        </CardTitle>
        <CopyButton value={toYAML(doc)} label="copy" />
      </CardHeader>
      <CardContent>
        <pre className="overflow-x-auto rounded-lg border border-border bg-background p-4 font-mono text-xs leading-relaxed text-foreground">
          {toYAML(doc)}
        </pre>
      </CardContent>
    </Card>
  )
}

// ---- Backups ----
// Backups are PER CONSUMER: the backing environment itself never backs up.
// Every attached environment's Backup page selects its own attach databases
function ConsumerConnectionActions({ consumer, authentication }: { consumer: ConsumerLink; authentication?: string }) {
  const store = useStore()
  if (!consumer.connectionFactKey) {
    return <span className="text-xs text-muted-foreground">Connection fact unavailable</span>
  }
  const factKey = consumer.connectionFactKey
  return (
    <RevealValue
      loadValue={() => store.revealAttachFact(consumer.attachId, factKey)}
      label="connection"
      confirmWord={consumer.role || consumer.service}
      sensitive={authentication !== 'none'}
    />
  )
}

// and the adapter dumps exactly that database. This tab shows what consumers
// are backing up right now.

function BackupsTab({ g, env, svc }: { g: Project; env: NonNullable<Project['environments']>[number]; svc: NonNullable<Project['environments']>[number]['services'][number] }) {
  const store = useStore()
  const adapter = store.adapters.find((a) => a.key === svc.adapter)
  const consumerBackups = store.tenantProjects.flatMap((p) =>
    (p.environments ?? []).flatMap((e) => {
      const backup = e.backup
      if (!backup) return []
      return backup.sources
        .filter((source) => source.kind === 'attach' && e.attaches.find((attach) => attach.id === source.ref)?.projectId === g.id)
        .map((s) => {
          const attach = e.attaches.find((candidate) => candidate.id === s.ref)
          return {
            project: p.slug,
            environment: e.name,
			services: attach?.service ?? '—',
            database: s.target,
            kind: s.kind,
            enabled: backup.enabled,
            frequency: backup.frequency,
            keep: backup.keep,
            encryption: backup.encryption,
            records: 'Environment Backups',
            lastRun: backup.lastRun ?? '—',
            nextRun: backup.nextRun ?? '—',
          }
        })
    }),
  )
  const enabledCount = consumerBackups.filter((c) => c.enabled).length
  const disabledCount = consumerBackups.length - enabledCount
  const noPolicy = (g.consumers?.length ?? 0) - consumerBackups.length

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <RefreshCw className="size-4 text-muted-foreground" /> Backups · per consumer
        </CardTitle>
        <Badge variant={enabledCount > 0 ? 'success' : 'muted'}>{enabledCount} enabled</Badge>
      </CardHeader>
      <CardContent className="flex flex-col gap-4">
        {adapter?.custom ? (
          <p className="text-xs text-muted-foreground">
            Custom hooks do not provide a managed backup adapter. The operator is responsible for this
            service&apos;s backups, whether or not provisioning hooks are configured.
          </p>
        ) : (
          <p className="text-xs text-muted-foreground">
            The backing environment itself <span className="font-medium text-foreground">never runs a backup</span>.
            Every attached environment backs up its own attach through the adapter —{' '}
            <span className="font-mono">pg_dump</span> of exactly that database (shared attaches backed up once, never
            per service). Each consumer&apos;s Backup page selects and schedules its own sources; this page reports
            them.
          </p>
        )}

        {!adapter?.custom && (
          <div className="grid grid-cols-3 gap-3">
            <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface p-3">
              <span className="text-2xl font-semibold text-success">{enabledCount}</span>
              <span className="text-xs text-muted-foreground">consumers backing up (enabled)</span>
            </div>
            <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface p-3">
              <span className="text-2xl font-semibold">{disabledCount}</span>
              <span className="text-xs text-muted-foreground">consumers with backups off</span>
            </div>
            <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface p-3">
              <span className="text-2xl font-semibold">{noPolicy}</span>
              <span className="text-xs text-muted-foreground">attached, no backup source</span>
            </div>
          </div>
        )}

        {consumerBackups.length === 0 ? (
          <div className="text-xs text-muted-foreground">no consumers with backup sources yet</div>
        ) : (
          <div className="overflow-x-auto">
            <Table className="w-full text-sm">
              <TableHeader>
                <TableRow className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                  <TableHead className="py-2 pr-4">Consumer</TableHead>
                  <TableHead className="py-2 pr-4">Database</TableHead>
                  <TableHead className="py-2 pr-4">Kind</TableHead>
                  <TableHead className="py-2 pr-4">Policy</TableHead>
                  <TableHead className="py-2 pr-4">Records</TableHead>
                  <TableHead className="py-2 pr-4">Last run</TableHead>
                  <TableHead className="py-2">Next run</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {consumerBackups.map((c) => (
                  <TableRow key={`${c.project}-${c.environment}-${c.database}`} className="border-b border-border last:border-0">
                    <TableCell className="py-2 pr-4">
                      <span className="font-mono text-xs">
                        {c.project}/{c.environment}
                      </span>
                      <span className="ml-2 font-mono text-[10px] text-muted-foreground">for {c.services}</span>
                    </TableCell>
                    <TableCell className="py-2 pr-4 font-mono text-xs">{c.database}</TableCell>
                    <TableCell className="py-2 pr-4 font-mono text-xs">{c.kind}</TableCell>
                    <TableCell className="py-2 pr-4">
                      <span className="font-mono text-xs">
                        {c.enabled ? `${c.frequency} · keep ${c.keep}` : 'off'}
                      </span>
                      <span className="ml-2 font-mono text-[10px] text-muted-foreground">
                        {c.enabled ? c.encryption : ''}
                      </span>
                    </TableCell>
                    <TableCell className="py-2 pr-4 text-xs">{c.records}</TableCell>
                    <TableCell className="py-2 pr-4 text-xs text-muted-foreground">{c.enabled ? c.lastRun : '—'}</TableCell>
                    <TableCell className="py-2 text-xs text-muted-foreground">{c.enabled ? c.nextRun : '—'}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
