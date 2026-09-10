'use client'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Link } from 'react-router-dom'
import { useNavigate } from 'react-router-dom'
import { useRequiredParams } from '@/lib/router'
import {
  Activity,
  ArrowLeft,
  ArrowUpCircle,
  Boxes,
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Clock,
  Database,
  FileCode2,
  HardDrive,
  History,
  KeyRound,
  Layers,
  Pencil,
  Plug,
  Plus,
  RefreshCw,
  RotateCw,
  Router as RouterIcon,
  ShieldCheck,
  Tag,
  Terminal,
  Trash2,
  X,
} from 'lucide-react'
import { useStore } from '@/lib/store'
import { MAXIMUM_BACKUP_POLICY_KEEP, isValidBackupPolicyKeep } from '@/lib/backup-policy-contract'
import { BlueprintWorkspace } from '@/features/blueprint/blueprint-workspace'
import { EnvironmentRouterUnavailable } from '@/lib/environment-router-unavailable'
import { EnvironmentDeletionFence } from '@/lib/environment-deletion-fence'
import { PageHeader } from '@/components/common/page-header'
import { LogViewer } from '@/features/logs/log-viewer'
import { MetaPill } from '@/components/common/meta-pill'
import { StatCard } from '@/components/common/stat-card'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge, StatusDot } from '@/components/common/status-badge'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Tabs, TabsList, TabsTab, TabsPanel } from '@/components/ui/tabs'
import { Select } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Label } from '@/components/ui/label'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { TaskDetailDrawer as AuthoritativeTaskDrawer } from '@/components/common/task-detail-drawer'
import { RevealValue } from '@/components/common/reveal-value'
import { CopyButton } from '@/components/common/copy-button'
import { EmptyState } from '@/components/common/empty-state'
import { ServiceFormBody } from '@/components/common/service-form-body'
import { ActivityIcon } from '@/components/common/activity-icon'
import { EnvironmentConnectorManager } from '@/features/connectors/environment-connector-manager'
import { EnvironmentVolumeManager } from '@/features/volume/environment-volume-manager'
import { ReleaseGroupsPanel } from '@/features/release-group/release-group-surface'
import { ServiceStateBadges } from '@/features/service/service-runtime-actions'
import { ServiceDetailsDrawer, DetailRow } from './service-details-drawer'
import { ServicesList } from './services-list'
import { ComponentZonePicker } from './component-zone-picker'
import { CaddyTemplateEditor } from './caddy-template-editor'
import { cn, newId } from '@/lib/utils'
import { valkeyAuthenticationDetails } from '@/lib/valkey-authentication'
import type { ActivityEntry, Attach, BackupPolicyReplacement, BackupPolicySourceInput, BackupPolicySourceRecord, Environment, EnvironmentEntry, Route, Service, TaskJournalScope, TaskStep, Zone } from '@/lib/types'

type EnvTab =
  | 'overview'
  | 'services'
  | 'state'
  | 'router'
  | 'releases'
  | 'release-groups'
  | 'tasks'
  | 'backups'
  | 'volumes'
  | 'environments'
  | 'settings'
  | 'scripts'

export default function EnvironmentPage() {
  const params = useRequiredParams('tenant', 'project', 'env')
  const store = useStore()
  const env = store.getEnvironment(params.tenant, params.project, params.env)
  const project = store.getProject(params.tenant, params.project)
  // Deep-linkable tabs: ?tab=tasks opens the Tasks tab (journal click-through).
  const [tab, setTab] = useState<EnvTab>('overview')
  useEffect(() => {
    const t = new URLSearchParams(window.location.search).get('tab')
    if (t && ['overview', 'services', 'state', 'router', 'releases', 'release-groups', 'tasks', 'backups', 'volumes', 'environments', 'settings', 'scripts'].includes(t)) {
      setTab(t as EnvTab)
    }
  }, [])
  if (!env || !project) {
    if (store.tenantsLoading || store.projectsLoading) {
      return <EmptyState icon={<Layers />} title="Loading environment" />
    }
    return (
      <EmptyState
        icon={<Layers />}
        title="Environment not found"
        description={`${params.tenant}/${params.project}/${params.env} does not exist.`}
        action={
          <Link to={`/t/${params.tenant}`}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to tenant
            </Button>
          </Link>
        }
      />
    )
  }
  const provisioningFailed = env.provisioningState === 'failed'
  const serviceCount = provisioningFailed ? '—' : env.services.length
  const unhealthy = env.services.filter((s) => s.status === 'degraded' || s.status === 'failed').length
  const unavailable = env.services.filter((s) => s.status === 'unknown').length
  const deletionFailure = store.getEnvironmentDeletionFailure(env.id)
  const deletionInProgress = env.deletionTaskId !== null || store.isEnvironmentDeletionPending(env.id)
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={
          <>
            {env.name}
            <StatusBadge status={env.status} className="ml-2 align-middle" />
          </>
        }
        description={env.id}
        icon={<Layers />}
        meta={
          <>
            <MetaPill icon={<Tag />}>release {env.release}</MetaPill>
            <MetaPill icon={<Clock />}>deployed {env.lastDeployAt}</MetaPill>
          </>
        }
        actions={
          <div className="flex flex-wrap gap-2">
            <LogViewer target={{ kind: 'environment', id: env.id }} label="Environment logs" />
            <DeployControls key={deletionInProgress ? 'deletion-fenced' : 'editable'} env={env} disabled={deletionInProgress} />
          </div>
        }
      />
      <EnvironmentDeletionFence
        inProgress={deletionInProgress}
        failure={deletionFailure}
        onRetry={() => deletionFailure?.kind === 'task' ? store.retryTask(deletionFailure.taskId) : store.refreshEnvironmentDeletion(env.id)}
        retryLabel={deletionFailure?.kind === 'task' ? 'Retry deletion' : 'Retry refresh'}
      />
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-4">
        <StatCard
          icon={<Boxes />}
          label="Services"
          value={serviceCount}
          hint={provisioningFailed ? 'provisioning failed' : unhealthy ? `${unhealthy} need attention` : unavailable ? `${unavailable} state${unavailable === 1 ? '' : 's'} unavailable` : 'all healthy'}
          tone={provisioningFailed || unhealthy || unavailable ? 'warning' : 'success'}
        />
        <StatCard icon={<Layers />} label="Zones" value={env.zones.length} hint="network zones" />
        <StatCard
          icon={<Plug />}
          label="Routes"
          value={env.routes.length}
          hint={env.routes.some((r) => r.exposure === 'public') ? 'public · needs ingress component' : 'internal only'}
        />
        <StatCard icon={<History />} label="Last deploy" value={env.release} hint={env.lastDeployAt} />
      </div>
      <fieldset key={deletionInProgress ? 'deletion-fenced' : 'editable'} disabled={deletionInProgress} className="contents" aria-label={deletionInProgress ? 'Environment deletion in progress' : undefined}>
      <Tabs value={tab} onValueChange={(v) => setTab(v as EnvTab)}>
        <TabsList>
          <TabsTab value="overview">Overview</TabsTab>
          <TabsTab value="services">Services</TabsTab>
          <TabsTab value="state">Blueprint</TabsTab>
          <TabsTab value="router">Router</TabsTab>
          <TabsTab value="releases">Releases</TabsTab>
          <TabsTab value="release-groups">Release groups</TabsTab>
          <TabsTab value="tasks">Tasks</TabsTab>
          <TabsTab value="backups">Backups</TabsTab>
          <TabsTab value="volumes">Volumes</TabsTab>
          <TabsTab value="environments">Variables</TabsTab>
          <TabsTab value="scripts">Scripts</TabsTab>
          <TabsTab value="settings">Settings</TabsTab>
        </TabsList>
        <TabsPanel value="overview" className="mt-6 flex flex-col gap-6">
          <Topology env={env} />
          <RoutesCard env={env} />
          <AttachesCard env={env} />
        </TabsPanel>
        <TabsPanel value="services" className="mt-6 flex flex-col gap-4">
          <ServicesPanel env={env} />
        </TabsPanel>
        <TabsPanel value="state" className="mt-6 flex flex-col gap-4">
          <BlueprintState env={env} />
        </TabsPanel>
        <TabsPanel value="router" className="mt-6 flex flex-col gap-6">
          <RouterCard env={env} />
        </TabsPanel>
        <TabsPanel value="releases" className="mt-6 flex flex-col gap-4">
          <ReleasesCard env={env} />
        </TabsPanel>
        <TabsPanel value="release-groups" className="mt-6 flex flex-col gap-4">
          <ReleaseGroupsPanel env={env} />
        </TabsPanel>

        <TabsPanel value="tasks" className="mt-6 flex flex-col gap-4">
          <TasksCard env={env} />
        </TabsPanel>

        <TabsPanel value="backups" className="mt-6 flex flex-col gap-4">
          <BackupsCard env={env} />
        </TabsPanel>

        <TabsPanel value="volumes" className="mt-6 flex flex-col gap-4">
          <EnvironmentVolumeManager env={env} />
        </TabsPanel>

        <TabsPanel value="environments" className="mt-6 flex flex-col gap-4">
          <EnvVarsCard env={env} />
          <FactsCard env={env} />
        </TabsPanel>

        <TabsPanel value="scripts" className="mt-6 flex flex-col gap-4">
          <ScriptsCard env={env} />
        </TabsPanel>

        <TabsPanel value="settings" className="mt-6 flex flex-col gap-4">
          <SettingsCard env={env} />
        </TabsPanel>
      </Tabs>
      </fieldset>
    </div>
  )
}

// Deploy / Rollback live in their own component so opening a dialog only
// re-renders this small subtree, not the whole environment page.
function DeployControls({ env, disabled }: { env: Environment; disabled?: boolean }) {
  const [deployOpen, setDeployOpen] = useState(false)
  const [rollbackOpen, setRollbackOpen] = useState(false)
  return (
    <>
      <Button variant="outline" disabled={disabled} onClick={() => setRollbackOpen(true)}>
        <History className="size-4" /> Rollback
      </Button>
      <Button disabled={disabled} onClick={() => setDeployOpen(true)}>
        <ArrowUpCircle className="size-4" /> Deploy
      </Button>
      <DeployDialog env={env} open={deployOpen} onOpenChange={setDeployOpen} />
      <RollbackDialog env={env} open={rollbackOpen} onOpenChange={setRollbackOpen} />
    </>
  )
}

// ---- Deploy: per-service, tag + strategy chosen at deploy time ----

function DeployDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment
  open: boolean
  onOpenChange: (v: boolean) => void
}) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const defaultSvc = env.services.find((s) => s.strategy === 'blue-green') ?? env.services[0]
  const [service, setService] = useState(defaultSvc?.name ?? '')
  const svc = env.services.find((s) => s.name === service)
  // Default = the service's CURRENT TAG only (never the image name): redeploy
  // is the common case (spec / env / secret edits need re-application without
  // a new image). Re-sync on open so a freshly edited image is always the
  // default.
  const [tag, setTag] = useState(imageTag(defaultSvc?.image ?? ''))
  const [strategy, setStrategy] = useState<Service['strategy']>(defaultSvc?.strategy ?? 'recreate')

  useEffect(() => {
    if (open && svc) {
      setTag(imageTag(svc.image))
      setStrategy(svc.strategy)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  if (!svc) return null
  const steps = deploySteps(env, svc.name, strategy)

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="drawer"
      title={`Deploy ${svc.name} · ${env.name}`}
      description="A deployment targets ONE service: pick the immutable image tag and the deploy strategy for this release. The tag defaults to the service's current tag — redeploy re-applies spec, env, and secret changes without a new image. The Controller sequences it; the Agent applies each step and only switches traffic after the healthcheck passes."
      type="deploy"
      target={env.id}
      workspace={params.tenant}
      startLabel="Deploy"
      review={
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-3 gap-3">
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-service">Service</Label>
              <Select
                id="dep-service"
                value={service}
                onValueChange={(v) => {
                  const next = env.services.find((s) => s.name === v)
                  setService(v)
                  if (next) setTag(imageTag(next.image)) // current tag of the newly selected service
                  setStrategy(next?.strategy ?? 'recreate')
                }}
                options={env.services.map((s) => ({ value: s.name, label: s.name }))}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-tag">Image tag</Label>
              <Input id="dep-tag" value={tag} onChange={(e) => setTag(e.target.value)} placeholder="sha-…" />
              <p className="text-xs text-muted-foreground">
                tag only — the image name ({imageName(svc.image)}) is taken from the service. Defaults to the current
                tag; redeploy applies spec / env / secret changes without a new image.
              </p>
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="dep-strategy">Strategy</Label>
              <Select
                id="dep-strategy"
                value={strategy}
                onValueChange={(v) => setStrategy(v as Service['strategy'])}
                options={[
                  { value: 'blue-green', label: 'blue-green' },
                  { value: 'recreate', label: 'recreate' },
                  { value: 'rolling', label: 'rolling (deferred)' },
                ]}
              />
            </div>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2 font-mono text-xs">
            <span className="w-24 text-muted-foreground">Image</span>
            <span className="flex-1 truncate text-right text-muted-foreground">{svc.image}</span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="flex-1 truncate text-foreground">
              {imageName(svc.image)}:{tag}
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2 font-mono text-xs">
            <span className="w-24 text-muted-foreground">Strategy</span>
            <span className="flex-1 text-right text-muted-foreground">declared: {svc.strategy}</span>
            <span className="mx-2 text-muted-foreground">→</span>
            <span className="flex-1 text-foreground">
              {strategy}
              {strategy === 'rolling' ? ' (declared-deferred)' : ''}
            </span>
          </div>
          <p className="text-xs text-muted-foreground">
            A public Route is served only after the required ingress components are enabled through the future live
            Component surface. Creating a Route never enables them.
          </p>
        </div>
      }
      steps={steps}
      onDispatch={() => store.commitDeploy(env.id, svc.name, tag, strategy)}
    />
  )
}

// ---- Rollback: per-service, previous tag tracked and pre-selected ----

function RollbackDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment
  open: boolean
  onOpenChange: (v: boolean) => void
}) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const lastService = env.deploys[0]?.service ?? env.services[0]?.name ?? ''
  const [service, setService] = useState(lastService)
  const svc = env.services.find((s) => s.name === service)
  const history = env.deploys.filter((d) => d.service === service)
  // Rollback target = the most recent SUCCESSFUL deploy whose tag differs
  // from the current one (failed/timed-out records are never selectable,
  // and redeploying the current tag never advances the rollback point).
  const currentTag = history.find((d) => d.status === 'active')?.tag
  const previousTag = history.find((d) => d.status === 'superseded' && d.tag !== currentTag)?.tag ?? ''
  const [tag, setTag] = useState(previousTag)

  if (!svc) return null

  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      variant="drawer"
      title={`Roll back ${svc.name} · ${env.name}`}
      description="Rollback targets the SAME service and tracks its previous tag from deploy history — pre-selected below (editable for an explicit tag). It is a traffic switch, never a cold start, and never reverses migrations."
      type="rollback"
      target={env.id}
      workspace={params.tenant}
      destructive
      confirmText={env.name}
      startLabel="Roll back"
      review={
        <div className="flex flex-col gap-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="flex flex-col gap-1">
              <Label>Service</Label>
              <Select
                value={service}
                onValueChange={(v) => {
                  setService(v)
                  const h = env.deploys.filter((d) => d.service === v)
                  const cur = h.find((d) => d.status === 'active')?.tag
                  setTag(h.find((d) => d.status === 'superseded' && d.tag !== cur)?.tag ?? '')
                }}
                options={env.services.map((s) => ({ value: s.name, label: s.name }))}
              />
            </div>
            <div className="flex flex-col gap-1">
              <Label htmlFor="rb-tag">Previous tag (tracked)</Label>
              <Input id="rb-tag" value={tag} onChange={(e) => setTag(e.target.value)} placeholder="sha-…" />
            </div>
          </div>
          <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
            <span>
              history for <span className="font-mono text-foreground">{service}</span>:{' '}
              {history.length === 0 ? (
                <span className="text-muted-foreground">no prior deploys — nothing to roll back to</span>
              ) : (
                history.map((d) => (
                  <span key={d.id} className="mr-2 font-mono">
                    {d.tag} ({d.when}) {d.status === 'active' ? '· active' : ''}
                  </span>
                ))
              )}
            </span>
            {!previousTag && history.length > 0 && (
              <span className="text-warning">no rollback target — no successful deploy with a tag different from the current one</span>
            )}
          </div>
          <p className="text-xs text-muted-foreground">
            Rollback changes the application image only; it does not reverse database migrations.
          </p>
        </div>
      }
      steps={[
        { label: 'Run pre-rollback hooks', state: 'pending' },
        { label: 'Start previous image in inactive slot', state: 'pending' },
        { label: 'Wait for healthcheck', state: 'pending' },
        { label: 'Reload router (traffic switch)', state: 'pending' },
        { label: 'Run post-rollback hooks', state: 'pending' },
      ]}
      onDispatch={() => store.commitRollback(env.id, svc.name, tag)}
    />
  )
}

// Image name/tag split: "storefront-app:sha-9f3c1ad" -> name "storefront-app",
// tag "sha-9f3c1ad". The last ':' splits unless the remainder looks like a
// registry path (ghcr.io/org/image stays untagged -> "latest").
function imageName(image: string): string {
  const i = image.lastIndexOf(':')
  return i > -1 && !image.slice(i + 1).includes('/') ? image.slice(0, i) : image
}

function imageTag(image: string): string {
  const i = image.lastIndexOf(':')
  return i > -1 && !image.slice(i + 1).includes('/') ? image.slice(i + 1) : 'latest'
}

function deploySteps(env: Environment, serviceName: string, strategy: Service['strategy']): TaskStep[] {
  const serviceId = env.services.find((service) => service.name === serviceName)?.id
  const hasPublic = env.routes.some((route) => route.exposure === 'public' && route.targetServiceId === serviceId)
  if (strategy === 'blue-green')
    return [
      { label: 'Run pre-deploy hooks', state: 'pending' },
      { label: 'Start inactive slot', state: 'pending' },
      { label: 'Wait for healthcheck', state: 'pending' },
      ...(hasPublic
        ? [
            { label: 'Render + validate Caddyfile', state: 'pending' as const },
            { label: 'Reload router (traffic switch)', state: 'pending' as const },
          ]
        : []),
      { label: 'Recreate workers (singleton)', state: 'pending' },
      { label: 'Run post-deploy hooks', state: 'pending' },
    ]
  if (strategy === 'rolling')
    return [
      { label: 'Run pre-deploy hooks', state: 'pending' },
      { label: 'Roll out replicas one by one', state: 'pending' },
      { label: 'Wait for healthcheck on each replica', state: 'pending' },
      { label: 'Run post-deploy hooks', state: 'pending' },
    ]
  return [
    { label: 'Run pre-deploy hooks', state: 'pending' },
    { label: 'Stop current container', state: 'pending' },
    { label: 'Start new container', state: 'pending' },
    { label: 'Wait for healthcheck', state: 'pending' },
    { label: 'Run post-deploy hooks', state: 'pending' },
  ]
}

// Drag-to-scroll for horizontal overflow containers. A drag only starts after
// 5px of movement, and any click that follows a real drag is suppressed so
// cards inside the container keep working normally.
function useDragScroll() {
  const ref = useRef<HTMLDivElement | null>(null)
  const [dragging, setDragging] = useState(false)
  const drag = useRef({ down: false, startX: 0, startScroll: 0, moved: false })

  useEffect(() => {
    const el = ref.current
    if (!el) return
    const d = drag.current

    const down = (e: PointerEvent) => {
      if (e.button !== 0) return
      d.down = true
      d.startX = e.clientX
      d.startScroll = el.scrollLeft
      d.moved = false
    }
    const move = (e: PointerEvent) => {
      if (!d.down) return
      const dx = e.clientX - d.startX
      if (Math.abs(dx) > 5) {
        d.moved = true
        setDragging(true)
      }
      if (d.moved) el.scrollLeft = d.startScroll - dx
    }
    const up = () => {
      d.down = false
      setDragging(false)
    }
    const clickCapture = (e: MouseEvent) => {
      if (d.moved) {
        e.preventDefault()
        e.stopPropagation()
        d.moved = false
      }
    }

    el.addEventListener('pointerdown', down)
    el.addEventListener('pointermove', move)
    el.addEventListener('pointerup', up)
    el.addEventListener('pointercancel', up)
    el.addEventListener('click', clickCapture, true)
    return () => {
      el.removeEventListener('pointerdown', down)
      el.removeEventListener('pointermove', move)
      el.removeEventListener('pointerup', up)
      el.removeEventListener('pointercancel', up)
      el.removeEventListener('click', clickCapture, true)
    }
  }, [])

  return { ref, dragging }
}

// ---- Topology: zone columns with service cards ----

function Topology({ env }: { env: Environment }) {
  const [zoneOpen, setZoneOpen] = useState(false)
  const { ref, dragging } = useDragScroll()
  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <h2 className="flex items-center gap-2 text-sm font-semibold">
          <Layers className="size-4 text-muted-foreground" /> Topology
        </h2>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => setZoneOpen(true)}>
            <Plus className="size-3.5" /> Zone
          </Button>
          <ServiceFormDialog env={env} />
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="size-2 rounded-full bg-success" /> service
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-2 rounded-full bg-muted-foreground" /> attached backing service
        </span>
        <span className="flex items-center gap-1.5">
          <span className="size-2 rounded-full border border-dashed border-muted-foreground" /> no zone
        </span>
        <span className="text-muted-foreground/60">services appear in every zone they join</span>
      </div>
      {env.zones.length === 0 && (
        <p className="text-xs text-muted-foreground">
          no zones yet — add one to start networking; services without a zone land in the column below
        </p>
      )}
      <div
        ref={ref}
        className={cn(
          'flex gap-4 overflow-x-auto pb-2',
          dragging ? 'cursor-grabbing select-none' : 'cursor-grab',
        )}
      >
        {env.zones.map((z) => (
              <ZoneColumn key={z.id} zone={z} env={env} />
        ))}
        <UnzonedColumn env={env} />
      </div>

      <ZoneFormDialog env={env} open={zoneOpen} onOpenChange={setZoneOpen} />
    </div>
  )
}

// Services without a zone (e.g. after a zone removal) stay visible here so
// they can never disappear from the topology: they join no network until
// edited back into one. The column is ALWAYS rendered — even with zero
// zones — so unzoned services can never vanish from view.
function UnzonedColumn({ env }: { env: Environment }) {
  const unzoned = env.services.filter((s) => s.zones.length === 0)
  return (
    <div className="flex min-w-[240px] flex-1 flex-col gap-2 rounded-xl border border-dashed border-muted-foreground/40 bg-card p-3">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm font-semibold text-muted-foreground">no zone</span>
        <span className="font-mono text-[11px] text-muted-foreground">no network</span>
      </div>
      <div className="text-xs text-muted-foreground">
        not attached to any network — they can talk to nothing until you edit them into a zone
      </div>
      <div className="mt-1 flex flex-col gap-1.5">
        {unzoned.length === 0 && <div className="text-xs text-muted-foreground/60">no services</div>}
        {unzoned.map((s) => (
          <ServiceCard key={s.id} service={s} env={env} />
        ))}
      </div>
    </div>
  )
}

function ZoneColumn({ zone, env }: { zone: Zone; env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [detailOpen, setDetailOpen] = useState(false)
  const [detailZone, setDetailZone] = useState<Zone>()
  const [detailError, setDetailError] = useState<string>()
  const [removeOpen, setRemoveOpen] = useState(false)
  const [removalImpact, setRemovalImpact] = useState<Awaited<ReturnType<typeof store.getZoneRemovalImpact>>>()
  const [removalImpactError, setRemovalImpactError] = useState<string>()
  const services = env.services.filter((s) => s.zones.includes(zone.name))
  const attaches = env.attaches.filter((a) => {
    const g = store.getBackingProject(a.projectId)
    return (g?.environments?.[0]?.zones ?? []).some((z) => z.name === zone.name)
  })
  const impactServices = removalImpact?.services ?? []
  const impactAttaches = removalImpact?.attaches ?? []
  const impactDatabases = removalImpact?.databases ?? []
  return (
    <div className="flex min-w-[240px] flex-1 flex-col gap-2 rounded-xl border border-border bg-card p-3">
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-sm font-semibold text-primary">{zone.name}</span>
        <span className="flex items-center gap-1">
          <span className="font-mono text-[11px] text-muted-foreground">{zone.subnet}</span>
          <Button
            variant="ghost"
            size="icon-xs"
            data-action-id="zone.show"
            aria-label={`Show Zone ${zone.name}`}
            title="Show zone details"
            onClick={() => {
              setDetailOpen(true)
              setDetailZone(undefined)
              setDetailError(undefined)
              void store.getZone(zone.id).then(setDetailZone).catch((error: unknown) => {
                setDetailError(error instanceof Error ? error.message : 'Unable to load Zone details')
              })
            }}
          >
            <ChevronRight className="size-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="icon-xs"
            className="text-muted-foreground hover:text-destructive"
            onClick={() => {
              setRemovalImpact(undefined)
              setRemovalImpactError(undefined)
              setRemoveOpen(true)
              void store.getZoneRemovalImpact(zone.id).then(setRemovalImpact).catch((error: unknown) => {
                setRemovalImpactError(error instanceof Error ? error.message : 'Unable to load zone removal impact')
              })
            }}
            title="Remove zone"
          >
            <Trash2 className="size-3.5" />
          </Button>
        </span>
      </div>
      <div className="text-xs text-muted-foreground">
        {zone.ownerKind === 'environment' ? 'Environment-owned' : 'Backing Project-owned'} · {zone.internal ? 'internal' : 'egress allowed'}
      </div>
      <div className="mt-1 flex flex-col gap-1.5">
        {services.length === 0 && <div className="text-xs text-muted-foreground/60">no services</div>}
        {services.map((s) => (
          <ServiceCard key={s.id} service={s} env={env} />
        ))}
        {attaches.map((a) => (
          <div
            key={a.id}
            className="flex items-start gap-1 rounded-lg border border-dashed border-border bg-surface/40 px-2 py-1.5"
          >
            <Link
              to={`/platform/backing-services/${a.projectId}`}
              className="flex min-w-0 flex-1 items-start gap-2 transition-colors hover:border-ring/50"
            >
              <Database className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
              <div className="flex min-w-0 flex-col">
                <span className="font-mono text-xs font-medium">{a.projectId}</span>
                <span className="truncate font-mono text-[11px] text-muted-foreground">
                  {a.database !== '—' ? `db ${a.database} · role ${a.role}` : 'attached'}
				  {a.service ? ` · for ${a.service}` : ''} · open backing service →
                </span>
              </div>
            </Link>
            <div className="flex items-center gap-1">
              <RenameAttach env={env} attach={a} />
              <DetachAttach env={env} attach={a} />
            </div>
          </div>
        ))}
      </div>

      <Drawer open={detailOpen} onOpenChange={setDetailOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Zone details · {zone.name}</DialogTitle>
          </DialogHeader>
          {!detailZone && !detailError ? <p role="status" className="text-sm text-muted-foreground">Loading Zone details...</p> : null}
          {detailError ? <p role="alert" className="text-sm text-destructive">{detailError}</p> : null}
          {detailZone ? (
            <div className="flex flex-col">
              <DetailRow label="ID" value={detailZone.id} mono />
              <DetailRow label="Environment" value={detailZone.environmentId} mono />
              <DetailRow label="Name" value={detailZone.name} mono />
              <DetailRow label="Subnet" value={detailZone.subnet} mono />
              <DetailRow label="Internal" value={detailZone.internal ? 'yes' : 'no'} />
              <DetailRow label="Owner kind" value={detailZone.ownerKind} />
              <DetailRow label="Owner ID" value={detailZone.ownerId} mono />
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDetailOpen(false)}>Close</Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      <TaskRunnerDialog
        open={removeOpen}
        onOpenChange={setRemoveOpen}
        title={`Remove zone · ${zone.name}`}
        description={`Removes ${zone.name} (${zone.subnet}) and disconnects ${services.length} joined service${services.length === 1 ? '' : 's'}.`}
        type="destroy"
        target={zone.id}
        workspace={params.tenant}
        destructive
        confirmText={zone.name}
        startLabel="Remove zone"
        review={
          <div className="flex flex-col gap-2 text-xs">
            {removalImpactError ? <p className="text-destructive">{removalImpactError}</p> : null}
            {!removalImpact && !removalImpactError ? (
              <p className="text-muted-foreground">Loading exact removal impact...</p>
            ) : null}
            {removalImpact ? (
              <>
				<p>{impactServices.length} affected service{impactServices.length === 1 ? '' : 's'}</p>
				{impactServices.map((service) => (
                  <p key={service.id} className="font-mono">{service.name} ({service.id})</p>
                ))}
				<p>{impactAttaches.length} detach{impactAttaches.length === 1 ? '' : 'es'}</p>
				{impactAttaches.map((attach) => (
                  <p key={attach.id} className="font-mono">{attach.name} ({attach.id})</p>
                ))}
				<p>{impactDatabases.length} provisioned database{impactDatabases.length === 1 ? '' : 's'}</p>
				{impactDatabases.map((database) => (
                  <p key={database.attach_id} className="font-mono">{database.name}</p>
                ))}
              </>
            ) : null}
          </div>
        }
        steps={[
          { label: 'Validate dependent services and attaches', state: 'pending' },
          { label: `Remove network ${zone.name}`, state: 'pending' },
          { label: 'Update affected service memberships', state: 'pending' },
        ]}
        onDispatch={() => {
          if (!removalImpact) return Promise.reject(new Error('Exact zone removal impact is not loaded'))
		  return store.removeZone(env.id, zone.id, removalImpact.impact_token)
        }}
      />
    </div>
  )
}

function ServiceCard({ service, env }: { service: Service; env: Environment }) {
  const store = useStore()
  const attached = env.attaches.filter((a) => a.service === service.name)
  const [open, setOpen] = useState(false)
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        title={`Edit ${service.name}`}
        className="flex items-start gap-2 rounded-lg border border-border bg-surface px-2 py-1.5 text-left transition-colors hover:border-ring"
      >
        <StatusDot status={service.status} className="mt-1.5" />
        <div className="flex min-w-0 flex-col">
          <span className="font-mono text-xs font-medium">{service.name}</span>
          <ServiceStateBadges service={service} compact />
          <span className="truncate text-[11px] text-muted-foreground">{service.role}</span>
          <span className="truncate font-mono text-[10px] text-muted-foreground/60">{service.image}</span>
          <span className="truncate text-[10px] text-muted-foreground/70">
            zones: {service.zones.join(', ') || 'none'}
          </span>
          <div className="mt-1 flex flex-wrap gap-1">
            {service.resources && (
              <span className="rounded-full bg-primary/10 px-1.5 py-0.5 font-mono text-[10px] text-primary">
                {service.resources.mem} · {service.resources.cpus} cpu
              </span>
            )}
            {service.healthcheck && (
              <span className="rounded-full bg-secondary px-1.5 py-0.5 font-mono text-[10px] text-secondary-foreground">
                {service.healthcheck.kind === 'http'
                  ? `hc ${service.healthcheck.target}`
                  : service.healthcheck.kind === 'tcp'
                    ? `tcp ${service.healthcheck.target}`
                    : `pgrep ${service.healthcheck.target}`}
              </span>
            )}
            {service.strategy !== 'recreate' && (
              <span className="rounded-full bg-warning/10 px-1.5 py-0.5 font-mono text-[10px] text-warning">{service.strategy}</span>
            )}
            {service.replicas > 1 && (
              <span className="rounded-full bg-secondary px-1.5 py-0.5 font-mono text-[10px] text-secondary-foreground">
                ×{service.replicas}
              </span>
            )}
            {attached.map((a) => {
              const g = store.getBackingProject(a.projectId)
              return (
                <Link
                  key={a.id}
                  to={`/platform/backing-services/${a.projectId}`}
                  className="flex items-center gap-1 rounded-full bg-primary/10 px-1.5 py-0.5 font-mono text-[10px] text-primary transition-colors hover:bg-primary/20"
                >
                  <Plug className="size-2.5" />
                  {g?.environments?.[0]?.services[0]?.serviceName ?? a.projectId}
                  {a.database !== '—' ? ` · ${a.database}` : ''}
                </Link>
              )
            })}
          </div>
        </div>
      </button>
		<ServiceDetailsDrawer
			env={env}
			service={service}
			open={open}
			onOpenChange={setOpen}
		/>
    </>
  )
}

function ServicesPanel({ env }: { env: Environment }) {
  const count = env.services.length
  return (
    <section aria-labelledby="environment-services-heading" className="flex flex-col gap-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 id="environment-services-heading" className="flex items-center gap-2 text-sm font-semibold">
            <Boxes className="size-4 text-muted-foreground" /> Services
          </h2>
          <p className="mt-1 text-xs text-muted-foreground">
            Inspect each service's desired configuration, runtime state, health, image, and network zones.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="outline" className="font-mono">
            {count} {count === 1 ? 'service' : 'services'}
          </Badge>
          <ServiceFormDialog env={env} />
        </div>
      </div>
      {count === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No services yet"
          description="Add a service to start building this environment's workload."
          action={<ServiceFormDialog env={env} />}
        />
      ) : (
        <ServicesList env={env} />
      )}
    </section>
  )
}

// ---- Routes ----

function RoutesCard({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [open, setOpen] = useState(false)
  const [detailTarget, setDetailTarget] = useState<Route | null>(null)
  const [detailRoute, setDetailRoute] = useState<Route>()
  const [detailError, setDetailError] = useState<string>()
  const [editing, setEditing] = useState<Route | null>(null)
  const [removing, setRemoving] = useState<Route | null>(null)
  const [exposure, setExposure] = useState<Route['exposure']>('internal')
  const [editSubmitting, setEditSubmitting] = useState(false)
  const [editError, setEditError] = useState<string>()

  const saveExposure = async () => {
    if (!editing) return
    setEditSubmitting(true)
    setEditError(undefined)
    try {
      await store.updateRoute(env.id, editing.id, { exposure })
      setEditing(null)
    } catch (error) {
      setEditError(error instanceof Error ? error.message : 'Unable to edit Route')
    } finally {
      setEditSubmitting(false)
    }
  }
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <RouterIcon className="size-4 text-muted-foreground" /> Routes
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Plus className="size-3.5" /> Route
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-1.5">
        {env.routes.map((r) => (
          <div key={r.id} className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <div className="flex min-w-0 items-center gap-3">
              <ExposurePill exposure={r.exposure} />
              <span className="truncate font-mono text-sm">{r.host || 'internal'}{r.path}</span>
            </div>
            <div className="flex min-w-0 items-center gap-1">
              <span className="truncate font-mono text-xs text-muted-foreground">
                → {env.services.find((service) => service.id === r.targetServiceId)?.name ?? r.targetServiceId}:{r.targetPort}
              </span>
              <Button
                variant="ghost"
                size="icon-xs"
                data-action-id="route.show"
                aria-label={`Show Route ${r.host || 'internal'}${r.path}`}
                title="Show route details"
                onClick={() => {
                  setDetailTarget(r)
                  setDetailRoute(undefined)
                  setDetailError(undefined)
                  void store.getRoute(r.id).then(setDetailRoute).catch((error: unknown) => {
                    setDetailError(error instanceof Error ? error.message : 'Unable to load Route details')
                  })
                }}
              >
                <ChevronRight className="size-3.5" />
              </Button>
              <Button
                variant="ghost"
                size="icon-xs"
                title="Edit route exposure"
                onClick={() => {
                  setExposure(r.exposure)
                  setEditError(undefined)
                  setEditing(r)
                }}
              >
                <Pencil className="size-3.5" />
              </Button>
              <Button variant="ghost" size="icon-xs" className="text-muted-foreground hover:text-destructive" title="Remove route" onClick={() => setRemoving(r)}>
                <Trash2 className="size-3.5" />
              </Button>
            </div>
          </div>
        ))}
        {env.routes.length === 0 && (
          <div className="text-xs text-muted-foreground">no routes — add one; public routes need an ingress component (Router tab) to be served</div>
        )}
      </CardContent>
      <RouteFormDialog env={env} open={open} onOpenChange={setOpen} />
      <Drawer open={detailTarget !== null} onOpenChange={(next) => !next && setDetailTarget(null)}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Route details · {detailTarget ? `${detailTarget.host || 'internal'}${detailTarget.path}` : ''}</DialogTitle>
          </DialogHeader>
          {!detailRoute && !detailError ? <p role="status" className="text-sm text-muted-foreground">Loading Route details...</p> : null}
          {detailError ? <p role="alert" className="text-sm text-destructive">{detailError}</p> : null}
          {detailRoute ? (
            <div className="flex flex-col">
              <DetailRow label="ID" value={detailRoute.id} mono />
              <DetailRow label="Environment" value={detailRoute.environmentId} mono />
              <DetailRow label="Host" value={detailRoute.host || 'hostless internal'} mono />
              <DetailRow label="Path" value={detailRoute.path} mono />
              <DetailRow label="Exposure" value={detailRoute.exposure} />
              <DetailRow label="Target Service" value={detailRoute.targetServiceId} mono />
              <DetailRow label="Target port" value={String(detailRoute.targetPort)} mono />
            </div>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setDetailTarget(null)}>Close</Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <Drawer open={!!editing} onOpenChange={(next) => {
        if (!next) {
          setEditing(null)
          setEditError(undefined)
        }
      }}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Edit route · {editing ? `${editing.host || 'internal'}${editing.path}` : ''}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="route-edit-exposure">Exposure</Label>
              <Select
                id="route-edit-exposure"
                value={exposure}
                onValueChange={(value) => setExposure(value as Route['exposure'])}
                options={[
                  { value: 'public', label: 'public — needs ingress component' },
                  { value: 'internal', label: 'internal — no host port' },
                ]}
              />
            </div>
            <p className="text-xs text-muted-foreground">The route host, path, target service, and target port remain unchanged.</p>
            {editError ? <p role="alert" className="text-sm text-destructive">{editError}</p> : null}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setEditing(null)}>Cancel</Button>
            <Button
              data-action-id="route.edit"
              disabled={editSubmitting}
              onClick={() => void saveExposure()}
            >
              {editSubmitting ? 'Saving...' : 'Save exposure'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove route · ${removing ? `${removing.host || 'internal'}${removing.path}` : ''}`}
        description="Removes this route from the environment and reconciles the router configuration."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        confirmText={removing ? `${removing.host || 'internal'}${removing.path}` : ''}
        startLabel="Remove route"
        steps={[
          { label: 'Validate the route record', state: 'pending' },
          { label: 'Remove the route', state: 'pending' },
          { label: 'Reconcile router configuration', state: 'pending' },
        ]}
        onDispatch={async () => {
          if (!removing) throw new Error('No Route selected for removal')
          return store.removeRoute(env.id, removing.id)
        }}
      />
    </Card>
  )
}

export function ExposurePill({ exposure }: { exposure: Route['exposure'] }) {
  if (exposure === 'public')
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-success/10 px-2 py-0.5 text-xs text-success">
        <span className="size-1.5 rounded-full bg-current" /> public
      </span>
    )
  if (exposure === 'internal')
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-warning/10 px-2 py-0.5 text-xs text-warning">
        <span className="size-1.5 rounded-full bg-current" /> internal
      </span>
    )
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
      <span className="size-1.5 rounded-full bg-current" /> loopback
    </span>
  )
}

// ---- Attaches ----

function AttachesCard({ env }: { env: Environment }) {
  const [open, setOpen] = useState(false)
  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between">
        <CardTitle className="flex items-center gap-2">
          <Plug className="size-4 text-muted-foreground" /> Attached backing services
        </CardTitle>
        <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
          <Plus className="size-3.5" /> Attach backing
        </Button>
      </CardHeader>
      <CardContent className="flex flex-col gap-1.5">
        {env.attaches.map((a) => (
          <div key={a.id} className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2">
            <Link to={`/platform/backing-services/${a.projectId}`} className="flex min-w-0 flex-1 items-center justify-between gap-2 transition-colors hover:border-ring/50">
              <span className="font-mono text-sm">{a.projectId}</span>
              <span className="truncate font-mono text-xs text-muted-foreground">
                {a.database !== '—' ? `database ${a.database} · role ${a.role}` : 'attached'}
                {a.service ? ` · for ${a.service}` : ''}
              </span>
            </Link>
            <DetachAttach env={env} attach={a} />
          </div>
        ))}
        {env.attaches.length === 0 && (
          <div className="text-xs text-muted-foreground">
            no backing service attached — attach a running backing service to a service to provision its own database + role
          </div>
        )}
      </CardContent>
      <AttachFormDialog env={env} open={open} onOpenChange={setOpen} />
    </Card>
  )
}

// ---- Blueprint ----

function BlueprintState({ env }: { env: Environment }) {
  const params = useRequiredParams('tenant')
  return <BlueprintWorkspace environment={env} workspace={params.tenant} />
}

// ---- Router ----

// C07 may summarize Route desired state, but live Router component state and
// controls belong to the C12/C14 capability surfaces.
function RouterCard({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const routerReady = env.components.some((component) => component.kind === 'caddy') && env.components.some((component) => component.kind === 'cloudflare-tunnel')
  const publicRoutes = env.routes.filter((route) => route.exposure === 'public')
  const caddy = env.components.find((component) => component.kind === 'caddy')
  const tunnel = env.components.find((component) => component.kind === 'cloudflare-tunnel')
  const tunnelSecrets = store.reusableSecrets.filter((secret) =>
    secret.kind === 'env' && (secret.scope === 'platform' || secret.projectId === env.projectId),
  )
  const [operation, setOperation] = useState<{
    component: Environment['components'][number]
    action: 'enable' | 'disable' | 'update' | 'config'
  } | null>(null)
  const [selectedZoneIds, setSelectedZoneIds] = useState<string[]>([])
  const [createdComponentZones, setCreatedComponentZones] = useState<Zone[]>([])
  const [creatingComponentZone, setCreatingComponentZone] = useState(false)
  const [routerAlias, setRouterAlias] = useState(caddy?.config?.alias ?? '')
  const [caddyTemplate, setCaddyTemplate] = useState(caddy?.config?.caddyfile_template ?? '')
  const [caddyTemplateBlocked, setCaddyTemplateBlocked] = useState(false)
  const [tunnelCredentialMode, setTunnelCredentialMode] = useState<'existing' | 'new'>('existing')
  const [tunnelSecret, setTunnelSecret] = useState(tunnel?.config?.secret_id ?? '')
  const [tunnelSecretName, setTunnelSecretName] = useState('CLOUDFLARE_TUNNEL_TOKEN')
  const [tunnelToken, setTunnelToken] = useState('')
  if (!routerReady) return <EnvironmentRouterUnavailable />

  const openConfig = (component: Environment['components'][number], action: 'config' | 'enable' = 'config') => {
    setSelectedZoneIds(component.config?.zone_ids ?? [])
    setCreatedComponentZones([])
    if (component.kind === 'caddy') {
      setRouterAlias(component.config?.alias ?? '')
      setCaddyTemplate(component.config?.caddyfile_template ?? '')
      setCaddyTemplateBlocked(false)
    } else {
      setTunnelCredentialMode('existing')
      setTunnelSecret(component.config?.secret_id ?? '')
      setTunnelToken('')
    }
    setOperation({ component, action })
  }

  const configuring = operation?.action === 'config' || operation?.action === 'enable'
  const availableComponentZones = [...env.zones, ...createdComponentZones.filter((zone) => !env.zones.some((current) => current.id === zone.id))]
  const tunnelHasEgress = selectedZoneIds.some((id) => availableComponentZones.some((zone) => zone.id === id && !zone.internal))
  const operationDisabled = configuring && (creatingComponentZone || selectedZoneIds.length === 0 || (
    operation.component.kind === 'caddy'
      ? (routerAlias !== '' && !/^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$/.test(routerAlias)) || caddyTemplateBlocked
      : !tunnelHasEgress || (tunnelCredentialMode === 'existing'
        ? !tunnelSecret
        : !tunnelSecretName || !tunnelToken)
  ))

  return (
    <>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <RouterIcon className="size-4 text-muted-foreground" /> Router and tunnel components
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {env.components.map((component) => {
            return (
              <div key={component.id} className="flex flex-col gap-3 rounded-lg border border-border bg-surface p-3">
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div>
                    <div className="flex items-center gap-2">
                      <span className="font-medium">{component.kind === 'caddy' ? 'Caddy' : 'Cloudflare Tunnel'}</span>
                      <StatusBadge status={component.status} />
                    </div>
                    <p className="mt-1 font-mono text-xs text-muted-foreground">{component.id}</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <Button variant="outline" size="sm" onClick={() => openConfig(component)}>Configure</Button>
                    <Button variant="outline" size="sm" onClick={() => setOperation({ component, action: 'update' })}>Update</Button>
                    <Button
                      size="sm"
                      variant={component.enabled ? 'destructive' : 'default'}
                      onClick={() => component.enabled ? setOperation({ component, action: 'disable' }) : openConfig(component, 'enable')}
                    >
                      {component.enabled ? 'Disable' : 'Enable'}
                    </Button>
                  </div>
                </div>
                {component.kind === 'caddy' ? (
                  <div className="grid gap-1 font-mono text-xs text-muted-foreground sm:grid-cols-2">
                    <span>zones={component.config?.zone_ids.join(', ') || 'not configured'}</span>
                    <span>ipv4={component.state.pinnedIPv4 || 'not allocated'}</span>
                  </div>
                ) : (
                  <div className="grid gap-1 font-mono text-xs text-muted-foreground sm:grid-cols-2">
                    <span>secret={component.config?.secret_id || 'not configured'}</span>
                    <span>zones={component.config?.zone_ids.join(', ') || 'not configured'}</span>
                    <span>routing=provider managed</span>
                  </div>
                )}
              </div>
            )
          })}
          <div className="rounded-lg border border-border bg-background px-3 py-2 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">Public Routes:</span>{' '}
            {publicRoutes.length > 0 ? publicRoutes.map((route) => route.host).filter(Boolean).join(', ') : 'none'}
          </div>
          <p className="text-xs text-muted-foreground">
            Groundplane starts the outbound connector from its Secret and reports health. DNS, public hostnames, ingress rules,
            origin targets, and protocol remain provider-managed.
          </p>
        </CardContent>
      </Card>

      {operation && (
        <TaskRunnerDialog
          open
          onOpenChange={(open) => { if (!open) setOperation(null) }}
          variant={configuring ? 'drawer' : 'dialog'}
          title={`${operation.action === 'config' ? 'Configure' : operation.action} ${operation.component.kind}`}
          description="The Controller updates the Environment Blueprint, renders the complete candidate, and assigns one reconciliation Task to the Agent."
          type="run"
          target={operation.component.id}
          workspace={params.tenant}
          startLabel={operation.action === 'config' ? 'Save and reconcile' : `${operation.action} component`}
          startDisabled={operationDisabled}
          review={configuring ? (
            <div className="flex flex-col gap-4">
              <ComponentZonePicker
                zones={availableComponentZones}
                selectedZoneIds={selectedZoneIds}
                onChange={setSelectedZoneIds}
                networkPool={env.networkPool}
                onCreate={async (input) => {
                  setCreatingComponentZone(true)
                  try {
                    const zone = await store.addZone(env.id, input)
                    setCreatedComponentZones((current) => [...current, zone])
                    return zone
                  } finally {
                    setCreatingComponentZone(false)
                  }
                }}
              />
              {operation.component.kind === 'caddy' ? (
                <div className="flex flex-col gap-1">
                  <Label htmlFor="component-primary-zone">Primary Router Zone</Label>
                  <Select
                    id="component-primary-zone"
                    value={selectedZoneIds[0] ?? ''}
                    onValueChange={(id) => setSelectedZoneIds((current) => [id, ...current.filter((value) => value !== id)])}
                    options={selectedZoneIds.map((id) => ({ value: id, label: availableComponentZones.find((zone) => zone.id === id)?.name ?? id }))}
                  />
                  <p className="text-xs text-muted-foreground">The pinned IPv4 and host/LAN DNS address belong to this Zone. Other selected interfaces use dynamic addresses.</p>
                </div>
              ) : (
                <p className="text-xs text-muted-foreground">Select at least one non-internal Zone. The first selected non-internal Zone supplies outbound connectivity.</p>
              )}
              {operation.component.kind === 'caddy' ? (
              <div className="flex flex-col gap-3">
                <div className="flex flex-col gap-1">
                  <Label htmlFor="router-alias">Router alias (optional)</Label>
                  <Input id="router-alias" value={routerAlias} maxLength={63} placeholder="kobwnewe-router" onChange={(event) => setRouterAlias(event.target.value)} aria-describedby="router-alias-help" />
                  <p id="router-alias-help" className="text-xs text-muted-foreground">One lowercase DNS label on the primary network. HTTP uses port 80. Leave empty to clear.</p>
                  <CaddyTemplateEditor key={operation.component.id} componentId={operation.component.id}
                    enabled={operation.component.enabled} value={caddyTemplate} onChange={setCaddyTemplate}
                    onBlockedChange={setCaddyTemplateBlocked} />
                </div>
              </div>
            ) : (
              <div className="flex flex-col gap-3">
                <div className="flex flex-col gap-1">
                  <Label htmlFor="component-credential-source">Credential source</Label>
                  <Select
                    id="component-credential-source"
                    value={tunnelCredentialMode}
                    onValueChange={(value) => setTunnelCredentialMode(value as 'existing' | 'new')}
                    options={[
                      { value: 'existing', label: 'Existing Secret' },
                      { value: 'new', label: 'New token' },
                    ]}
                  />
                </div>
                {tunnelCredentialMode === 'existing' ? (
                  <div className="flex flex-col gap-1">
                    <Label htmlFor="component-reusable-secret">Reusable Secret</Label>
                    <Select
                      id="component-reusable-secret"
                      value={tunnelSecret}
                      onValueChange={setTunnelSecret}
                      options={tunnelSecrets.map((secret) => ({
                        value: secret.id,
                        label: secret.key + ' · ' + secret.id,
                      }))}
                    />
                    {tunnelSecrets.length === 0 && (
                      <p className="text-xs text-warning">
                        Create a Project or platform env-var Secret first, or choose New token.
                      </p>
                    )}
                  </div>
                ) : (
                  <>
                    <div className="flex flex-col gap-1">
                      <Label htmlFor="tunnel-secret-name">Secret name</Label>
                      <Input
                        id="tunnel-secret-name"
                        value={tunnelSecretName}
                        onChange={(event) => setTunnelSecretName(event.target.value)}
                      />
                    </div>
                    <div className="flex flex-col gap-1">
                      <Label htmlFor="tunnel-token">Tunnel token</Label>
                      <Input
                        id="tunnel-token"
                        type="password"
                        value={tunnelToken}
                        onChange={(event) => setTunnelToken(event.target.value)}
                      />
                      <p className="text-xs text-muted-foreground">
                        The token is write-only and becomes a Project Secret.
                      </p>
                    </div>
                  </>
                )}
              </div>
            )}
            </div>
          ) : undefined}
          steps={[
            { label: 'Validate Component desired state', state: 'pending' },
            { label: 'Render generated services and configuration', state: 'pending' },
            { label: 'Apply the complete Environment Compose candidate', state: 'pending' },
            { label: 'Publish observed Component state', state: 'pending' },
          ]}
          onDispatch={() => {
            if (operation.action === 'disable') return store.setComponentEnabled(operation.component.id, false)
            if (operation.action === 'update') return store.reconcileEnvironmentComponent(operation.component.id)
            const config = operation.component.kind === 'caddy'
              ? {
                  zone_ids: selectedZoneIds,
                  alias: routerAlias,
                  ...(caddyTemplate ? { caddyfile_template: caddyTemplate } : {}),
                }
              : {
                  zone_ids: selectedZoneIds,
                  credential: tunnelCredentialMode === 'existing'
                    ? { mode: 'existing' as const, secret_id: tunnelSecret }
                    : { mode: 'new' as const, secret_name: tunnelSecretName, token: tunnelToken },
                }
            return operation.action === 'enable'
              ? store.setComponentEnabled(operation.component.id, true, config)
              : store.updateComponentConfig(operation.component.id, config)
          }}
          onCommit={() => { void store.refreshEnvironmentComponents(env.id).catch(() => undefined) }}
        />
      )}
    </>
  )
}

// ---- Deploys ----

// Detach is a task, like attach: the adapter deprovisions (revoke grants →
// drop role → optionally drop database) and the desired-state record is
// removed as part of that task — never a plain record delete.
function RenameAttach({ env, attach }: { env: Environment; attach: Attach }) {
  const store = useStore()
  const [open, setOpen] = useState(false)
  const [name, setName] = useState(attach.name)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string>()
  return (
    <>
      <Button
        variant="ghost"
        size="icon-xs"
        title={`Rename ${attach.name}`}
        onClick={() => {
          setName(attach.name)
          setError(undefined)
          setOpen(true)
        }}
      >
        <Pencil className="size-3.5" />
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rename Attach</DialogTitle>
            <DialogDescription>
              Changes the Environment-scoped spec key. Stable identity, facts, grants, and network membership do not change.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor={`attach-rename-${attach.id}`}>Attach name</Label>
            <Input
              id={`attach-rename-${attach.id}`}
              value={name}
              onChange={(event) => setName(event.target.value)}
              className="font-mono"
              autoFocus
            />
            {error ? <p className="text-xs text-destructive">{error}</p> : null}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>Cancel</Button>
            <Button
              disabled={saving || !name.trim() || name.trim() === attach.name}
              onClick={() => {
                setSaving(true)
                setError(undefined)
                void store
                  .renameAttach(env.id, attach.id, name.trim())
                  .then(() => setOpen(false))
                  .catch((cause: unknown) => {
                    setError(cause instanceof Error ? cause.message : 'Unable to rename Attach')
                  })
                  .finally(() => setSaving(false))
              }}
            >
              {saving ? 'Saving…' : 'Rename'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
function DetachAttach({ env, attach }: { env: Environment; attach: Attach }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [open, setOpen] = useState(false)
  const g = store.getBackingProject(attach.projectId)
  const manual = g ? store.adapters.find((a) => a.key === g.environments?.[0]?.services[0]?.adapter)?.manual : false
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="rounded-md p-1 text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-destructive focus-visible:ring-2 focus-visible:ring-ring"
        title={`Detach ${g?.name ?? attach.projectId}`}
        aria-label="Detach"
      >
        <Trash2 className="size-3.5" />
      </button>
      <TaskRunnerDialog
        open={open}
        onOpenChange={setOpen}
        title={`Detach ${g?.name ?? attach.projectId}`}
        description={
          manual
            ? 'Manual adapter: detaching only removes the network join — there is nothing to deprovision.'
            : "Runs the adapter's deprovision: revoke grants → drop role → optionally drop database. The desired-state record is removed as part of the task, never by a plain delete."
        }
        type="detach"
        target={env.id}
        workspace={params.tenant}
        destructive
        confirmText={g?.name ?? 'detach'}
        startLabel="Detach"
        steps={
          manual
            ? [{ label: 'remove network join', state: 'pending' }]
            : [
                { label: `revoke grants on ${attach.database}`, state: 'pending' },
                { label: `drop role ${attach.role}`, state: 'pending' },
                { label: attach.database !== '—' ? `drop database ${attach.database}` : 'no database to drop', state: 'pending' },
                { label: 'remove desired-state record', state: 'pending' },
              ]
        }
        onDispatch={() => store.removeAttach(env.id, attach.id)}
      />
    </>
  )
}
// ---- Tasks: the environment's live task queue ----
// Compact list — click a task to open the detail drawer with the full
// procedure and controls (abort, retry).
function TasksCard({ env }: { env: Environment }) {
  const store = useStore()
  const [filter, setFilter] = useState('all')
  const scope = useMemo<TaskJournalScope>(() => ({ kind: 'environment', environmentId: env.id }), [env.id])
  const journal = store.getTaskJournal(scope)
  const tasks = journal.entries
  const running = tasks.filter((t) => t.status === 'running' || t.status === 'pending').length
  const [open, setOpen] = useState<ActivityEntry | null>(null)
  useEffect(() => {
    void store.loadTaskJournal('tasks', scope).catch(() => undefined)
  }, [scope, store.loadTaskJournal])

  const filters: { key: string; label: string; match: (t: ActivityEntry) => boolean }[] = [
    { key: 'all', label: 'All', match: () => true },
    { key: 'inflight', label: 'In-flight', match: (t) => t.status === 'running' },
    { key: 'queued', label: 'Queued', match: (t) => t.status === 'pending' },
    { key: 'completed', label: 'Completed', match: (t) => t.status === 'completed' },
    { key: 'failed', label: 'Failed', match: (t) => t.status === 'failed' || t.status === 'timed_out' || t.status === 'aborted' },
  ]
  const visible = tasks.filter((t) => filters.find((f) => f.key === filter)?.match(t) ?? true)

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <CardTitle className="flex items-center gap-2">
          <Activity className="size-4 text-muted-foreground" /> Tasks · this environment
        </CardTitle>
        <div className="flex items-center gap-2">
          <Badge variant={running > 0 ? 'success' : 'muted'}>{running > 0 ? <>{running} in-flight</> : 'idle'}</Badge>
          <Button variant="outline" size="sm" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('tasks', scope).catch(() => undefined)}>
            <RefreshCw className="size-4" /> Refresh
          </Button>
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-2">
        <div className="flex items-center justify-between gap-2">
          <p className="text-xs text-muted-foreground">
            The live task queue — the same stream the Agent pulls from. Click a task to open its details and controls.
          </p>
          <div className="flex shrink-0 items-center gap-0.5 rounded-lg border border-border bg-surface p-0.5">
            {filters.map((f) => (
              <button
                key={f.key}
                type="button"
                onClick={() => setFilter(f.key)}
                className={
                  filter === f.key
                    ? 'rounded-md bg-primary px-2 py-1 text-[11px] font-medium text-primary-foreground'
                    : 'rounded-md px-2 py-1 text-[11px] font-medium text-muted-foreground transition-colors hover:text-foreground'
                }
              >
                {f.label}
              </button>
            ))}
          </div>
        </div>
        {journal.loading && (
          <div role="status" className="text-xs text-muted-foreground">loading environment tasks…</div>
        )}
        {journal.loadError && (
          <div role="alert" className="flex items-center justify-between gap-2 text-xs text-destructive">
            <span>{journal.loadError}</span>
            <Button variant="outline" size="sm" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('tasks', scope, journal.failedCursor ?? undefined).catch(() => undefined)}>Retry</Button>
          </div>
        )}
        {!journal.loading && visible.length === 0 ? (
          <div className="text-xs text-muted-foreground">no tasks match this filter</div>
        ) : (
          <div className="flex flex-col gap-1.5">
            {visible.map((t) => (
              <button
                key={t.id}
                type="button"
                onClick={() => setOpen(t)}
                className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-left transition-colors hover:border-ring/60 hover:bg-surface/60"
              >
                <div className="flex min-w-0 items-center gap-2 text-sm">
                  <StatusDot status={t.status} />
                  <span className="truncate font-medium">{t.title}</span>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  <span className="font-mono text-[10px] text-muted-foreground">
                    {t.status}
                  </span>
                  <ChevronRight className="size-3.5 text-muted-foreground/50" />
                </div>
              </button>
            ))}
          </div>
        )}
        {journal.nextCursor && !journal.loadError && (
          <Button variant="outline" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('tasks', scope, journal.nextCursor!).catch(() => undefined)}>
            {journal.loadingMore ? 'Loading…' : 'Load more'}
          </Button>
        )}
      </CardContent>
      {open && (
        <AuthoritativeTaskDrawer
          entry={open}
          scope={scope}
          surface="tasks"
          onOpenChange={(value) => !value && setOpen(null)}
        />
      )}
    </Card>
  )
}

function ReleasesCard({ env }: { env: Environment }) {
  const params = useRequiredParams('tenant', 'project', 'env')
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <History className="size-4 text-muted-foreground" /> Release ledger
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col">
        <p className="mb-2 text-xs text-muted-foreground">
          The per-service release ledger — state, not events: which tag is active, which are superseded. This is what
          rollback reads to pre-select the previous tag (a successful redeploy of the current tag never advances it).
          Live execution lives on the Tasks tab.
        </p>
        <div className="overflow-x-auto rounded-lg border border-border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                <th className="px-3 py-2">Service</th>
                <th className="px-3 py-2">Tag</th>
                <th className="px-3 py-2">Digest</th>
                <th className="px-3 py-2">Strategy</th>
                <th className="px-3 py-2">When</th>
                <th className="px-3 py-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {env.deploys.map((d, i) => (
                <tr key={d.id} className="border-b border-border last:border-0">
                  <td className="px-3 py-2 font-mono text-xs"><Link className="text-primary hover:underline" to={`/t/${params.tenant}/${params.project}/${params.env}/releases/${d.id}`}>{d.service}</Link></td>
                  <td className="px-3 py-2 font-mono text-xs">{d.tag}</td>
                  <td className="max-w-[220px] truncate px-3 py-2 font-mono text-xs text-muted-foreground">{d.digest}</td>
                  <td className="px-3 py-2">
                    <StrategyPill strategy={d.strategy} />
                  </td>
                  <td className="px-3 py-2 text-xs text-muted-foreground">{d.when}</td>
                  <td className="px-3 py-2">
                    {d.status === 'active' ? (
                      <span className="inline-flex items-center gap-1.5 rounded-full bg-success/10 px-2 py-0.5 text-xs text-success">
                        <span className="size-1.5 rounded-full bg-current" /> active
                      </span>
                    ) : (
                      <span className="inline-flex items-center gap-1.5 rounded-full bg-warning/10 px-2 py-0.5 text-xs text-warning">
                        <span className="size-1.5 rounded-full bg-current" /> superseded
                      </span>
                    )}
                  </td>
                </tr>
              ))}
              {env.deploys.length === 0 && (
                <tr>
                  <td colSpan={6} className="px-3 py-3 text-center text-xs text-muted-foreground">
                    no deploys yet
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
}

export function StrategyPill({ strategy }: { strategy: Service['strategy'] }) {
  if (strategy === 'blue-green')
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-2 py-0.5 text-xs text-primary">
        <span className="size-1.5 rounded-full bg-current" /> blue-green
      </span>
    )
  if (strategy === 'rolling')
    return (
      <span className="inline-flex items-center gap-1.5 rounded-full bg-warning/10 px-2 py-0.5 text-xs text-warning">
        <span className="size-1.5 rounded-full bg-current" /> rolling
      </span>
    )
  return (
    <span className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2 py-0.5 text-xs text-muted-foreground">
      <span className="size-1.5 rounded-full bg-current" /> recreate
    </span>
  )
}

// ---- Backups ----

function BackupsCard({ env }: { env: Environment }) {
  const store = useStore()
  const policyState = store.getBackupPolicyState(env.id)
  const backup = policyState.policy
  const points = policyState.recoveryPoints
  const [policyOpen, setPolicyOpen] = useState(false)
  const [runTaskId, setRunTaskId] = useState<string | null>(null)
  const [runError, setRunError] = useState<string | null>(null)
  useEffect(() => {
    void store.loadBackupPolicy(env.id).catch(() => undefined)
  }, [env.id, store.loadBackupPolicy])
  useEffect(() => {
    void store.loadRecoveryPoints(env.id).catch(() => undefined)
  }, [env.id, store.loadRecoveryPoints])
  const activeConnector = store.connectors.find(
    (connector) => connector.scopeRef === env.id && connector.id === backup.connectorId,
  )
  const configured = backupPolicyConfigured(backup)
  return (
    <>
      <Card>
        <CardHeader
          aria-busy={policyState.loading || policyState.saving}
          className="flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between"
        >
          <CardTitle className="flex items-start gap-2 leading-snug">
            <RefreshCw className="mt-0.5 size-4 shrink-0 text-muted-foreground" /> Backup policy · one policy, selected sources
          </CardTitle>
          <div className="flex flex-col gap-2 sm:flex-row sm:flex-wrap sm:items-center sm:justify-end">
            <span className="font-mono text-xs text-muted-foreground">Next: {backup.nextRunAt ?? "not scheduled"}</span>
            <Button
              variant="ghost"
              size="sm"
              disabled={policyState.loading || policyState.saving}
              onClick={() => void store.loadBackupPolicy(env.id).catch(() => undefined)}
            >
              <RefreshCw className={cn('size-3.5', policyState.loading && 'animate-spin')} /> Refresh
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={!configured || !backup.enabled || policyState.loading || policyState.saving || Boolean(policyState.loadError)}
              onClick={() => {
                setRunError(null)
                void store.runBackup(env.id)
                  .then((taskId) => setRunTaskId(taskId))
                  .catch((error) => setRunError(error instanceof Error ? error.message : 'Unable to run backup'))
              }}
            >
              <Terminal className="size-3.5" /> Backup now
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={!policyState.loaded || policyState.loading || policyState.saving || Boolean(policyState.loadError)}
              onClick={() => setPolicyOpen(true)}
            >
              Edit policy + sources
            </Button>
          </div>
          {(runTaskId || runError) && (
            <div className="flex flex-wrap items-center gap-2 text-xs">
              {runTaskId && <span className="text-muted-foreground">Task published: <code>{runTaskId}</code></span>}
              {runError && <span className="text-destructive">{runError}</span>}
            </div>
          )}
        </CardHeader>
        {policyState.loadError && (
          <p className="mx-4 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            {policyState.loadError}. The last loaded policy remains visible.
          </p>
        )}
        {!backup.enabled && (
          <p className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
            Backups are <span className="font-medium">off</span> for this environment — nothing is scheduled or backed up.
            {configured ? ' The retained policy can be enabled again.' : ' This policy is not configured yet.'}
          </p>
        )}
        <CardContent className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
          <PolicyCell label="Status" value={backup.enabled ? 'enabled' : 'off'} />
          <PolicyCell label="Strategy" value={deriveStrategy(store, env)} />
          <PolicyCell label="Frequency · UTC" value={backup.frequency ?? 'not configured'} />
          <PolicyCell label="Retention" value={backup.keep !== undefined ? `keep ${backup.keep} backups` : 'not configured'} />
          <PolicyCell label="Encryption" value={`${backup.encryption ?? 'not configured'}${backup.ageRecipient ? ` · age1…${backup.ageRecipient.slice(-8)}` : backup.encryption === 'age' ? ' · key generated on first enable' : ''}`} />
          <PolicyCell
            label="Target"
            value={
              activeConnector
                ? `s3://${activeConnector.bucket}/${activeConnector.prefix}`
                : backup.connectorId
                  ? `missing : ${backup.connectorId}`
                  : 'not selected'
            }
          />
          <PolicyCell label="Key era" value={backup.keyEra ? `era ${backup.keyEra}` : '—'} />
        </CardContent>
        <CardContent className="flex flex-col gap-1.5 border-t border-border pt-3">
          <span className="text-xs font-medium text-muted-foreground">Sources · one run backs up every selected source</span>
          <div className="flex flex-wrap gap-1.5">
            {backup.sources.map((src) => (
              <span key={src.id} className="inline-flex items-center gap-1.5 rounded-full bg-primary/10 px-2.5 py-1 font-mono text-xs text-primary">
                <RefreshCw className="size-3" />
                {backupSourceLabel(store, env, src)}
              </span>
            ))}
            {backup.sources.length === 0 && <span className="text-xs text-muted-foreground">no sources selected</span>}
          </div>
        </CardContent>
      </Card>

      <EnvironmentConnectorManager env={env} />

      {backup.sources.map((src) => {
        const strategy = backupSourceStrategy(store, env, src)
        const sourceLabel = backupSourceLabel(store, env, src)
        return (
          <Card key={src.id}>
            <CardHeader>
              <CardTitle className="flex items-center gap-2">
                <RefreshCw className="size-4 text-muted-foreground" /> Source · {adapterLabel(strategy)}
                <Badge variant="default">{sourceLabel}</Badge>
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-2">
              <p className="text-xs text-muted-foreground">
                {src.kind === 'volume'
                  ? `volume ${sourceLabel}`
                  : src.kind === 'config'
                    ? `environment config — env vars, files & secrets (values included, age-encrypted). Does NOT include backing environments or platform state.`
                    : `database ${sourceLabel}`}
                {' · '}backed up and restored by the same <span className="font-mono">{strategy}</span> adapter.
              </p>
              <div className="flex flex-col gap-1 border-b border-border pb-2">
                {adapterSteps(strategy).map((s, i) => (
                  <div key={i} className="flex items-baseline gap-2.5 text-xs">
                    <span className="size-1.5 shrink-0 translate-y-[-2px] rounded-full bg-success" />
                    <span className="w-32 shrink-0 font-mono text-primary">{s.op}</span>
                    <span className="break-all font-mono text-muted-foreground">{s.detail}</span>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        )
      })}

      <Card>
        <CardHeader
          aria-busy={points.loading || points.loadingMore}
          className="flex-col items-stretch gap-3 sm:flex-row sm:items-center sm:justify-between"
        >
          <CardTitle className="flex items-center gap-2">
            <RefreshCw className="size-4 text-muted-foreground" /> Recovery Points
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              size="sm"
              disabled={points.loading || points.loadingMore}
              onClick={() => void store.loadRecoveryPoints(env.id).catch(() => undefined)}
            >
              <RefreshCw className={cn('size-3.5', points.loading && 'animate-spin')} /> Refresh
            </Button>
          </div>
        </CardHeader>
        {points.loadError && (
          <div className="mx-4 flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            <span>{points.loadError}</span>
            <Button
              variant="outline"
              size="sm"
              disabled={points.loading || points.loadingMore}
              onClick={() => void store.loadRecoveryPoints(env.id, points.failedCursor ?? undefined).catch(() => undefined)}
            >
              Retry
            </Button>
          </div>
        )}
        <CardContent aria-busy={points.loading || points.loadingMore}>
          {points.loading && !points.loaded && <p className="text-xs text-muted-foreground">Loading verified Recovery Points...</p>}
          {!points.loading && !points.loadError && points.loaded && points.items.length === 0 && (
            <p className="rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
              No verified Recovery Points for this Environment.
            </p>
          )}
          {points.items.length > 0 && (
              <div className="overflow-x-auto rounded-lg border border-border">
                <table className="w-full min-w-[760px] text-left text-xs">
                  <thead className="bg-surface text-muted-foreground">
                    <tr>
                      <th className="px-3 py-2 font-medium">Point</th>
                      <th className="px-3 py-2 font-medium">Source</th>
                      <th className="px-3 py-2 font-medium">Target</th>
                      <th className="px-3 py-2 font-medium">Created</th>
                      <th className="px-3 py-2 font-medium">Size</th>
                      <th className="px-3 py-2 font-medium">Encryption</th>
                      <th className="px-3 py-2 font-medium">Status</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {points.items.map((point) => (
                      <tr key={point.id}>
                        <td className="px-3 py-2 font-mono text-primary">{point.id}</td>
                        <td className="px-3 py-2 font-mono">{point.sourceKind} · {point.sourceId}</td>
                        <td className="px-3 py-2 font-mono">{point.targetId}</td>
                        <td className="px-3 py-2 whitespace-nowrap">{point.createdAt}</td>
                        <td className="px-3 py-2 font-mono">{point.sizeBytes.toLocaleString()} B</td>
                        <td className="px-3 py-2">{point.encrypted ? `age · era ${point.keyEra}` : 'none'}</td>
                        <td className="px-3 py-2 text-success">{point.status}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
          )}
              {points.nextCursor && (
                <div className="mt-3 flex justify-center">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={points.loadingMore}
                    onClick={() => void store.loadRecoveryPoints(env.id, points.nextCursor ?? undefined).catch(() => undefined)}
                  >
                    {points.loadingMore ? 'Loading...' : 'Load more'}
                  </Button>
                </div>
              )}
        </CardContent>
      </Card>

      <BackupPolicyDialog env={env} open={policyOpen} onOpenChange={setPolicyOpen} />
    </>
  )
}

function PolicyCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface p-3">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="break-all font-mono text-sm">{value}</span>
    </div>
  )
}

function deriveStrategy(store: ReturnType<typeof useStore>, env: Environment) {
  const kinds = Array.from(new Set(store.getBackupPolicyState(env.id).policy.sources.map((source) => backupSourceStrategy(store, env, source))))
  if (kinds.includes('volume')) return 'per source'
  return kinds.join(' + ') || '—'
}

function backupSourceStrategy(store: ReturnType<typeof useStore>, env: Environment, source: BackupPolicySourceRecord): 'postgres:16' | 'config' | 'volume' | 'unsupported' {
  if (source.kind !== 'attach') return source.kind
  const attach = store.getBackupPolicyState(env.id).attaches.find((candidate) => candidate.id === source.targetId)
  const adapter = store.getBackingProject(attach?.backingProjectId ?? '')?.environments?.[0]?.services
    .find((service) => service.id === attach?.backingServiceId)?.adapter
  return adapter === 'postgres:16' ? adapter : 'unsupported'
}

function backupSourceLabel(store: ReturnType<typeof useStore>, env: Environment, source: BackupPolicySourceRecord) {
  if (source.kind === 'config') return `${env.name} config`
  if (source.kind === 'volume') {
    return store.getBackupPolicyState(env.id).volumes.find((volume) => volume.id === source.targetId)?.slug ?? source.targetId
  }
  const attach = store.getBackupPolicyState(env.id).attaches.find((candidate) => candidate.id === source.targetId)
  const backing = attach ? store.getBackingProject(attach.backingProjectId) : undefined
  return attach ? `${backing?.name ? `${backing.name} · ` : ''}${attach.name}` : source.targetId
}

function backupPolicyConfigured(policy: ReturnType<typeof useStore>['backupPolicies'][string]['policy']) {
  return Boolean(
    policy.frequency || policy.keep !== undefined || policy.encryption || policy.connectorId || policy.sources.length > 0 || policy.ageRecipient,
  )
}

function adapterLabel(kind: string) {
  if (kind === 'postgres:16') return 'PostgreSQL 16'
  if (kind === 'config') return 'Environment config'
  if (kind === 'unsupported') return 'Attach · unsupported for MVP backups'
  return 'Volume archive'
}

function adapterSteps(kind: string) {
  if (kind === 'unsupported') return []
  if (kind === 'config')
    return [
      { op: 'export', detail: 'serialize env entries (vars, files, secrets)' },
      { op: 'encrypt', detail: 'age-encrypt the bundle' },
      { op: 'upload', detail: 'r2://backups/…' },
      { op: 'verify', detail: 'HeadObject' },
      { op: 'prune', detail: 'past retention' },
    ]
  if (kind === 'postgres:16')
    return [
      { op: 'dump', detail: 'pg_dump --format=custom' },
      { op: 'encrypt', detail: 'age-encrypt with recipient' },
      { op: 'upload', detail: 'r2://backups/…' },
      { op: 'verify', detail: 'HeadObject' },
      { op: 'prune', detail: 'past retention' },
    ]
  return [
    { op: 'archive', detail: 'tar archive of volume dir' },
    { op: 'encrypt', detail: 'age-encrypt' },
    { op: 'upload', detail: 'r2://backups/…' },
    { op: 'verify', detail: 'HeadObject' },
    { op: 'prune', detail: 'past retention' },
  ]
}

// ---- Variables (env vars + env files) ----

function EnvVarsCard({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const entries = env.entries
  const [open, setOpen] = useState(false)
  const [kind, setKind] = useState<'env' | 'file'>('env')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [path, setPath] = useState('')
  const [uid, setUID] = useState('0')
  const [gid, setGID] = useState('0')
  const [sourceKind, setSourceKind] = useState<'literal' | 'secret_ref' | 'fact'>('literal')
  const [secretRef, setSecretRef] = useState('')
  const [factAttach, setFactAttach] = useState('')
  const [factGrantAttach, setFactGrantAttach] = useState('')
  const [factKey, setFactKey] = useState('')
  const [exposure, setExposure] = useState<string[]>(['all'])
  const [secret, setSecret] = useState(false)
  const [editing, setEditing] = useState<EnvironmentEntry | null>(null)
  const [removing, setRemoving] = useState<EnvironmentEntry | null>(null)
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  const [bulkOpen, setBulkOpen] = useState(false)
  const [bulkText, setBulkText] = useState('')
  const [bulkExposure, setBulkExposure] = useState<string[]>(['all'])
  const [bulkStorage, setBulkStorage] = useState<'plain' | 'secret'>('plain')
  const [bulkSaving, setBulkSaving] = useState(false)
  const [bulkError, setBulkError] = useState<string | null>(null)
  const serviceScoped = env.services.filter((service) =>
    entries.some((entry) => entry.exposure.includes(service.name)),
  )

  function startEdit(entry: EnvironmentEntry) {
    setKind(entry.type)
    setSecret(entry.secret)
    setKey(entry.key ?? '')
    setPath(entry.path ?? '')
    setUID(String(entry.uid ?? 0))
    setGID(String(entry.gid ?? 0))
    setSourceKind(entry.source.kind)
    setValue(entry.source.kind === 'literal' && !entry.secret ? entry.source.literal ?? '' : '')
    setSecretRef(entry.source.kind === 'secret_ref' ? entry.source.secretRef : '')
    setFactAttach(entry.source.kind === 'fact' ? entry.source.attachId : '')
    setFactGrantAttach(entry.source.kind === 'fact' ? entry.source.grantAttachId ?? '' : '')
    setFactKey(entry.source.kind === 'fact' ? entry.source.fact : '')
    setExposure([...entry.exposure])
    setEditing(entry)
    setSaveError(null)
    setOpen(true)
  }

  function closeDrawer() {
    setOpen(false)
    setEditing(null)
    setKind('env')
    setKey('')
    setValue('')
    setPath('')
    setUID('0')
    setGID('0')
    setSourceKind('literal')
    setSecretRef('')
    setFactAttach('')
    setFactGrantAttach('')
    setFactKey('')
    setExposure(['all'])
    setSecret(false)
    setSaveError(null)
  }

  function toggleServiceExposure(serviceName: string) {
    setExposure((current) => {
      const services = current.filter((candidate) => candidate !== 'all')
      if (services.includes(serviceName)) {
        return services.filter((candidate) => candidate !== serviceName)
      }
      return [...services, serviceName]
    })
  }

  function toggleBulkServiceExposure(serviceName: string) {
    setBulkExposure((current) => {
      const services = current.filter((candidate) => candidate !== 'all')
      if (services.includes(serviceName)) {
        return services.filter((candidate) => candidate !== serviceName)
      }
      return [...services, serviceName]
    })
  }

  async function saveBulkEntries() {
    if (bulkExposure.length === 0) {
      setBulkError('Select all services or at least one service.')
      return
    }
    let entries: { key: string; value: string }[]
    try {
      entries = parseBulkEnvironmentEntries(bulkText)
    } catch (error) {
      setBulkError(error instanceof Error ? error.message : 'Invalid bulk Entry input')
      return
    }
    setBulkSaving(true)
    setBulkError(null)
    try {
      await store.bulkUpsertEntries(env.id, {
        entries,
        exposure: bulkExposure,
        secret: bulkStorage === 'secret',
      })
      setBulkOpen(false)
      setBulkText('')
      setBulkExposure(['all'])
      setBulkStorage('plain')
    } catch (error) {
      setBulkError(error instanceof Error ? error.message : 'Unable to bulk edit Entries')
    } finally {
      setBulkSaving(false)
    }
  }

  async function saveEntry() {
    if (exposure.length === 0) {
      setSaveError('Select all services or at least one service.')
      return
    }
    let source
    switch (sourceKind) {
      case 'literal':
        source = { kind: 'literal', literal: value }
        break
      case 'secret_ref':
        if (!secretRef.trim()) {
          setSaveError('Secret reference is required.')
          return
        }
        source = { kind: 'secret_ref', secret_ref: secretRef.trim() }
        break
      case 'fact':
        if (!factAttach.trim() || !factKey.trim()) {
          setSaveError('Fact Attach id and fact key are required.')
          return
        }
        source = {
          kind: 'fact',
          attach_id: factAttach.trim(),
          grant_attach_id: factGrantAttach.trim() || undefined,
          fact: factKey.trim(),
        }
        break
    }
    setSaving(true)
    setSaveError(null)
    try {
      if (editing) {
        await store.updateEntry(env.id, editing.id, { source, exposure })
      } else {
        await store.addEntry(env.id, {
          type: kind,
          key: kind === 'env' ? key.trim() : undefined,
          path: kind === 'file' ? path.trim() : undefined,
          uid: kind === 'file' ? Number(uid) : undefined,
          gid: kind === 'file' ? Number(gid) : undefined,
          source,
          exposure,
          secret,
        })
      }
      closeDrawer()
    } catch (error) {
      setSaveError(error instanceof Error ? error.message : 'Unable to save Entry')
    } finally {
      setSaving(false)
    }
  }

  function renderEntry(entry: EnvironmentEntry) {
    const label = entry.type === 'env' ? entry.key ?? entry.id : entry.path ?? entry.id
    const literal = entry.source.kind === 'literal' ? entry.source.literal : undefined
    return (
      <EntryRow
        key={entry.id}
        label={label}
        file={entry.type === 'file'}
        path={entry.path}
        secret={entry.secret}
        value={entry.secret ? undefined : literal ?? entry.source.kind}
        loadValue={entry.secret ? () => store.revealEntry(entry.id) : undefined}
        onEdit={() => startEdit(entry)}
        onRemove={() => setRemoving(entry)}
      />
    )
  }

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-muted-foreground" /> Environment Entries
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={() => setBulkOpen(true)}>
              Bulk edit
            </Button>
            <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
              <Plus className="size-3.5" /> Add entry
            </Button>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="text-xs font-medium text-muted-foreground">All services</span>
            <span className="font-mono text-xs text-muted-foreground">inherited by every service</span>
          </div>
          {entries.filter((entry) => entry.exposure.includes('all')).map(renderEntry)}
          {entries.length === 0 && (
            <div className="text-xs text-muted-foreground">no Entries yet</div>
          )}
          {serviceScoped.map((service) => (
            <div key={service.id} className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
                <span className="text-xs font-medium text-muted-foreground">Service-scoped · {service.name}</span>
                <Badge variant="primary">{service.name}</Badge>
              </div>
              {entries.filter((entry) => entry.exposure.includes(service.name)).map(renderEntry)}
            </div>
          ))}
          <p className="mt-2 text-xs text-muted-foreground">
            Entries are one resource model for environment variables and files. Secret values remain encrypted and load
            only through the explicit reveal action.
          </p>
        </CardContent>
      </Card>

      <Drawer open={open} onOpenChange={(next) => (next ? undefined : closeDrawer())}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>{editing ? 'Edit Entry' : 'Add Entry'} · {env.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label>Type</Label>
              <div className="flex gap-2">
                {(['env', 'file'] as const).map((candidate) => (
                  <Button
                    key={candidate}
                    variant={kind === candidate ? 'default' : 'outline'}
                    size="sm"
                    disabled={!!editing}
                    onClick={() => setKind(candidate)}
                  >
                    {candidate === 'env' ? 'env variable' : 'file'}
                  </Button>
                ))}
              </div>
            </div>

            {kind === 'env' ? (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-key">Key</Label>
                <Input
                  id="entry-key"
                  value={key}
                  disabled={!!editing}
                  onChange={(event) => setKey(event.target.value)}
                  placeholder="APP_ENV"
                />
              </div>
            ) : (
              <>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-path">Destination path</Label>
                  <Input
                    id="entry-path"
                    value={path}
                    disabled={!!editing}
                    onChange={(event) => setPath(event.target.value)}
                    placeholder="config/app.ini"
                  />
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="entry-uid">UID</Label>
                    <Input
                      id="entry-uid"
                      type="number"
                      min={0}
                      max={4294967294}
                      value={uid}
                      disabled={!!editing}
                      onChange={(event) => setUID(event.target.value)}
                    />
                  </div>
                  <div className="flex flex-col gap-1.5">
                    <Label htmlFor="entry-gid">GID</Label>
                    <Input
                      id="entry-gid"
                      type="number"
                      min={0}
                      max={4294967294}
                      value={gid}
                      disabled={!!editing}
                      onChange={(event) => setGID(event.target.value)}
                    />
                  </div>
                </div>
              </>
            )}

            <div className="flex flex-col gap-1.5">
              <Label>Source</Label>
              <div className="flex flex-wrap gap-2">
                {(['literal', 'secret_ref', 'fact'] as const).map((candidate) => (
                  <Button
                    key={candidate}
                    variant={sourceKind === candidate ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setSourceKind(candidate)}
                  >
                    {candidate.replace('_', ' ')}
                  </Button>
                ))}
              </div>
            </div>
            {sourceKind === 'literal' && (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-value">Literal value</Label>
                {kind === 'file' ? (
                  <Textarea
                    id="entry-value"
                    value={value}
                    onChange={(event) => setValue(event.target.value)}
                    rows={6}
                    spellCheck={false}
                    className="font-mono text-xs"
                  />
                ) : (
                  <Input id="entry-value" value={value} onChange={(event) => setValue(event.target.value)} />
                )}
                {editing?.secret && (
                  <p className="text-xs text-muted-foreground">
                    The existing secret is never loaded into this form. Enter a replacement value.
                  </p>
                )}
              </div>
            )}
            {sourceKind === 'secret_ref' && (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-secret-ref">Reusable Secret reference</Label>
                <Input
                  id="entry-secret-ref"
                  value={secretRef}
                  onChange={(event) => setSecretRef(event.target.value)}
                  placeholder="DATABASE_PASSWORD"
                />
              </div>
            )}
            {sourceKind === 'fact' && (
              <div className="grid gap-3">
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-fact-attach">Attach id</Label>
                  <Input
                    id="entry-fact-attach"
                    value={factAttach}
                    onChange={(event) => setFactAttach(event.target.value)}
                    placeholder="att_..."
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-fact-grant">Grant Attach id · optional</Label>
                  <Input
                    id="entry-fact-grant"
                    value={factGrantAttach}
                    onChange={(event) => setFactGrantAttach(event.target.value)}
                    placeholder="att_..."
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-fact-key">Fact key</Label>
                  <Input
                    id="entry-fact-key"
                    value={factKey}
                    onChange={(event) => setFactKey(event.target.value)}
                    placeholder="pg16_URL"
                  />
                </div>
              </div>
            )}

            <div className="flex flex-col gap-1.5">
              <Label>Exposure</Label>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={exposure.includes('all') ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setExposure(['all'])}
                >
                  All services
                </Button>
                {env.services.map((service) => (
                  <Button
                    key={service.id}
                    variant={exposure.includes(service.name) ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => toggleServiceExposure(service.name)}
                  >
                    {service.name}
                  </Button>
                ))}
              </div>
            </div>

            <label className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5">
              <div className="flex flex-col">
                <span className="text-sm font-medium">Secret storage class</span>
                <span className="text-xs text-muted-foreground">
                  Secret Entries are encrypted and materialize at mode 0600.
                </span>
              </div>
              <Switch checked={secret} disabled={!!editing} onCheckedChange={setSecret} />
            </label>
            {saveError && <p className="text-sm text-destructive">{saveError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={closeDrawer} disabled={saving}>
              Cancel
            </Button>
            <Button
              disabled={
                saving ||
                (kind === 'env' ? !key.trim() : !path.trim()) ||
                exposure.length === 0 ||
                (sourceKind === 'secret_ref' && !secretRef.trim()) ||
                (sourceKind === 'fact' && (!factAttach.trim() || !factKey.trim()))
              }
              onClick={() => void saveEntry()}
            >
              {saving ? 'Saving…' : editing ? 'Save Entry' : 'Add Entry'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <Drawer open={bulkOpen} onOpenChange={(next) => {
        setBulkOpen(next)
        if (!next) setBulkError(null)
      }}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Bulk edit environment variables · {env.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="entry-bulk-values">Variables</Label>
              <Textarea
                id="entry-bulk-values"
                value={bulkText}
                onChange={(event) => setBulkText(event.target.value)}
                rows={12}
                spellCheck={false}
                className="font-mono text-xs"
                placeholder={'APP_ENV=production\nDATABASE_URL=postgres://app:pass@db/app\nEMPTY_VALUE='}
              />
              <p className="text-xs text-muted-foreground">
                One KEY=value per line. Values may contain =. Blank lines and lines beginning with # are ignored.
                Matching keys are updated and omitted keys remain unchanged.
              </p>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label>Storage</Label>
              <div className="flex gap-2">
                {(['plain', 'secret'] as const).map((candidate) => (
                  <Button
                    key={candidate}
                    variant={bulkStorage === candidate ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => setBulkStorage(candidate)}
                  >
                    {candidate === 'plain' ? 'Plain values' : 'Secret values'}
                  </Button>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">
                Existing keys must already use the selected storage class.
              </p>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label>Exposure</Label>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={bulkExposure.includes('all') ? 'default' : 'outline'}
                  size="sm"
                  onClick={() => setBulkExposure(['all'])}
                >
                  All services
                </Button>
                {env.services.map((service) => (
                  <Button
                    key={service.id}
                    variant={bulkExposure.includes(service.name) ? 'default' : 'outline'}
                    size="sm"
                    onClick={() => toggleBulkServiceExposure(service.name)}
                  >
                    {service.name}
                  </Button>
                ))}
              </div>
            </div>
            {bulkError && <p className="text-sm text-destructive">{bulkError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setBulkOpen(false)} disabled={bulkSaving}>
              Cancel
            </Button>
            <Button
              disabled={bulkSaving || !bulkText.trim() || bulkExposure.length === 0}
              onClick={() => void saveBulkEntries()}
            >
              {bulkSaving ? 'Applying…' : 'Apply bulk edit'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <TaskRunnerDialog
		open={!!removing}
		onOpenChange={(next) => !next && setRemoving(null)}
		title={`Remove Entry · ${removing ? (removing.type === 'env' ? removing.key : removing.path) : ''}`}
		description="Removes the pinned materialization, rewrites generated environment files when needed, and deletes the Entry only after the Agent reports success."
		type="destroy"
		target={removing?.id ?? env.id}
		workspace={params.tenant}
		destructive
		confirmText={removing ? (removing.type === 'env' ? removing.key ?? removing.id : removing.path ?? removing.id) : ''}
		startLabel="Remove Entry"
		steps={[
		  { label: 'Validate the pinned Entry generation', state: 'pending' },
		  { label: 'Remove or rewrite materialized files', state: 'pending' },
		  { label: 'Finalize the Entry record', state: 'pending' },
		]}
		onDispatch={async () => {
		  if (!removing) throw new Error('No Entry selected for removal')
		  return store.removeEntry(env.id, removing.id)
		}}
	  />
    </>
  )
}

function parseBulkEnvironmentEntries(input: string): { key: string; value: string }[] {
  const entries: { key: string; value: string }[] = []
  const seen = new Set<string>()
  input.replaceAll('\r\n', '\n').split('\n').forEach((rawLine, index) => {
    const line = rawLine.endsWith('\r') ? rawLine.slice(0, -1) : rawLine
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) return
    const separator = line.indexOf('=')
    if (separator < 0) throw new Error(`Line ${index + 1} must use KEY=value.`)
    const key = line.slice(0, separator).trim()
    if (!key) throw new Error(`Line ${index + 1} has an empty key.`)
    if (seen.has(key)) throw new Error(`Line ${index + 1} duplicates ${key}.`)
    seen.add(key)
    entries.push({ key, value: line.slice(separator + 1) })
  })
  if (entries.length === 0) throw new Error('Enter at least one KEY=value line.')
  if (entries.length > 200) throw new Error('Bulk edit accepts at most 200 variables.')
  return entries
}

function EntryRow({
  label,
  value,
  path,
  file,
  secret,
  loadValue,
  onEdit,
  onRemove,
}: {
  label: string
  value?: string
  path?: string
  file?: boolean
  secret?: boolean
  loadValue?: () => Promise<string>
  onEdit?: () => void
  onRemove?: () => void
}) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
      <span className="flex min-w-0 items-center gap-2">
        <span className="truncate font-mono text-sm">{label}</span>
        {file && <Badge variant="muted">file</Badge>}
        {secret && <Badge variant="warning">secret</Badge>}
      </span>
      <span className="flex items-center gap-2">
        {path && (
          <span className="truncate font-mono text-xs text-muted-foreground">{path}</span>
        )}
        {secret && loadValue ? (
          <RevealValue loadValue={loadValue} label={label} />
        ) : !path ? (
          <span className="truncate font-mono text-xs text-muted-foreground">{value}</span>
        ) : null}
        {onEdit && (
          <Button variant="ghost" size="icon-sm" className="text-muted-foreground hover:text-primary" onClick={onEdit} title={`Edit ${label}`}>
            <Pencil className="size-3.5" />
          </Button>
        )}
        {onRemove && (
		  <Button variant="ghost" size="icon-sm" className="text-muted-foreground hover:text-destructive" onClick={onRemove} title={`Remove ${label}`}>
			<Trash2 className="size-3.5" />
		  </Button>
		)}
      </span>
    </div>
  )
}

// ---- Shared-infra facts ----

// ---- Environment settings: identity, encryption key, rename, delete ----

function SettingsCard({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant', 'project')
  const navigate = useNavigate()
  const [renameOpen, setRenameOpen] = useState(false)
  const [networkPoolOpen, setNetworkPoolOpen] = useState(false)
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [newName, setNewName] = useState(env.name)
  const [networkPool, setNetworkPool] = useState(env.networkPool)
  const [networkPoolSaving, setNetworkPoolSaving] = useState(false)
  const [networkPoolError, setNetworkPoolError] = useState<string | null>(null)
  const [renameSaving, setRenameSaving] = useState(false)
  const [renameError, setRenameError] = useState<string | null>(null)
  const [deleteSaving, setDeleteSaving] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [confirmTyped, setConfirmTyped] = useState('')
  const [keyAction, setKeyAction] = useState<'rotate' | 'export' | null>(null)
  const [keyError, setKeyError] = useState<string | null>(null)
  const keyExportController = useRef<AbortController | null>(null)
  const deletionFailure = store.getEnvironmentDeletionFailure(env.id)

  useEffect(() => {
    setNewName(env.name)
    setNetworkPool(env.networkPool)
  }, [env.name, env.networkPool])

  useEffect(() => () => {
    keyExportController.current?.abort()
    keyExportController.current = null
  }, [])

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Layers className="size-4 text-muted-foreground" /> Environment
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" onClick={() => {
              setNetworkPool(env.networkPool)
              setNetworkPoolError(null)
              setNetworkPoolOpen(true)
            }}>
              <Pencil className="size-3.5" /> Edit pool
            </Button>
            <Button variant="outline" size="sm" onClick={() => setRenameOpen(true)}>
              <FileCode2 className="size-3.5" /> Rename
            </Button>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">id (static — every reference keys off this)</span>
            <span className="flex items-center gap-2">
              <span className="truncate font-mono text-xs text-foreground">{env.id}</span>
              <CopyButton value={env.id} />
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">name (label only)</span>
            <span className="truncate font-mono text-xs text-foreground">{env.name}</span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">network pool (globally reserved IPv4 CIDR)</span>
            <span className="truncate font-mono text-xs text-foreground">{env.networkPool}</span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">allocation (Zone CIDR addresses)</span>
            <span className="truncate font-mono text-xs text-foreground">
              {env.networkCapacity.allocatedAddresses.toLocaleString()} /
              {' '}{env.networkCapacity.totalAddresses.toLocaleString()}
              {' · '}{env.networkCapacity.availableAddresses.toLocaleString()} available
              {' · '}{env.networkCapacity.zoneCount} Zones
            </span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">volume folder (id-derived, never renamed)</span>
            <span className="truncate font-mono text-xs text-muted-foreground">{env.volumeDir}</span>
          </div>
          <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
            <span className="font-mono text-[11px] text-muted-foreground">deterministic env file</span>
            <span className="truncate font-mono text-xs text-muted-foreground">secrets/.env.{env.id}</span>
          </div>
        </CardContent>
      </Card>

      <Drawer open={networkPoolOpen} onOpenChange={(open) => {
        if (networkPoolSaving) return
        setNetworkPoolOpen(open)
        if (!open) setNetworkPoolError(null)
      }}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Edit network pool · {env.name}</DialogTitle>
            <DialogDescription>
              The replacement must be a canonical IPv4 CIDR, contain every existing Zone subnet, and overlap no other
              Environment allocation.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="environment-network-pool">Network pool</Label>
            <Input
              id="environment-network-pool"
              value={networkPool}
              onChange={(event) => setNetworkPool(event.target.value)}
              placeholder="10.40.0.0/16"
              autoFocus
            />
            {networkPoolError && <p className="text-xs text-destructive">{networkPoolError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" disabled={networkPoolSaving} onClick={() => setNetworkPoolOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={networkPoolSaving || !networkPool.trim() || networkPool.trim() === env.networkPool}
              onClick={async () => {
                setNetworkPoolSaving(true)
                setNetworkPoolError(null)
                try {
                  await store.editEnvironment(env.id, networkPool.trim())
                  setNetworkPoolOpen(false)
                } catch (error) {
                  setNetworkPoolError(error instanceof Error ? error.message : 'Unable to edit Environment network pool')
                } finally {
                  setNetworkPoolSaving(false)
                }
              }}
            >
              {networkPoolSaving ? 'Saving…' : 'Save pool'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-muted-foreground" /> Encryption key · age
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            disabled={!env.age || keyAction !== null}
            onClick={async () => {
              if (!env.age || !window.confirm('Rotate this Environment age key? Previous recovery points require the previously exported identity.')) return
              setKeyAction('rotate')
              setKeyError(null)
              try {
                const taskID = await store.rotateBackupKey(env.id)
                setKeyError(`Rotation task ${taskID} dispatched.`)
              } catch (error) {
                setKeyError(error instanceof Error ? error.message : 'Unable to rotate the backup age key')
              } finally {
                setKeyAction(null)
              }
            }}
          >
            <RotateCw className={cn('size-3.5', keyAction === 'rotate' && 'animate-spin')} /> {keyAction === 'rotate' ? 'Rotating…' : 'Rotate key'}
          </Button>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          {env.age ? (
            <>
              <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
                <span className="font-mono text-[11px] text-muted-foreground">recipient (public — encrypts backups)</span>
                <span className="flex items-center gap-2">
                  <span className="truncate font-mono text-xs text-foreground">{env.age.recipient}</span>
                  <CopyButton value={env.age.recipient} />
                </span>
              </div>
              <div className="flex flex-col gap-2 rounded-lg border border-border bg-surface px-3 py-2 sm:flex-row sm:items-center sm:justify-between">
                <span className="font-mono text-[11px] text-muted-foreground">identity export (private — decrypts, wrapped at rest)</span>
                <span className="flex min-w-0 items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={keyAction !== null}
                    onClick={async () => {
                      keyExportController.current?.abort()
                      const controller = new AbortController()
                      keyExportController.current = controller
                      setKeyAction('export')
                      setKeyError(null)
                      try {
                        await store.exportBackupKey(env.id, controller.signal)
                      } catch (error) {
                        if (!controller.signal.aborted) {
                          setKeyError(error instanceof Error ? error.message : 'Unable to export the backup age identity')
                        }
                      } finally {
                        if (keyExportController.current === controller) {
                          keyExportController.current = null
                          setKeyAction(null)
                        }
                      }
                    }}
                  >
                    {keyAction === 'export' ? 'Exporting…' : 'Export identity'}
                  </Button>
                </span>
              </div>
              <p className="text-xs text-muted-foreground">
                Backups encrypt with the public recipient (safe in desired state). The Controller keeps the private identity
                wrapped outside the ordinary Console store; export is a repeatable transient no-store response to keep off-host
                for disaster recovery. Rotating affects new backups only; previous recovery points need the previously exported identity.
              </p>
              {env.age.lastRotatedAt && (
                <p className="text-xs text-muted-foreground">
                  generated {env.age.generatedAt} · last rotated {env.age.lastRotatedAt}
                </p>
              )}
              {keyError && <p className="text-xs text-muted-foreground">{keyError}</p>}
            </>
          ) : (
            <p className="text-xs text-muted-foreground">
              Not generated yet — the keypair is created lazily when backups are first enabled on the Backups tab. A
              staging/dev environment that never backs up gets no key at all.
            </p>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-destructive">
            <Trash2 className="size-4" /> Delete environment
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          <p className="text-xs text-muted-foreground">
            Removes this environment and everything scoped to it: its env entries, wrapped backup key, backups, and
            recovery points. The project and other environments are untouched.
          </p>
          <div>
            <Button variant="destructive" size="sm" onClick={() => setDeleteOpen(true)}>
              <Trash2 className="size-3.5" /> Delete {env.name}
            </Button>
          </div>

        </CardContent>
      </Card>

      <Drawer open={renameOpen} onOpenChange={(open) => {
        if (renameSaving) return
        setRenameOpen(open)
        if (!open) setRenameError(null)
      }}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Rename environment · {env.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="ren-name">Name (label only)</Label>
              <Input id="ren-name" value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="production" autoFocus />
              <p className="text-xs text-muted-foreground">
                The id <span className="font-mono">{env.id}</span> stays fixed — every reference (secrets, age identity,
                backups, volume folder) keys off it, so renaming never breaks anything. The URL, deterministic env file,
                and activity labels follow the new name.
              </p>
              {renameError && <p className="text-xs text-destructive" role="alert">{renameError}</p>}
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" disabled={renameSaving} onClick={() => setRenameOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={renameSaving || !newName.trim() || newName.trim() === env.name}
              onClick={async () => {
                setRenameSaving(true)
                setRenameError(null)
                try {
                  const renamed = await store.renameEnvironment(env.id, newName.trim())
                  setRenameOpen(false)
                  navigate(`/t/${params.tenant}/${params.project}/${encodeURIComponent(renamed.name)}`)
                } catch (error) {
                  setRenameError(error instanceof Error ? error.message : 'Unable to rename Environment')
                } finally {
                  setRenameSaving(false)
                }
              }}
            >
              {renameSaving ? 'Renaming…' : 'Rename'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete {env.name}?</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <p className="text-xs text-muted-foreground">
              This permanently removes the environment <span className="font-mono">{env.id}</span> and its env-scoped
              secrets, backups, and recovery points. Cannot be undone.
            </p>
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <Label htmlFor="del-confirm">Type the required value to confirm</Label>
                <CopyButton value={env.name} label="copy required value" />
              </div>
              <code className="select-all rounded-md border border-border bg-surface px-2.5 py-1.5 font-mono text-xs text-foreground">
                {env.name}
              </code>
              <Input id="del-confirm" placeholder={env.name} onChange={(e) => setConfirmTyped(e.target.value)} autoFocus />
            </div>
            {(deleteError ?? deletionFailure?.message) && <p className="text-xs text-destructive" role="alert">{deleteError ?? deletionFailure?.message}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" disabled={deleteSaving} onClick={() => setDeleteOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              disabled={deleteSaving || confirmTyped !== env.name}
              onClick={async () => {
                setDeleteSaving(true)
                setDeleteError(null)
                try {
                  await store.deleteEnvironment(env.id)
                  navigate(`/t/${params.tenant}/${params.project}`)
                } catch (error) {
                  setDeleteError(error instanceof Error ? error.message : 'Unable to delete Environment')
                  setDeleteSaving(false)
                }
              }}
            >
              <Trash2 className="size-4" /> {deleteSaving ? 'Deleting…' : 'Delete environment'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}

function FactsCard({ env }: { env: Environment }) {
  const store = useStore()
  const factGroups = env.attaches.flatMap((a) => {
    const g = store.getBackingProject(a.projectId)
    if (!g) return []
    const sets = a.factSets.map((set) => ({
      label: set.grantAttachId ? `${g.name} · grant ${set.grantAttachId}` : `${g.name} · ${a.name}`,
      rows: set.facts.map((fact) => ({
        attachId: a.id,
        grantAttachId: set.grantAttachId,
        k: fact.key,
        secret: fact.secret,
      })),
    }))
	return [{ id: `facts-${a.id}`, title: `for ${a.service}`, sets }]
  })

  if (factGroups.length === 0) return null

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" /> Shared infra · facts
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        <p className="text-xs text-muted-foreground">
          Attaching exposes <span className="font-medium text-foreground">facts, not injected vars</span>. Values are
          generated by the Controller and decrypted only through the explicit reveal action. Create env entries from these
          facts — the names are yours. A grant adds a fact set over the granted database.
        </p>
        {factGroups.map((grp) => (
          <div key={grp.id} className="flex flex-col gap-2">
            <span className="font-mono text-[11px] text-muted-foreground">{grp.title}</span>
            {grp.sets.map((set) => (
              <div key={set.label} className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
                <div className="flex items-center justify-between">
                  <span className="font-mono text-xs font-medium">{set.label}</span>
                </div>
                <div className="flex flex-col gap-1 border-t border-border pt-1.5">
                  {set.rows.map((r) => (
                    <FactRow key={r.k} {...r} />
                  ))}
                </div>
              </div>
            ))}
          </div>
        ))}
      </CardContent>
    </Card>
  )
}

function FactRow({
  attachId,
  grantAttachId,
  k,
  secret,
}: {
  attachId: string
  grantAttachId?: string
  k: string
  secret: boolean
}) {
  const store = useStore()
  const [value, setValue] = useState<string>()
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string>()
  return (
    <div className="flex items-center justify-between gap-2">
      <span className="font-mono text-[11px] text-muted-foreground">{k}</span>
      <div className="flex items-center gap-2">
        <span className="font-mono text-[11px] text-foreground">
          {error ?? value ?? (secret ? '••••••••' : 'not loaded')}
        </span>
        {value ? <CopyButton value={value} /> : null}
        <Button
          variant="ghost"
          size="sm"
          disabled={loading}
          onClick={() => {
            setLoading(true)
            setError(undefined)
            void store
              .revealAttachFact(attachId, k, grantAttachId)
              .then(setValue)
              .catch((cause: unknown) => {
                setError(cause instanceof Error ? cause.message : 'Unable to reveal fact')
              })
              .finally(() => setLoading(false))
          }}
        >
          {loading ? 'Loading…' : value ? 'Refresh' : 'Reveal'}
        </Button>
      </div>
    </div>
  )
}

// ---- Scripts ----

function ScriptsCard({ env }: { env: Environment }) {
  const store = useStore()
  const params = useRequiredParams('tenant')
  const [addOpen, setAddOpen] = useState(false)
  const [editing, setEditing] = useState<Environment['scripts'][number] | null>(null)
  const [removing, setRemoving] = useState<Environment['scripts'][number] | null>(null)
  const [sName, setSName] = useState('')
  const [sService, setSService] = useState(env.services[0]?.name ?? '')
  const [sBody, setSBody] = useState('')
  const [sWhen, setSWhen] = useState('manual')
  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Terminal className="size-4 text-muted-foreground" /> Scripts
          </CardTitle>
          <Button variant="outline" size="sm" onClick={() => {
            setEditing(null)
            setSName('')
            setSService(env.services[0]?.name ?? '')
            setSBody('')
            setSWhen('manual')
            setAddOpen(true)
          }}>
            <Plus className="size-3.5" /> Script
          </Button>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          {env.scripts.map((s) => (
            <div key={s.id} className="flex items-center justify-between gap-3 rounded-lg border border-border bg-surface px-3 py-2">
              <div className="flex min-w-0 flex-col">
                <div className="flex items-center gap-2">
				  <span className="font-mono text-sm">{s.slug}</span>
                  <Badge variant={s.when === 'manual' ? 'default' : 'primary'}>{s.when}</Badge>
                </div>
                <span className="truncate font-mono text-xs text-muted-foreground">
                  {s.body.split('\n')[0]}
                  {s.body.split('\n').length > 1 ? ` · +${s.body.split('\n').length - 1} lines` : ''}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <span className="font-mono text-xs text-muted-foreground">→ {s.service}</span>
				<Button variant="outline" size="sm" onClick={() => void store.runScript(s.id)}>
                  <Terminal className="size-3.5" /> Run
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  title="Edit script"
                  onClick={() => {
					setSName(s.slug)
                    setSService(s.service)
                    setSBody(s.body)
                    setSWhen(s.when)
                    setEditing(s)
                  }}
                >
                  <Pencil className="size-3.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  className="text-muted-foreground hover:text-destructive"
                  title="Remove script"
                  onClick={() => setRemoving(s)}
                >
                  <Trash2 className="size-3.5" />
                </Button>
              </div>
            </div>
          ))}
          {env.scripts.length === 0 && (
            <div className="text-xs text-muted-foreground">no scripts — hooks run automatically on deploy/rollback</div>
          )}
        </CardContent>
      </Card>

      <Drawer open={addOpen || !!editing} onOpenChange={(next) => {
        setAddOpen(next)
        if (!next) {
          setEditing(null)
          setSubmitError(null)
        }
      }}>
        <DrawerContent>
          <DialogHeader>
			<DialogTitle>{editing ? `Edit script · ${editing.slug}` : `Add script · ${env.name}`}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-name">Name</Label>
			  <Input id="s-name" value={sName} onChange={(e) => setSName(e.target.value)} placeholder="migrate" />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-service">Service</Label>
              {editing ? (
                <Input id="s-service" value={sService} disabled />
              ) : (
                <Select
                  id="s-service"
                  value={sService}
                  onValueChange={setSService}
                  options={env.services.map((s) => ({ value: s.name, label: s.name }))}
                />
              )}
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-body">Script (one line or many)</Label>
              <Textarea id="s-body" value={sBody} onChange={(e) => setSBody(e.target.value)} rows={6} spellCheck={false} className="font-mono text-xs" placeholder={'php artisan migrate --force\nphp artisan db:seed --force'} />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="s-when">When</Label>
              <Select
                id="s-when"
                value={sWhen}
                onValueChange={setSWhen}
                options={['manual', 'pre-deploy', 'post-deploy', 'pre-rollback', 'post-rollback', 'on-failure'].map((w) => ({ value: w, label: w }))}
              />
            </div>
            <p className="text-xs text-muted-foreground">
              A full script body that runs against a service. Hooks run automatically on deploy/rollback.
            </p>
            {submitError && <p className="text-xs text-destructive">{submitError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => {
              setAddOpen(false)
              setEditing(null)
            }}>
              Cancel
            </Button>
            <Button
              disabled={submitting || !sName.trim() || !sService || !sBody.trim()}
              onClick={async () => {
                setSubmitting(true)
                setSubmitError(null)
                try {
                  if (editing) {
                    await store.updateScript(env.id, editing.id, {
					  slug: sName.trim(),
                      when: sWhen as Environment['scripts'][number]['when'],
                      body: sBody,
                    })
                  } else {
                    await store.addScript(env.id, {
					  slug: sName.trim(),
                      service: sService,
                      when: sWhen as Environment['scripts'][number]['when'],
                      body: sBody,
                    })
                  }
                  setAddOpen(false)
                  setEditing(null)
                  setSName('')
                  setSBody('')
                } catch (error) {
                  setSubmitError(error instanceof Error ? error.message : 'Script mutation failed')
                } finally {
                  setSubmitting(false)
                }
              }}
            >
              {submitting ? 'Saving…' : editing ? 'Save script' : 'Add script'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
		title={`Remove script · ${removing?.slug ?? ''}`}
        description="Removes this script and its deploy or rollback hook from the environment."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
		confirmText={removing?.slug ?? ''}
        startLabel="Remove script"
        steps={[
          { label: 'Validate the script record', state: 'pending' },
          { label: 'Remove the automatic hook registration', state: 'pending' },
          { label: 'Remove the script', state: 'pending' },
        ]}
        onDispatch={async () => {
          if (!removing) throw new Error('No Script selected for removal')
          return store.removeScript(env.id, removing.id)
        }}
      />
    </>
  )
}

// ---- Forms ----

function ZoneFormDialog({ env, open, onOpenChange }: { env: Environment; open: boolean; onOpenChange: (v: boolean) => void }) {
  const store = useStore()
  const [name, setName] = useState('')
  const [subnet, setSubnet] = useState('')
  const [internal, setInternal] = useState(false)
	const [submitting, setSubmitting] = useState(false)
	const [submitError, setSubmitError] = useState<string | null>(null)
	const normalizedName = name.trim()
	const nameError = normalizedName !== '' && !/^[A-Za-z0-9._-]+$/.test(normalizedName)
		? 'Use only letters, numbers, dot, underscore, and hyphen.'
		: null
	const subnetError = subnet.trim() ? null : 'Subnet is required.'
	const submit = async () => {
		setSubmitting(true)
		setSubmitError(null)
		try {
			await store.addZone(env.id, {
				name: normalizedName,
				subnet: subnet.trim(),
				internal,
			})
			onOpenChange(false)
			setName('')
			setSubnet('')
			setInternal(false)
		} catch (error) {
			setSubmitError(error instanceof Error ? error.message : 'Unable to create Zone')
		} finally {
			setSubmitting(false)
		}
	}
  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>Add zone · {env.name}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="z-name">Zone name</Label>
            <Input id="z-name" value={name} onChange={(e) => setName(e.target.value)} placeholder="backend" autoFocus />
			<p className={nameError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}>
			  {nameError ?? 'Must be a valid Docker Compose network key.'}
			</p>
          </div>
		  <div className="flex flex-col gap-1.5">
			<Label htmlFor="z-subnet">Subnet</Label>
			<Input id="z-subnet" value={subnet} onChange={(e) => setSubnet(e.target.value)} placeholder="10.200.20.0/24" />
			<p className={subnetError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}>
			  {subnetError ?? `Must be inside ${env.networkPool} and disjoint from every existing Zone.`}
			</p>
          </div>
          <label className="flex items-center gap-2 text-sm">
            <Switch checked={internal} onCheckedChange={setInternal} />
            <span className="text-muted-foreground">internal network — no egress, no published ports</span>
          </label>
		  {submitError && <p className="text-sm text-destructive" role="alert">{submitError}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
			disabled={!normalizedName || nameError !== null || subnetError !== null || submitting}
            onClick={() => void submit()}
          >
            {submitting ? 'Creating…' : 'Create zone'}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  )
}

function ServiceFormDialog({ env }: { env: Environment }) {
  const params = useRequiredParams('tenant')
  const [open, setOpen] = useState(false)
  return (
    <>
      <Button size="sm" onClick={() => setOpen(true)}>
        <Plus className="size-3.5" /> Service
      </Button>
      <Drawer open={open} onOpenChange={setOpen}>
        <ServiceFormBody env={env} workspace={params.tenant} onClose={() => setOpen(false)} />
      </Drawer>
    </>
  )
}

function routeHostError(host: string, exposure: Route['exposure']): string | null {
  if (!host) return exposure === 'public' ? 'Public Routes require a DNS hostname.' : null
  if (host.length > 253 || host.endsWith('.') || host !== host.toLowerCase()) {
    return 'Use a lowercase ASCII DNS hostname without a trailing dot.'
  }
  if (/^\d+(?:\.\d+){3}$/.test(host)) return 'Use a DNS hostname, not an IP address.'
  const labels = host.split('.')
  if (labels.some((label) => !/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(label))) {
    return 'Each DNS label must use letters, numbers, or internal hyphens.'
  }
  return null
}

function routePathError(path: string): string | null {
  if (path.length > 2048) return 'Route paths cannot exceed 2048 bytes.'
  if (!path.startsWith('/')) return 'Route paths must start with /.'
  if (/[?#]/.test(path)) return 'Route paths cannot contain a query or fragment.'
  const firstWildcard = path.indexOf('*')
  if (firstWildcard >= 0 && (firstWildcard !== path.length - 1 || path.lastIndexOf('*') !== firstWildcard)) {
    return 'A Route path may use one wildcard only, at the end.'
  }
  if (/%(?![0-9A-Fa-f]{2})/u.test(path)) return 'Percent escapes must use exactly two hexadecimal digits.'
  if (/[^A-Za-z0-9/%\-._~:@!$&()+,;=*]/u.test(path)) {
    return 'Route paths contain a character that cannot be rendered safely.'
  }
  return null
}

function RouteFormDialog({ env, open, onOpenChange }: { env: Environment; open: boolean; onOpenChange: (v: boolean) => void }) {
  const store = useStore()
	const [host, setHost] = useState('')
	const [path, setPath] = useState('/')
	const [target, setTarget] = useState(env.services[0]?.id ?? '')
	const [targetPort, setTargetPort] = useState('')
	const [exposure, setExposure] = useState<'public' | 'internal'>('internal')
	const [submitting, setSubmitting] = useState(false)
	const [submitError, setSubmitError] = useState<string>()
	const normalizedHost = host.trim()
	const normalizedPath = path.trim()
	const hostValidationError = routeHostError(normalizedHost, exposure)
	const pathValidationError = routePathError(normalizedPath)
	const validTargetPort = /^\d+$/.test(targetPort) && Number(targetPort) >= 1 && Number(targetPort) <= 65535

	const submit = async () => {
		setSubmitting(true)
		setSubmitError(undefined)
		try {
			await store.addRoute(env.id, {
				host: normalizedHost,
				path: normalizedPath,
				exposure,
				targetServiceId: target,
				targetPort: Number(targetPort),
			})
			onOpenChange(false)
			setHost('')
			setPath('/')
			setTargetPort('')
		} catch (error) {
			setSubmitError(error instanceof Error ? error.message : 'Unable to create Route')
		} finally {
			setSubmitting(false)
		}
	}
  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>Add route · {env.name}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          <p className="text-xs text-muted-foreground">
            A Route sends traffic to a Service. Public Routes require separately managed ingress components to be served;
            creating a Route never enables them.
          </p>
          <div className="flex flex-col gap-1.5">
			<Label htmlFor="r-host">Host</Label>
			<Input id="r-host" value={host} onChange={(event) => setHost(event.target.value)} placeholder="app.example.com" aria-invalid={hostValidationError !== null} autoFocus />
			<p className={hostValidationError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}>
			  {hostValidationError ?? 'Required for public Routes; optional for hostless internal Routes.'}
			</p>
		  </div>
		  <div className="flex flex-col gap-1.5">
			<Label htmlFor="r-path">Path</Label>
			<Input id="r-path" value={path} onChange={(event) => setPath(event.target.value)} placeholder="/api/*" aria-invalid={pathValidationError !== null} />
			<p className={pathValidationError ? 'text-xs text-destructive' : 'text-xs text-muted-foreground'}>
			  {pathValidationError ?? 'Absolute path with one optional terminal wildcard.'}
			</p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-target">Target service</Label>
            <Select
              id="r-target"
              value={target}
              onValueChange={setTarget}
              options={
                env.services.length > 0
				  ? env.services.map((service) => ({ value: service.id, label: service.name }))
                  : [{ value: '', label: '— no services yet —' }]
              }
            />
		  </div>
		  <div className="flex flex-col gap-1.5">
			<Label htmlFor="r-target-port">Target port</Label>
			<Input
			  id="r-target-port"
			  inputMode="numeric"
			  value={targetPort}
			  onChange={(event) => setTargetPort(event.target.value)}
			  placeholder="8080"
			/>
		  </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="r-exposure">Exposure</Label>
            <Select
              id="r-exposure"
              value={exposure}
				onValueChange={(v) => setExposure(v as 'public' | 'internal')}
              options={[
                { value: 'public', label: 'public — needs ingress component' },
                { value: 'internal', label: 'internal — no host port' },
              ]}
            />
          </div>
          {submitError ? <p role="alert" className="text-sm text-destructive">{submitError}</p> : null}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            data-action-id="route.create"
            disabled={hostValidationError !== null || pathValidationError !== null || !target || !validTargetPort || submitting}
            onClick={() => void submit()}
          >
            {submitting ? 'Adding...' : 'Add route'}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  )
}

function AttachFormDialog({ env, open, onOpenChange }: { env: Environment; open: boolean; onOpenChange: (v: boolean) => void }) {
  const store = useStore()
  const available = store.backingProjects.filter((g) => g.status !== 'stopped')
  const [gid, setGid] = useState(available[0]?.id ?? '')
  const [service, setService] = useState(env.services[0]?.name ?? '')
  const [credentialMode, setCredentialMode] = useState<'new' | 'existing'>('new')
  const [credentialAttachId, setCredentialAttachId] = useState('')
  const [grants, setGrants] = useState<string[]>([])
  const [name, setName] = useState('')
  const [saveError, setSaveError] = useState<string>()
  const [saving, setSaving] = useState(false)
  const g = store.getBackingProject(gid)
  const svc = g?.environments?.[0]?.services[0]
  const adapter = store.adapters.find((a) => a.key === svc?.adapter)
  const manual = !!adapter?.manual
  const authenticationDetails = valkeyAuthenticationDetails(svc?.authentication)
  const authenticationUnavailable = svc?.adapter === 'valkey:9' && !authenticationDetails
  const needsDatabase = adapter?.requires.database ?? true
  const attachName = name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-')
  const alreadyAttached = env.attaches.filter((a) => a.projectId === gid)
  const credentialOwners = alreadyAttached.filter(
    (attach) => attach.backingServiceId === svc?.id && attach.credential.mode === 'new' && attach.status === 'healthy',
  )
  const grantOptions = env.attaches.filter((a) => a.projectId === gid && a.database !== '—')
  const factRows = manual
    ? []
    : authenticationDetails
      ? authenticationDetails.factSuffixes.map((suffix) => `${adapter?.prefix ?? 'service'}_${suffix}`)
      : authenticationUnavailable
        ? []
        : [
            `${adapter?.prefix ?? 'service'}_HOST`,
            `${adapter?.prefix ?? 'service'}_PORT`,
            ...(needsDatabase ? [`${adapter?.prefix ?? 'service'}_DATABASE`] : []),
            `${adapter?.prefix ?? 'service'}_ROLE`,
            `${adapter?.prefix ?? 'service'}_PASSWORD`,
            `${adapter?.prefix ?? 'service'}_URL`,
          ]

  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>Attach backing · {env.name}</DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          <p className="text-xs text-muted-foreground">
            {manual ? (
              <>
                Attaching grants a <span className="font-medium text-foreground">specific service</span> network access to{' '}
                <span className="font-mono">{g?.name}</span> — the service joins its network (zone{' '}
                <span className="font-mono">{svc?.serviceName}</span>) and that&apos;s all: this is a{' '}
                <span className="font-medium text-foreground">manual</span> adapter, no auto-provisioning, no facts, no
                credentials. Reach the service at <span className="font-mono">{svc?.serviceName}</span> from the joining
                service.
              </>
            ) : authenticationDetails ? (
              <>
                Attaching grants a <span className="font-medium text-foreground">specific service</span> access to{' '}
                <span className="font-mono">{g?.name}</span> and inherits the backing instance&apos;s immutable{' '}
                <span className="font-medium text-foreground">{authenticationDetails.label.toLowerCase()}</span> authentication mode.{' '}
                {authenticationDetails.summary} The attach name below is only the spec key; the mode cannot be changed here.
              </>
            ) : (
              <>
                Attaching grants a <span className="font-medium text-foreground">specific service</span> access to a backing
                service and provisions its own database + role on the shared instance — named{' '}
                <span className="font-mono">&lt;service&gt;_&lt;first-6-of-attach-id&gt;</span> (the attach id&apos;s random
                tail, so every database on this shared instance stays unique — millions of attaches, no collisions). The
                attach name below is only the spec key (unique per environment, e.g. <span className="font-mono">api-db</span>).
                You may attach the same backing service multiple times.
              </>
            )}
          </p>
          <div className="flex flex-col gap-1.5">
            <Label>Attach name (the spec key, unique per environment)</Label>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder={service ? `${service}-db` : 'api-db'}
              className="font-mono"
            />
            {attachName && !manual && needsDatabase && (
              <p className="text-xs text-muted-foreground">
                the Controller generates the collision-resistant database, role, password, and URL
              </p>
            )}
            {attachName && authenticationDetails && (
              <p className="text-xs text-muted-foreground">
                {svc?.authentication === 'none'
                  ? 'the Controller creates a self-owned fact binding; it generates no credential'
                  : `the Controller provisions the ${authenticationDetails.label.toLowerCase()} identity and mode-appropriate facts`}
              </p>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            <Label>Backing service</Label>
            <Select
              value={gid}
              onValueChange={(v) => {
                setGid(v)
                setGrants([])
                setCredentialMode('new')
                setCredentialAttachId('')
              }}
              options={
                available.length > 0
                  ? available.map((p) => ({ value: p.id, label: `${p.name} (${p.environments?.[0]?.services[0]?.adapter ?? '—'})` }))
                  : [{ value: '', label: '— no backing services running —' }]
              }
            />
            {alreadyAttached.length > 0 && (
              <p className="text-xs text-muted-foreground">
                already attached as{' '}
                {alreadyAttached.map((a) => a.name).join(', ')} —{' '}
                {manual
                  ? 'attaching again joins the network again (still no provisioning)'
                  : authenticationDetails
                    ? svc?.authentication === 'none'
                      ? 'attaching again creates another fact owner and network membership, with no credential'
                      : `attaching again provisions another ${authenticationDetails.label.toLowerCase()} identity`
                  : 'attaching again provisions another database + role'}
              </p>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            <Label>Service that gains access</Label>
            {env.services.length === 0 && <p className="text-xs text-muted-foreground">no services yet — add a service first</p>}
            <Select
              value={service}
              onValueChange={setService}
              options={env.services.length > 0
                ? env.services.map((candidate) => ({ value: candidate.name, label: candidate.name }))
                : [{ value: '', label: '— no services yet —' }]}
            />
          </div>
          {!manual && (
            <div className="flex flex-col gap-1.5">
              <Label>{authenticationDetails?.credentialLabel ?? 'Credential'}</Label>
              <Select
                value={credentialMode}
                onValueChange={(value) => {
                  const mode = value as 'new' | 'existing'
                  setCredentialMode(mode)
                  setGrants([])
                  if (mode === 'new') setCredentialAttachId('')
                }}
                options={[
                  { value: 'new', label: authenticationDetails?.newOwnerLabel ?? 'Create new credential' },
                  { value: 'existing', label: authenticationDetails?.existingOwnerLabel ?? 'Use existing credential' },
                ]}
              />
              {credentialMode === 'existing' && (
                <Select
                  value={credentialAttachId}
                  onValueChange={setCredentialAttachId}
                  options={credentialOwners.length > 0
                    ? credentialOwners.map((attach) => ({ value: attach.id, label: `${attach.name} (${attach.service})` }))
                    : [{ value: '', label: '— no ready credential owner —' }]}
                />
              )}
            </div>
          )}
          {credentialMode === 'new' && grantOptions.length > 0 && (
            <div className="flex flex-col gap-1.5">
              <Label>Also grant access to (other attaches' databases, same role)</Label>
              <div className="flex flex-wrap gap-2">
                {grantOptions.map((grant) => (
                  <label key={grant.id} className="flex items-center gap-1.5 rounded-lg border border-border bg-surface px-2.5 py-1.5 text-xs">
                    <input
                      type="checkbox"
                      checked={grants.includes(grant.id)}
                      onChange={(e) =>
                        setGrants((prev) => (e.target.checked ? [...prev, grant.id] : prev.filter((x) => x !== grant.id)))
                      }
                      className="accent-primary"
                    />
                    <span className="font-mono">{grant.name}</span>
                  </label>
                ))}
              </div>
            </div>
          )}
          {authenticationDetails && (
            <div className="flex flex-col gap-1.5">
              <Label>{credentialMode === 'new' ? 'Provisioning for this mode' : 'Existing owner behavior'}</Label>
              <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
                {credentialMode === 'existing' ? (
                  <p className="text-xs text-muted-foreground">
                    Reuses the selected owner&apos;s facts and only reconciles this Service&apos;s network membership. It provisions no new identity.
                  </p>
                ) : authenticationDetails.provision.length === 0 ? (
                  <p className="text-xs text-muted-foreground">
                    No credential is generated and no ACL identity is provisioned. The Attach creates the self-owned fact binding and joins the network.
                  </p>
                ) : (
                  authenticationDetails.provision.map((operation) => (
                    <div key={operation.op} className="flex items-baseline gap-2.5 text-xs">
                      <span className="w-36 shrink-0 font-mono text-primary">{operation.op}</span>
                      <span className="break-all font-mono text-muted-foreground">{operation.detail}</span>
                    </div>
                  ))
                )}
              </div>
            </div>
          )}
          {manual ? (
            <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
              <span>
                <span className="font-medium text-foreground">Network-only attach.</span> No facts, no credentials, no
                provisioning steps — the service simply joins the zone{' '}
                <span className="font-mono">{svc?.serviceName}</span> and can reach{' '}
                <span className="font-mono">{svc?.serviceName}</span> directly. The operator runs and manages this
                service themselves.
              </span>
            </div>
          ) : authenticationUnavailable ? (
            <p role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
              The Controller did not return this Valkey backing instance&apos;s authentication mode. Refresh before attaching.
            </p>
          ) : (
            (
              <div className="flex flex-col gap-1.5">
                <Label>Facts you will see immediately after attaching (create env vars from these — names are yours)</Label>
                <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
                  {factRows.map((key) => (
                    <div key={key} className="flex items-center justify-between gap-2 font-mono text-[11px]">
                      <span className="text-muted-foreground">{key}</span>
                      <span className="text-foreground">resolved by Controller</span>
                    </div>
                  ))}
                </div>
                {grants.length > 0 && (
                  <p className="text-xs text-muted-foreground">
                    {grants.length} additional grant fact set{grants.length === 1 ? '' : 's'}
                  </p>
                )}
              </div>
            )
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          {saveError ? <p className="text-xs text-destructive">{saveError}</p> : null}
          <Button
            disabled={saving || authenticationUnavailable || !gid || !attachName || !svc?.id || !service || (credentialMode === 'existing' && !credentialAttachId)}
            onClick={() => {
              if (!svc?.id) return
              setSaving(true)
              setSaveError(undefined)
              void store
                .addAttach(env.id, {
                  serviceId: env.services.find((candidate) => candidate.name === service)?.id ?? service,
                  backingServiceId: svc.id,
                  name: attachName,
                  credential: credentialMode === 'new'
                    ? { mode: 'new' }
                    : { mode: 'existing', attachId: credentialAttachId },
                  grantAttachIds: credentialMode === 'new' && grants.length > 0 ? grants : undefined,
                })
                .then(() => {
                  onOpenChange(false)
                  setService(env.services[0]?.name ?? '')
                  setCredentialMode('new')
                  setCredentialAttachId('')
                  setGrants([])
                  setName('')
                })
                .catch((cause: unknown) => {
                  setSaveError(cause instanceof Error ? cause.message : 'Unable to attach backing service')
                })
                .finally(() => setSaving(false))
            }}
          >
            {saving ? 'Dispatching…' : 'Attach'}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  )
}

const BACKUP_FREQUENCY = /^(?:\*-\*-\*|(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) \*-\*-\*) (?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d$/
const MAX_BACKUP_POLICY_SOURCES = 12

function validateBackupPolicy(
  input: BackupPolicyReplacement,
  connectorAvailable: boolean,
  sourcesAvailable: boolean,
): string | null {
  if (input.sources.length > MAX_BACKUP_POLICY_SOURCES) return 'Select at most 12 sources.'
  const sourceKeys = input.sources.map((source) => `${source.kind}:${source.targetId}`)
  if (new Set(sourceKeys).size !== sourceKeys.length) return 'Each source can be selected only once.'
  if (input.frequency !== undefined && !BACKUP_FREQUENCY.test(input.frequency)) {
    return 'Enter a valid daily or weekly UTC frequency.'
  }
  if (input.keep !== undefined && !isValidBackupPolicyKeep(input.keep)) {
    return `Retention must be an integer between 1 and ${MAXIMUM_BACKUP_POLICY_KEEP}.`
  }
  if (input.sources.some((source) => source.kind === 'config') && input.encryption !== 'age') {
    return 'Environment config contains secret values and requires age encryption.'
  }
  const configured = input.frequency !== undefined
    || input.keep !== undefined
    || input.encryption !== undefined
    || input.connectorId !== undefined
    || input.sources.length > 0
  if (configured) {
    if (input.frequency === undefined) return 'A configured policy requires a daily or weekly UTC frequency.'
    if (input.keep === undefined) return 'A configured policy requires a positive retention count.'
    if (input.encryption === undefined) return 'A configured policy requires an encryption choice.'
    if (input.connectorId === undefined) return 'A configured policy requires a Connector.'
    if (input.sources.length === 0) return 'A configured policy requires at least one source.'
  }
  if (!input.enabled) return null
  if (!configured) return 'Configure the policy before enabling backups.'
  if (!sourcesAvailable) return 'Every selected source must still exist in this Environment.'
  if (!connectorAvailable) return 'Select a Connector owned by this Environment.'
  return null
}

function BackupPolicyDialog({ env, open, onOpenChange }: { env: Environment; open: boolean; onOpenChange: (v: boolean) => void }) {
  const store = useStore()
  const policyState = store.getBackupPolicyState(env.id)
  const backup = policyState.policy
  const [enabled, setEnabled] = useState(false)
  const [frequency, setFrequency] = useState('')
  const [keep, setKeep] = useState('')
  const [encryption, setEncryption] = useState<'age' | 'none' | ''>('')
  const [connector, setConnector] = useState('')
  const [sources, setSources] = useState<BackupPolicySourceInput[]>([])
  const openedEmpty = useRef(false)
  const autoFilledFrequency = useRef(false)
  const autoFilledKeep = useRef(false)
  const connectorOptions = store.connectors.filter((candidate) => candidate.scopeRef === env.id)
  const selectedConnector = connectorOptions.find((candidate) => candidate.id === connector)
  // Sources: one per attach (never per service — a shared attach is never
  // backed up twice), any subset of volumes, and the environment's CONFIG
  // (env entries: vars, files, secrets — values included, age-encrypted).
  const attachOptions = policyState.attaches.filter((attach) => {
    const backing = store.getBackingProject(attach.backingProjectId)
    return backing?.environments?.[0]?.services
      .find((service) => service.id === attach.backingServiceId)?.adapter === 'postgres:16'
  })
  const selectedRetainedAttachSources = backup.sources.filter((source) =>
    source.kind === 'attach'
    && sources.some((selected) => selected.kind === 'attach' && selected.targetId === source.targetId),
  )
  const unsupportedAttachSources = selectedRetainedAttachSources.filter((source) =>
    policyState.attaches.some((attach) => attach.id === source.targetId)
    && !attachOptions.some((attach) => attach.id === source.targetId),
  )
  const missingAttachSources = selectedRetainedAttachSources.filter((source) =>
    !policyState.attaches.some((attach) => attach.id === source.targetId),
  )
  const missingVolumeSources = backup.sources.filter((source) =>
    source.kind === 'volume'
    && sources.some((selected) => selected.kind === 'volume' && selected.targetId === source.targetId)
    && !policyState.volumes.some((volume) => volume.id === source.targetId),
  )
  useEffect(() => {
    if (!open) return
    setEnabled(backup.enabled)
    setFrequency(backup.frequency ?? '')
    setKeep(backup.keep === undefined ? '' : String(backup.keep))
    setEncryption(backup.encryption ?? '')
    setConnector(backup.connectorId ?? '')
    setSources(backup.sources.map(({ kind, targetId }) => ({ kind, targetId })))
    openedEmpty.current = !backupPolicyConfigured(backup)
    autoFilledFrequency.current = false
    autoFilledKeep.current = false
  }, [open])
  const includesSource = (kind: BackupPolicySourceInput['kind'], targetId: string) =>
    sources.some((source) => source.kind === kind && source.targetId === targetId)
  const toggleSource = (kind: BackupPolicySourceInput['kind'], targetId: string, checked: boolean) => {
    setSources((current) => checked
      ? current.some((source) => source.kind === kind && source.targetId === targetId)
        ? current
        : [...current, { kind, targetId }]
      : current.filter((source) => source.kind !== kind || source.targetId !== targetId))
  }
  const moveSource = (index: number, offset: -1 | 1) => {
    setSources((current) => {
      const destination = index + offset
      if (destination < 0 || destination >= current.length) return current
      const next = [...current]
      ;[next[index], next[destination]] = [next[destination], next[index]]
      return next
    })
  }
  const keepNumber = keep === '' ? undefined : /^\d+$/.test(keep) ? Number(keep) : Number.NaN
  const replacement: BackupPolicyReplacement = {
    enabled,
    frequency: frequency || undefined,
    keep: keepNumber,
    encryption: encryption || undefined,
    connectorId: connector || undefined,
    sources,
  }
  const sourcesAvailable = sources.every((source) => {
    if (source.kind === 'config') return source.targetId === env.id
    const catalog = source.kind === 'attach' ? attachOptions : policyState.volumes
    return catalog.some((candidate) => candidate.id === source.targetId)
  })
  const validationError = validateBackupPolicy(replacement, Boolean(selectedConnector), sourcesAvailable)
  const policyAuthoritative = policyState.loaded && !policyState.loading && !policyState.loadError
  const policyLocked = policyState.saving || !policyAuthoritative
  return (
    <Drawer open={open} onOpenChange={(next) => !policyState.saving && onOpenChange(next)}>
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>Backup policy · {deriveStrategy(store, env)}</DialogTitle>
        </DialogHeader>
        <fieldset disabled={policyLocked} className="contents">
        <div className="flex flex-col gap-4">
          <p className="text-xs text-muted-foreground">
            Strategy comes from the adapter. These are the operator&apos;s choices for this backup resource.
          </p>
          <label className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5">
            <div className="flex flex-col">
              <span className="text-sm font-medium">Backups</span>
              <span className="text-xs text-muted-foreground">
                {enabled ? 'enabled — scheduled runs per the policy below' : 'off — nothing is ever backed up (staging/dev friendly)'}
              </span>
            </div>
            <Switch
              checked={enabled}
              disabled={policyState.saving}
              onCheckedChange={(next) => {
                setEnabled(next)
                if (next) {
                  if (openedEmpty.current && !frequency) {
                    setFrequency('*-*-* 03:15:00')
                    autoFilledFrequency.current = true
                  }
                  if (openedEmpty.current && (keepNumber === undefined || !isValidBackupPolicyKeep(keepNumber))) {
                    setKeep('7')
                    autoFilledKeep.current = true
                  }
                } else {
                  if (autoFilledFrequency.current) setFrequency('')
                  if (autoFilledKeep.current) setKeep('')
                  autoFilledFrequency.current = false
                  autoFilledKeep.current = false
                }
              }}
            />
          </label>
          {enabled && (
            <>
          <div className="flex flex-col gap-1.5">
            <Label>Sources · what to back up</Label>
            <p className="text-xs text-muted-foreground">
              One source per attach, not per service — a shared attach (api + worker + scheduler) is backed up once. One run
              backs up every selected source in the order shown below. Select only what you need — at most 12 sources.
            </p>
            {attachOptions.length === 0 && (
              <p className="text-xs text-muted-foreground">no supported PostgreSQL attaches yet</p>
            )}
            {unsupportedAttachSources.map((source) => (
              <div key={source.id} className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning/5 px-3 py-2 text-xs text-warning">
                <span>{backupSourceLabel(store, env, source)} survives, but its Attach kind is unsupported for MVP backup. Remove it before enabling.</span>
                <Button variant="outline" size="sm" onClick={() => toggleSource('attach', source.targetId, false)}>Remove</Button>
              </div>
            ))}
            {missingAttachSources.map((source) => (
              <div key={source.id} className="flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
                <span>Missing Attach target {source.targetId}. Remove this stale source before enabling.</span>
                <Button variant="outline" size="sm" onClick={() => toggleSource('attach', source.targetId, false)}>Remove</Button>
              </div>
            ))}
            {missingVolumeSources.map((source) => (
              <div key={source.id} className="flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
                <span>Missing Volume target {source.targetId}. Remove this stale source before enabling.</span>
                <Button variant="outline" size="sm" onClick={() => toggleSource('volume', source.targetId, false)}>Remove</Button>
              </div>
            ))}
            <div className="flex flex-col gap-1.5">
              {attachOptions.map((a) => {
                const g = store.getBackingProject(a.backingProjectId)
                const checked = includesSource('attach', a.id)
                return (
                  <label key={a.id} className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs">
                    <span className="flex items-center gap-2">
                      <input
                        type="checkbox"
                        checked={checked}
                        disabled={policyState.saving || (!checked && sources.length >= MAX_BACKUP_POLICY_SOURCES)}
                        onChange={(e) => toggleSource('attach', a.id, e.target.checked)}
                        className="accent-primary"
                      />
                      <Database className="size-3.5 text-muted-foreground" />
                      <span className="font-mono">{g?.name ?? a.name}</span>
                      <span className="font-mono text-muted-foreground">{a.name}</span>
                    </span>
                    <span className="font-mono text-[10px] text-muted-foreground">{a.id}</span>
                  </label>
                )
              })}
            </div>
            {policyState.volumes.length > 0 && (
              <div className="flex flex-col gap-1.5">
                <span className="text-xs font-medium text-muted-foreground">Volumes · pick a few, not all</span>
                {policyState.volumes.map((v) => {
                  const checked = includesSource('volume', v.id)
                  return (
                    <label key={v.id} className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs">
                      <span className="flex items-center gap-2">
                        <input
                          type="checkbox"
                          checked={checked}
                          disabled={policyState.saving || (!checked && sources.length >= MAX_BACKUP_POLICY_SOURCES)}
                          onChange={(e) => toggleSource('volume', v.id, e.target.checked)}
                          className="accent-primary"
                        />
                        <HardDrive className="size-3.5 text-muted-foreground" />
                        <span className="font-mono">{v.slug}</span>
                        <span className="font-mono text-muted-foreground">key: {v.key}</span>
                      </span>
                      <span className="font-mono text-[10px] text-muted-foreground">{v.id}</span>
                    </label>
                  )
                })}
              </div>
            )}
            <label className="flex items-start gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs">
              <input
                type="checkbox"
                checked={includesSource('config', env.id)}
                disabled={policyState.saving || (!includesSource('config', env.id) && sources.length >= MAX_BACKUP_POLICY_SOURCES)}
                onChange={(e) => toggleSource('config', env.id, e.target.checked)}
                className="mt-0.5 accent-primary"
              />
              <span className="flex flex-col gap-1">
                <span className="font-medium text-foreground">Environment config — env vars, files &amp; secrets (values included)</span>
                <span className="text-muted-foreground">
                  Exported and age-encrypted like any source; restore replaces this environment&apos;s entries. Does{' '}
                  <span className="font-medium text-warning">NOT</span> back up backing environments — postgres,
                  valkey, and their configs are never included — and never platform state.
                </span>
              </span>
            </label>
            <p className="text-xs text-muted-foreground">{sources.length} / {MAX_BACKUP_POLICY_SOURCES} sources selected.</p>
            {sources.length > 0 && (
              <div className="flex flex-col gap-1.5 rounded-lg border border-border bg-surface p-2">
                <span className="text-xs font-medium text-muted-foreground">Backup order</span>
                {sources.map((source, index) => (
                  <div key={`${source.kind}:${source.targetId}`} className="flex items-center gap-2 rounded-md bg-background px-2 py-1.5 text-xs">
                    <span className="w-5 font-mono text-muted-foreground">{index + 1}</span>
                    <span className="min-w-0 flex-1 truncate font-mono">
                      {backupSourceInputLabel(store, env, source)}
                    </span>
                    <Button variant="ghost" size="icon" disabled={policyState.saving || index === 0} aria-label={`Move ${backupSourceInputLabel(store, env, source)} up`} onClick={() => moveSource(index, -1)}>
                      <ChevronUp className="size-3.5" />
                    </Button>
                    <Button variant="ghost" size="icon" disabled={policyState.saving || index === sources.length - 1} aria-label={`Move ${backupSourceInputLabel(store, env, source)} down`} onClick={() => moveSource(index, 1)}>
                      <ChevronDown className="size-3.5" />
                    </Button>
                  </div>
                ))}
              </div>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="bp-freq">Frequency · UTC</Label>
            <Input
              id="bp-freq"
              disabled={policyState.saving}
              value={frequency}
              onChange={(event) => {
                autoFilledFrequency.current = false
                setFrequency(event.target.value)
              }}
              placeholder="*-*-* 03:15:00"
            />
            <p className="text-xs text-muted-foreground">Daily: <span className="font-mono">*-*-* HH:MM:SS</span>. Weekly: <span className="font-mono">Mon *-*-* HH:MM:SS</span>. Exact spacing, UTC only.</p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="bp-keep">Retention (backups kept)</Label>
            <Input
              id="bp-keep"
              type="number"
              min={1}
              max={MAXIMUM_BACKUP_POLICY_KEEP}
              step={1}
              disabled={policyState.saving}
              value={keep}
              onChange={(event) => {
                autoFilledKeep.current = false
                setKeep(event.target.value)
              }}
              placeholder="7"
            />
            <p className="text-xs text-muted-foreground">Enter an integer from 1 to {MAXIMUM_BACKUP_POLICY_KEEP} while enabled.</p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label>Encryption</Label>
            <div aria-disabled={policyState.saving} className={cn(policyState.saving && 'pointer-events-none opacity-60')}>
              <Select
                value={encryption}
                onValueChange={(v) => !policyState.saving && setEncryption(v as 'age' | 'none')}
                placeholder="Select encryption"
                options={[
                  { value: 'age', label: 'age — encrypted' },
                  { value: 'none', label: 'unencrypted' },
                ]}
              />
            </div>
          </div>
          {encryption === 'age' && (
            <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">Recipient (generated per environment)</span>
              <div className="flex items-center justify-between gap-2">
                <span className="truncate font-mono text-xs text-foreground">{backup.ageRecipient ?? '— generated on first enable —'}</span>
                {backup.ageRecipient && <CopyButton value={backup.ageRecipient} />}
              </div>
              <p className="text-xs text-muted-foreground">
                Managed on the Backups tab: export the current identity repeatably with no-store for DR; rotate per environment.
              </p>
            </div>
          )}
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="bp-connector">Connector</Label>
            <div aria-disabled={policyState.saving} className={cn(policyState.saving && 'pointer-events-none opacity-60')}>
              <Select
                id="bp-connector"
                value={connector}
                onValueChange={(value) => !policyState.saving && setConnector(value)}
                placeholder="Select an environment connector"
                options={connectorOptions.map((candidate) => ({ value: candidate.id, label: candidate.name }))}
              />
            </div>
            {selectedConnector ? (
              <p className="font-mono text-xs text-muted-foreground">
                s3://{selectedConnector.bucket}/{selectedConnector.prefix}
              </p>
            ) : (
              <p className="text-xs text-destructive">Create and select a connector owned by this environment.</p>
            )}
          </div>
          </>
          )}
        </div>
        </fieldset>
        <DialogFooter>
          {validationError && <p className="mr-auto text-xs text-destructive">{validationError}</p>}
          {policyState.saveError && <p className="mr-auto text-xs text-destructive">{policyState.saveError}</p>}
          <Button variant="outline" disabled={policyState.saving} onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            disabled={policyLocked || Boolean(validationError)}
            onClick={() => {
              if (validationError || !policyAuthoritative) return
              void store.replaceBackupPolicy(env.id, replacement).then(() => onOpenChange(false)).catch(() => undefined)
            }}
          >
            {policyState.saving ? 'Saving…' : 'Save policy'}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  )
}

function backupSourceInputLabel(
  store: ReturnType<typeof useStore>,
  env: Environment,
  source: BackupPolicySourceInput,
) {
  return backupSourceLabel(store, env, { id: '', ...source })
}
