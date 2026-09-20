'use client'

import { FormSection } from '@/components/ui/form-section'
import { NetworkRange } from '@/components/common/network-range'
import { BackingHookFields } from '@/features/backing-service/hook-fields'
import type { BackingHooks } from '@/features/backing-service/api'

import { Select } from '@/components/ui/select'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { ArrowRight, Database, HardDrive, Plug, Plus, RefreshCw } from 'lucide-react'
import { useStore } from '@/lib/store'
import { useLinkedSlug } from '@/lib/use-linked-slug'
import { PageHeader } from '@/components/common/page-header'
import { StatusBadge } from '@/components/common/status-badge'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/common/empty-state'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import {
  backingAuthenticationCreateFields,
  valkeyAuthenticationDetails,
  type ValkeyAuthenticationSelection,
} from '@/lib/valkey-authentication'
import { serviceObservationState } from '@/features/service/service-observation'
import { useVisibleServiceObservations } from '@/features/service/use-service-observation-refresh'
import { Checkbox } from '@/components/ui/checkbox'

export default function PlatformBackingServicesPage() {
  const store = useStore()
  const visibleEnvironments = store.backingProjects.flatMap((project) => project.environments?.slice(0, 1) ?? [])
  const observationRefresh = useVisibleServiceObservations({
    environmentIds: visibleEnvironments.map((environment) => environment.id),
    observations: visibleEnvironments.flatMap((environment) => environment.services.map((service) => service.observation)),
    refreshEnvironment: store.refreshEnvironmentServices,
  })
  const [createOpen, setCreateOpen] = useState(false)
  const { name, slug, setName, setSlug, reset: resetBackingIdentity } = useLinkedSlug()
  const [description, setDescription] = useState('')
  const [adapter, setAdapter] = useState<'postgres:16' | 'valkey:9' | 'custom'>('postgres:16')
  const [image, setImage] = useState('')
  const [hooks, setHooks] = useState<BackingHooks>({})
  const [hookError, setHookError] = useState<string | null>(null)
  const [authentication, setAuthentication] = useState<ValkeyAuthenticationSelection>('')
  const [networkPool, setNetworkPool] = useState('10.200.0.0/16')
  const [zoneName, setZoneName] = useState('data')
  const [zoneSubnet, setZoneSubnet] = useState('10.200.20.0/24')
  const [zoneInternal, setZoneInternal] = useState(true)
  const [createError, setCreateError] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)

  const openCreate = () => setCreateOpen(true)
  const createFields = adapter === 'custom'
    ? image.trim() ? { adapter: 'custom' as const, image: image.trim(), hooks } : undefined
    : backingAuthenticationCreateFields(adapter, authentication)

  const createBackingService = async () => {
    if (adapter === 'custom' && hookError) return
    if (!createFields) {
      setCreateError(adapter === 'custom'
        ? 'Enter the container image for this custom backing service.'
        : 'Select an authentication mode for this Valkey backing service.')
      return
    }

    setCreating(true)
    setCreateError(null)
    try {
      await store.addBackingProject({
        slug: slug.trim(),
        name: name.trim(),
        description: description.trim() || undefined,
        ...createFields,
        network_pool: networkPool.trim(),
        zone: { name: zoneName.trim(), subnet: zoneSubnet.trim(), internal: zoneInternal },
      })
      setCreateOpen(false)
      resetBackingIdentity()
      setDescription('')
      setImage('')
      setHooks({})
      setHookError(null)
      setAuthentication('')
    } catch (error) {
      setCreateError(error instanceof Error ? error.message : 'Unable to create backing service')
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Backing services"
        description="Shared services run once and connected to individual consumer services. Built-in adapters provide database provisioning; custom containers can use optional hooks."
        icon={<Database />}
        actions={
          <Button onClick={openCreate}>
            <Plus className="size-4" /> New backing service
          </Button>
        }
      />

      {observationRefresh.refreshError && (
        <p role="alert" className="text-sm text-destructive">
          Runtime refresh failed; evidence will expire locally. {observationRefresh.refreshError}
        </p>
      )}

      {store.backingProjectsLoading ? (
        <EmptyState
          icon={<Database />}
          title="Loading backing services"
          description="Reading the platform-owned backing Project, main Environment, and adapter Service facades."
        />
      ) : store.backingProjectError ? (
        <EmptyState
          icon={<Database />}
          title="Backing services unavailable"
          description={store.backingProjectError}
        />
      ) : store.backingProjects.length === 0 ? (
        <EmptyState
          icon={<Database />}
          title="No backing services"
          description="Create a backing service explicitly — it is never lazily created. Then environments can attach to it."
          action={
            <Button onClick={openCreate}>
              <Plus className="size-4" /> New backing service
            </Button>
          }
        />
      ) : (
        <div className="grid gap-4 md:grid-cols-2">
          {store.backingProjects.map((g) => {
            const env = g.environments?.[0]
            const svc = env?.services[0]
            const adapter = store.adapters.find((a) => a.key === svc?.adapter)
            const authenticationDetails = valkeyAuthenticationDetails(svc?.authentication)
            const runtimeState = svc ? serviceObservationState(svc.observation, observationRefresh.now) : 'unavailable'
            const port = adapter?.urlScheme === 'redis' ? 6379 : adapter?.urlScheme === 'pgsql' ? 5432 : undefined
            const backupSourceCount = store.tenantProjects.reduce(
              (count, project) => count + (project.environments ?? []).reduce(
                (environmentCount, environment) => environmentCount + (environment.backup?.sources ?? []).filter(
                  (source) => source.kind === 'attach' && environment.attaches.find(
                    (attach) => attach.id === source.ref && attach.projectId === g.id,
                  ),
                ).length,
                0,
              ),
              0,
            )
            return (
              <Link
                key={g.id}
                to={`/platform/backing-services/${g.id}`}
                className="group flex flex-col rounded-xl border border-border bg-card p-4 transition-colors hover:border-ring/50"
              >
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <span className="flex size-10 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary [&_svg]:size-5">
                      <Database />
                    </span>
                    <div className="flex flex-col">
                      <div className="flex items-center gap-2">
                        <span className="text-base font-semibold">{g.name}</span>
                        {env && <StatusBadge status={env.status} label={`Provisioning ${env.provisioningState}`} />}
                        <StatusBadge status={runtimeState} label={`Runtime ${runtimeState}`} />
                      </div>
                      <span className="font-mono text-xs text-muted-foreground">
                        {svc?.serviceName}{port ? `:${port}` : ''} · {svc?.adapter}
                        {authenticationDetails ? ` · ${authenticationDetails.label}` : ''}
                      </span>
                    </div>
                  </div>
                  <ArrowRight className="size-4 text-muted-foreground/40 transition-transform group-hover:translate-x-0.5 group-hover:text-foreground" />
                </div>
                <p className="mt-2 flex-1 text-sm text-muted-foreground">{g.description}</p>
                <div className="mt-3 flex flex-wrap items-center gap-x-6 gap-y-2 border-t border-border pt-3">
                  <MiniStat icon={<Plug />} value={g.consumers?.length ?? 0} label="consumers" />
                  <MiniStat icon={<RefreshCw />} value={backupSourceCount} label="backup sources" />
                  <MiniStat icon={<HardDrive />} value={svc?.resources.mem ?? '—'} label="memory" />
                </div>
              </Link>
            )
          })}
        </div>
      )}

      <Drawer open={createOpen} onOpenChange={setCreateOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New backing service</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-5">
          <FormSection title="Identity" description="How this backing service appears in Groundplane.">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="backing-name">Name</Label>
              <Input id="backing-name" value={name} onChange={(event) => setName(event.target.value)} placeholder="Primary database" autoFocus />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="backing-slug">Slug</Label>
              <Input id="backing-slug" value={slug} onChange={(event) => setSlug(event.target.value)} placeholder="primary-database" />
              <p className="text-xs text-muted-foreground">Follows Name until edited.</p>
            </div>
            <div className="flex flex-col gap-1.5 sm:col-span-2">
              <Label htmlFor="backing-description">Description (optional)</Label>
              <Input id="backing-description" value={description} onChange={(event) => setDescription(event.target.value)} placeholder="Shared application datastore" />
            </div>
          </FormSection>
          <FormSection title="Service type" description="Select a managed engine or run a custom container on its dedicated network.">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="backing-adapter">Adapter</Label>
              <Select id="backing-adapter" value={adapter} onValueChange={(value) => {
                if (value !== 'postgres:16' && value !== 'valkey:9' && value !== 'custom') return
                setAdapter(value)
                setImage('')
                setHooks({})
                setHookError(null)
                setAuthentication('')
              }} options={[
                { value: 'postgres:16', label: 'PostgreSQL 16' },
                { value: 'valkey:9', label: 'Valkey 9' },
                { value: 'custom', label: 'Custom container' },
              ]} />
            </div>
            {adapter === 'valkey:9' && (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="backing-authentication">Authentication</Label>
                <Select id="backing-authentication" value={authentication} placeholder="Select authentication"
                  onValueChange={(value) => { if (value === 'username_password' || value === 'password' || value === 'none') setAuthentication(value) }}
                  options={[{ value: 'username_password', label: 'Username + password' }, { value: 'password', label: 'Password only' }, { value: 'none', label: 'None · no authentication' }]} />
                <p className="text-xs text-muted-foreground">Immutable for this backing instance. Every Attach inherits this mode.</p>
              </div>
            )}
            {adapter === 'custom' && (
              <div className="flex flex-col gap-1.5 sm:col-span-2">
                <Label htmlFor="backing-image">Container image</Label>
                <Input
                  id="backing-image"
                  value={image}
                  onChange={(event) => setImage(event.target.value)}
                  placeholder="registry.example/internal/search:1.4"
                />
                <p className="text-xs text-muted-foreground">
                  Groundplane runs your image and connects consumers to its network. Optional hooks can provision
                  credentials and facts. No volumes, health checks, grants or backups are added automatically.
                </p>
              </div>
            )}
          </FormSection>
          <FormSection title="Network" description="Reserve a pool for this backing Environment, then choose its zone subnet within that pool.">
            <div className="flex flex-col gap-1.5 sm:col-span-2">
              <Label htmlFor="backing-pool">Environment pool (CIDR)</Label>
              <Input id="backing-pool" value={networkPool} onChange={(event) => setNetworkPool(event.target.value)} placeholder="10.200.0.0/16" />
            </div>
            <NetworkRange cidr={networkPool} label="Environment pool" />
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="backing-zone-name">Zone name</Label>
              <Input id="backing-zone-name" value={zoneName} onChange={(event) => setZoneName(event.target.value)} placeholder="data" />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="backing-subnet">Zone subnet (CIDR)</Label>
              <Input id="backing-subnet" value={zoneSubnet} onChange={(event) => setZoneSubnet(event.target.value)} placeholder="10.200.20.0/24" />
            </div>
            <label className="flex items-center gap-2 text-sm sm:col-span-2">
              <Checkbox checked={zoneInternal} onChange={(event) => setZoneInternal(event.target.checked)} />
              Internal network (no external access through this zone)
            </label>
            <p className="text-xs text-muted-foreground sm:col-span-2">Services on this zone can communicate with each other. External access requires another, non-internal zone. This does not change the host&apos;s internet access.</p>
          </FormSection>
            {adapter === 'custom' && <BackingHookFields value={hooks} onChange={setHooks} onError={setHookError} />}
            {adapter === 'custom' && hookError && <p role="alert" className="text-xs text-destructive">{hookError}</p>}
            {adapter === 'valkey:9' && authentication === 'none' && (
              <p role="status" className="rounded-lg border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning sm:col-span-2">
                Any client that can reach this backing service can access it without authentication.
              </p>
            )}
            {createError && <p role="alert" className="text-xs text-destructive sm:col-span-2">{createError}</p>}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button
              disabled={creating || (adapter === 'custom' && !!hookError) || !createFields || !slug.trim() || !name.trim() || !networkPool.trim() || !zoneName.trim() || !zoneSubnet.trim()}
              onClick={() => void createBackingService()}
            >
              {creating ? 'Creating...' : 'Create backing service'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
    </div>
  )
}

function MiniStat({ icon, value, label }: { icon: React.ReactNode; value: React.ReactNode; label: string }) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground [&_svg]:size-3.5">
      {icon}
      <span className="font-mono text-sm font-semibold text-foreground">{value}</span>
      <span className="text-muted-foreground/70">{label}</span>
    </span>
  )
}
