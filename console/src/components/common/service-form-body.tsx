'use client'

import { FormSection } from '@/components/ui/form-section'
import { BackingHookFields } from '@/features/backing-service/hook-fields'
import type { BackingHooks } from '@/features/backing-service/api'
import { useState } from 'react'
import { DrawerContent } from '@/components/ui/drawer'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { useStore } from '@/lib/store'
import type { Environment, Service } from '@/lib/types'
import { Checkbox } from '@/components/ui/checkbox'

// The full service form, shared between:
//  - tenant environments ("Add service" / "Edit service") — one container
//    with zones, healthcheck, resources, expose ports, restart…
//  - backing services ("New backing service") — the SAME form, plus the
//    adapter + facts-prefix fields, creating the backing project with one
//    environment ("main") and one adapter-backed service. A backing service
//    is a service: it gets the same full spec, never a reduced subset.

export type ServicePatch = {
  name: string
  image: string
  role: string
  zones: string[]
  strategy: Service['strategy']
  onFailure: 'switch_back' | 'leave_active'
  healthcheck: Service['healthcheck']
  resources: { mem: string; cpus: string }
  expose: string[]
  restart: Service['restart']
  replicas: number
  hooks?: BackingHooks
}

type BackingFields = {
  adapterKey: string
  prefix: string
  onAdapterChange: (key: string) => void
  onPrefixChange: (value: string) => void
}

type ServiceFormBodyProps =
  | {
      env: Environment
      workspace: string
      initial?: Service
      backing?: never
      onCreateBacking?: never
      onClose: () => void
    }
  | {
      env?: never
      workspace?: never
      initial?: never
      backing: BackingFields
      onCreateBacking: (patch: ServicePatch, adapter: string, prefix: string) => void
      onClose: () => void
    }

export function ServiceFormBody({
  env,
  initial,
  backing,
  onCreateBacking,
  onClose,
}: ServiceFormBodyProps) {
  const store = useStore()
  const editing = !!initial
  const isBacking = !!backing
  const selectedAdapter = store.adapters.find((adapter) => adapter.key === backing?.adapterKey)

  const [name, setName] = useState(initial?.name ?? '')
  const [image, setImage] = useState(initial?.image ?? '')
  const [role, setRole] = useState(initial?.role ?? '')
  const [strategy, setStrategy] = useState<Service['strategy']>(initial?.strategy ?? (isBacking ? 'recreate' : 'recreate'))
  const [onFailure, setOnFailure] = useState<'switch_back' | 'leave_active'>(initial?.onFailure ?? 'switch_back')
  const [zones, setZones] = useState<string[]>(initial?.zones ?? [])
  const [hcKind, setHcKind] = useState<'http' | 'tcp' | 'pgrep' | 'none'>(initial?.healthcheck?.kind ?? (isBacking ? 'tcp' : 'none'))
  const [hcTarget, setHcTarget] = useState(initial?.healthcheck?.target ?? (isBacking ? '5432' : ''))
  const [hcInterval, setHcInterval] = useState(initial?.healthcheck?.interval ?? '15s')
  const [hcTimeout, setHcTimeout] = useState(initial?.healthcheck?.timeout ?? '3s')
  const [hcStart, setHcStart] = useState(initial?.healthcheck?.startPeriod ?? '20s')
  const [mem, setMem] = useState(initial?.resources.mem ?? (isBacking ? '1g' : ''))
  const [cpus, setCpus] = useState(initial?.resources.cpus ?? (isBacking ? '1.0' : ''))
  const [expose, setExpose] = useState((initial?.expose ?? []).join(', '))
  const [exposeEdited, setExposeEdited] = useState((initial?.expose ?? []).length > 0)
  const [restart, setRestart] = useState<Service['restart']>(initial?.restart ?? 'unless-stopped')
  const [replicas, setReplicas] = useState(String(initial?.replicas ?? 1))
  const [hooks, setHooks] = useState<BackingHooks>(initial?.hooks ?? {})
  const [hookError, setHookError] = useState<string | null>(null)
  // A backing service owns its own network: the zone named after it is
  // created with it, and consumers join it as `external` when they attach —
  // the operator never picks a zone for a backing service.
  const backingZone = isBacking ? (name.trim().toLowerCase() || 'backing') : ''

  function applyAdapter(key: string) {
    const a = store.adapters.find((x) => x.key === key)
    if (!a) return
    const port = a.urlScheme === 'redis' ? '6379' : '5432'
    if (isBacking) {
      backing?.onAdapterChange(key)
      backing?.onPrefixChange(a.prefix)
      setHcTarget(port)
      if (!exposeEdited) setExpose(`${name.trim().toLowerCase() || 'svc'}:${port}`)
    }
  }

  function buildPatch(): ServicePatch {
    return {
      name: name.trim(),
      image: isBacking ? '' : image.trim(),
      role: role.trim() || (isBacking ? 'Adapter-backed service' : 'Added in the Console'),
      zones: isBacking ? [backingZone] : zones,
      strategy,
      onFailure,
      healthcheck:
        hcKind === 'none'
          ? null
          : { kind: hcKind, target: hcTarget.trim() || (hcKind === 'tcp' ? '8080' : '/up'), interval: hcInterval, timeout: hcTimeout, startPeriod: hcStart, retries: 3 },
      resources: { mem: mem.trim() || '128m', cpus: cpus.trim() || '0.25' },
      expose: expose.split(',').map((s) => s.trim()).filter(Boolean),
      restart,
      replicas: Math.max(1, parseInt(replicas, 10) || 1),
      hooks: initial?.adapter === 'custom' ? hooks : undefined,
    }
  }

  const [submitting, setSubmitting] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  async function save() {
    if (hookError) return
    if (isBacking && onCreateBacking) {
      onCreateBacking(buildPatch(), backing.adapterKey, backing.prefix)
      return
    }
    const patch = buildPatch()
	setSubmitting(true)
	setSubmitError(null)
	try {
	  if (initial && env) await store.updateService(env.id, initial.id, patch)
	  else if (env) await store.addService(env.id, patch)
	  onClose()
	} catch (error) {
	  setSubmitError(error instanceof Error ? error.message : 'Service mutation failed')
	} finally {
	  setSubmitting(false)
    }
  }

  return (
    <DrawerContent>
      <DialogHeader>
        <DialogTitle>
          {isBacking ? 'New backing service' : editing ? `Edit service · ${env?.name}` : `Add service · ${env?.name}`}
        </DialogTitle>
      </DialogHeader>
      <div className="flex flex-col gap-4">
        <p className="text-xs text-muted-foreground">
          {isBacking
            ? `A backing service is a service plus the adapter that knows how to provision, connect, and back it up. The adapter resolves its immutable managed workload release; operators do not select an image. It follows the same hierarchy: backing project → one environment ("main") → this service. It owns its own network: the zone "${backingZone}" is created with it, and consumers join it as external when they attach. It stays running even with zero consumers; only an explicit Destroy removes it.`
            : 'Define the container workload, its network and runtime settings. Saving changes desired configuration; deploy the service to apply it.'}
        </p>
        <FormSection title="Service" description="Name the service and choose its container image."><div className="flex flex-col gap-4 sm:col-span-2">
        {isBacking && (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="bs-adapter">Adapter</Label>
              <Select
                id="bs-adapter"
                value={backing.adapterKey}
                onValueChange={applyAdapter}
                options={store.adapters.map((a) => ({ value: a.key, label: a.key }))}
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="bs-prefix">Prefix (facts keys on attach — e.g. pg16_URL)</Label>
              <Input
                id="bs-prefix"
                value={backing.prefix}
                onChange={(e) => backing.onPrefixChange(e.target.value)}
                placeholder="pg16"
              />
            </div>
          </div>
        )}
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="sv-name">{isBacking ? 'Name (unique service name)' : 'Name'}</Label>
          <Input
            id="sv-name"
            value={name}
            onChange={(e) => {
              setName(e.target.value)
              if (isBacking && !exposeEdited) {
                const port = selectedAdapter?.urlScheme === 'redis' ? '6379' : '5432'
                setExpose(`${e.target.value.trim().toLowerCase() || 'svc'}:${port}`)
              }
            }}
            autoFocus
            disabled={editing}
          />
        </div>
        {!isBacking && <div className="flex flex-col gap-1.5">
          <Label htmlFor="sv-image">Image</Label>
          <Input id="sv-image" value={image} onChange={(e) => setImage(e.target.value)} />
        </div>}
        {isBacking && <div className="flex flex-col gap-1.5">
          <Label htmlFor="sv-role">Note</Label>
          <Input id="sv-role" value={role} onChange={(e) => setRole(e.target.value)} />
        </div>}
        </div></FormSection>
        <FormSection title="Network" description="Choose network memberships and the ports other services can reach."><div className="flex flex-col gap-4 sm:col-span-2">
        {!isBacking &&
          env &&
          env.zones.length > 0 && (
            <div className="flex flex-col gap-1.5">
              <Label>Zones</Label>
              <div className="flex flex-wrap gap-2">
                {env.zones.map((z) => (
                  <label key={z.name} className="flex items-center gap-1.5 text-xs">
                    <Checkbox

                      checked={zones.includes(z.name)}
                      onChange={(e) =>
                        setZones((prev) => (e.target.checked ? [...prev, z.name] : prev.filter((x) => x !== z.name)))
                      }
                      className="accent-primary"
                    />
                    <span>
                      {z.name}
                      {z.internal ? ' (internal)' : ''}
                    </span>
                  </label>
                ))}
              </div>
            </div>
          )}
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="sv-expose">Expose ports (comma list)</Label>
          <Input
            id="sv-expose"
            value={expose}
            onChange={(event) => {
              setExpose(event.target.value)
              setExposeEdited(true)
            }}
          />
        </div>
        </div></FormSection>
        <FormSection title="Runtime and resources" description="Set deployment behavior, replicas and resource limits."><div className="flex flex-col gap-4 sm:col-span-2">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="sv-strategy">Strategy (default — chosen per deployment)</Label>
          <Select
            id="sv-strategy"
            value={strategy}
            onValueChange={(v) => setStrategy(v as Service['strategy'])}
            options={[
              { value: 'blue-green', label: 'blue-green' },
              { value: 'recreate', label: 'recreate' },
              { value: 'rolling', label: 'rolling (deferred)' },
            ]}
          />
        </div>
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="sv-on-failure">Failure policy (default)</Label>
          <Select id="sv-on-failure" value={onFailure} onValueChange={(value) => setOnFailure(value as 'switch_back' | 'leave_active')} options={[
            { value: 'switch_back', label: 'switch_back' },
            { value: 'leave_active', label: 'leave_active' },
          ]} />
        </div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-restart">Restart</Label>
            <Select
              id="sv-restart"
              value={restart}
              onValueChange={(v) => setRestart(v as Service['restart'])}
              options={[
                { value: 'unless-stopped', label: 'unless-stopped' },
                { value: 'always', label: 'always' },
                { value: 'no', label: 'no' },
              ]}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-replicas">Replicas</Label>
            <Input id="sv-replicas" value={replicas} onChange={(e) => setReplicas(e.target.value)} placeholder="1" />
          </div>
        </div>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-mem">Memory limit</Label>
            <Input id="sv-mem" value={mem} onChange={(e) => setMem(e.target.value)} placeholder="512m" />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-cpu">CPU limit (cores)</Label>
            <Input id="sv-cpu" value={cpus} onChange={(e) => setCpus(e.target.value)} placeholder="0.5" />
          </div>
        </div>
        </div></FormSection>
        <FormSection title="Health check" description="Choose how Groundplane checks whether the service is ready."><div className="flex flex-col gap-4 sm:col-span-2">
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-healthcheck-kind">Healthcheck</Label>
            <Select
              id="sv-healthcheck-kind"
              value={hcKind}
              onValueChange={(v) => setHcKind(v as 'http' | 'tcp' | 'pgrep' | 'none')}
              options={[
                { value: 'http', label: 'http' },
                { value: 'tcp', label: 'tcp' },
                { value: 'pgrep', label: 'pgrep' },
                { value: 'none', label: 'none' },
              ]}
            />
          </div>
          <div className="sm:col-span-2 flex flex-col gap-1.5">
            <Label htmlFor="sv-healthcheck-target">
              {hcKind === 'http' ? 'Healthcheck path' : hcKind === 'tcp' ? 'Healthcheck host:port' : hcKind === 'pgrep' ? 'Healthcheck cmd' : '—'}
            </Label>
            <Input
              id="sv-healthcheck-target"
              value={hcTarget}
              onChange={(e) => setHcTarget(e.target.value)}
              disabled={hcKind === 'none'}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-healthcheck-interval">Interval</Label>
            <Input id="sv-healthcheck-interval" value={hcInterval} onChange={(e) => setHcInterval(e.target.value)} disabled={hcKind === 'none'} />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-healthcheck-timeout">Timeout</Label>
            <Input id="sv-healthcheck-timeout" value={hcTimeout} onChange={(e) => setHcTimeout(e.target.value)} disabled={hcKind === 'none'} />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="sv-healthcheck-start">Start period</Label>
            <Input id="sv-healthcheck-start" value={hcStart} onChange={(e) => setHcStart(e.target.value)} disabled={hcKind === 'none'} />
          </div>
        </div>
        </div></FormSection>
      </div>
      {initial?.adapter === 'custom' && <BackingHookFields value={hooks} onChange={setHooks} onError={setHookError} />}
      {hookError && <p className="text-sm text-destructive" role="alert">{hookError}</p>}
      {submitError && <p className="text-sm text-destructive" role="alert">{submitError}</p>}
      <DialogFooter>
        <Button variant="outline" onClick={onClose}>
          Cancel
        </Button>
        <Button
          disabled={!name.trim() || (!isBacking && !image.trim()) || (isBacking && !backing.adapterKey) || submitting || !!hookError}
          onClick={() => void save()}
        >
          {submitting ? 'Saving…' : isBacking ? 'Create + deploy' : editing ? 'Save' : 'Create service'}
        </Button>
      </DialogFooter>
    </DrawerContent>
  )
}
