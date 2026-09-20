'use client'

import { useEffect, useRef, useState } from 'react'
import { useRequiredParams } from '@/lib/router'
import { useStore } from '@/lib/store'
import type { Environment, Service } from '@/lib/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { ServiceFormBody } from '@/components/common/service-form-body'
import { RemoveDesiredServiceButton, ServiceObservationDetails, ServiceOperationDialog, ServiceRuntimeActions, ServiceStateBadges, type ServiceOperation } from '@/features/service/service-runtime-actions'
import { useVisibleServiceObservations } from '@/features/service/use-service-observation-refresh'

export function ServiceDetailsDrawer({
  env,
  service: summary,
  now: listNow = Date.now(),
  open,
  onOpenChange,
}: {
  env: Environment
  service: Service
  now?: number
  open: boolean
  onOpenChange: (v: boolean) => void
}) {
  const params = useRequiredParams('tenant')
  const headingRef = useRef<HTMLHeadingElement>(null)
  const store = useStore()
  const [service, setService] = useState(summary)
  const detailClock = useVisibleServiceObservations({
    environmentIds: [],
    observations: open ? [service.observation] : [],
    refreshEnvironment: store.refreshEnvironmentServices,
  })
  const now = Math.max(listNow, detailClock.now)
  const [detailError, setDetailError] = useState<string>()
  const [editing, setEditing] = useState(false)
  const [operation, setOperation] = useState<ServiceOperation | null>(null)
  useEffect(() => {
    if (!open) {
      setEditing(false)
      setOperation(null)
      return
    }
    let current = true
    setService(summary)
    setDetailError(undefined)
    void store.getService(summary.id).then((detail) => {
      if (current) setService(detail)
    }).catch((error: unknown) => {
      if (current) setDetailError(error instanceof Error ? error.message : 'Unable to load Service details')
    })
    return () => { current = false }
  }, [open, summary, store.getService])

  return (
    <>
      <Drawer open={open && !editing && !operation} onOpenChange={(v) => onOpenChange(v)}>
        <DrawerContent initialFocus={headingRef}>
          <DialogHeader>
            <DialogTitle ref={headingRef} tabIndex={-1} className="flex flex-wrap items-center gap-2 outline-none">
              <span className="font-mono">{service.name}</span>
              <ServiceStateBadges service={service} now={now} />
            </DialogTitle>
          </DialogHeader>
			{detailError ? <p role="alert" className="text-sm text-destructive">{detailError}</p> : null}
          <div className="flex flex-col gap-1.5 text-sm">
            <DetailRow label="Image" value={service.image} mono />
            <DetailRow label="Note" value={service.role} />
            <DetailRow label="Zones" value={service.zones.join(', ') || '—'} mono />
            <DetailRow label="Strategy" value={`${service.strategy}${service.strategy === 'rolling' ? ' (deferred)' : ''}`} />
            <DetailRow label="Env files" value={service.envFiles.join(', ') || '—'} mono />
            <DetailRow
              label="Healthcheck"
              value={
                service.healthcheck
                  ? service.healthcheck.kind === 'http'
                    ? `GET ${service.healthcheck.target} · every ${service.healthcheck.interval} · timeout ${service.healthcheck.timeout} · start ${service.healthcheck.startPeriod} · retries ${service.healthcheck.retries}`
                    : service.healthcheck.kind === 'tcp'
                      ? `TCP ${service.healthcheck.target} · every ${service.healthcheck.interval} · timeout ${service.healthcheck.timeout} · start ${service.healthcheck.startPeriod} · retries ${service.healthcheck.retries}`
                      : `pgrep '${service.healthcheck.target}' · every ${service.healthcheck.interval} · timeout ${service.healthcheck.timeout} · start ${service.healthcheck.startPeriod} · retries ${service.healthcheck.retries}`
                  : 'none'
              }
              mono
            />
            <DetailRow label="Resources" value={`${service.resources.mem} · ${service.resources.cpus} cpu`} mono />
            <DetailRow
              label="Environment"
              value={service.environment.map((e) => `${e.key}=${e.value}`).join(', ') || '—'}
              mono
            />
            <DetailRow
              label="Mounts"
              value={
                service.mounts
                  .map((m) =>
                    m.type === 'volume' ? `${m.volume} → ${m.mount}` : `${m.file} → ${m.mount} :ro`,
                  )
                  .join(', ') || '—'
              }
              mono
            />
            {service.command && <DetailRow label="Command" value={service.command} mono />}
            {service.aliases.length > 0 && <DetailRow label="Aliases" value={service.aliases.join(', ')} mono />}
            {service.dependsOn.length > 0 && (
              <DetailRow label="Depends on" value={service.dependsOn.map((d) => `${d} (service_healthy)`).join(', ')} mono />
            )}
            <DetailRow label="Expose" value={service.expose.join(', ') || '—'} mono />
            <DetailRow label="Restart" value={service.restart} mono />
            <DetailRow label="Desired replicas" value={String(service.replicas)} mono />
            <DetailRow label="Runtime intent" value={service.runtimeIntent} mono />
            <DetailRow
              label="Labels"
              value={`com.groundplane.managed=true · com.groundplane.service-id=${service.id}`}
              mono
            />
          </div>
			<ServiceObservationDetails service={service} now={now} />
			{service.nativeCompose ? (
				<div className="flex flex-col gap-2">
					<p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Native Compose desired state</p>
					<pre className="max-h-72 overflow-auto rounded-lg border border-border bg-muted p-3 font-mono text-xs whitespace-pre-wrap">
						{service.nativeCompose}
					</pre>
				</div>
			) : !detailError ? <p role="status" className="text-sm text-muted-foreground">Loading native Compose desired state...</p> : null}
			<div className="flex flex-col gap-2">
				<p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">Release ledger</p>
				{service.releaseLedger?.length ? (
					<div className="overflow-hidden rounded-lg border border-border">
						{service.releaseLedger.map((release) => (
							<div key={release.id} className="flex items-center justify-between gap-3 border-b border-border px-3 py-2 last:border-b-0">
								<div className="min-w-0">
									<p className="truncate font-mono text-xs">{release.tag}</p>
									<p className="truncate font-mono text-[10px] text-muted-foreground">{release.digest || release.id}</p>
								</div>
								<Badge variant={release.status === 'active' ? 'primary' : 'outline'}>{release.status}</Badge>
							</div>
						))}
					</div>
				) : service.releaseLedger ? (
					<p className="text-sm text-muted-foreground">No releases yet.</p>
				) : !detailError ? (
					<p role="status" className="text-sm text-muted-foreground">Loading release ledger...</p>
				) : null}
			</div>
          <ServiceRuntimeActions service={service} onAction={setOperation} />
          <DialogFooter>
            <RemoveDesiredServiceButton onClick={() => setOperation('remove')} />
            <Button variant="outline" onClick={() => onOpenChange(false)}>
              Close
            </Button>
            <Button onClick={() => setEditing(true)}>Edit</Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <Drawer open={open && editing} onOpenChange={(v) => onOpenChange(v)}>
        <ServiceFormBody env={env} workspace={params.tenant} initial={service} onClose={() => onOpenChange(false)} />
      </Drawer>
      <ServiceOperationDialog
        env={env}
        service={service}
        operation={operation}
        workspace={params.tenant}
        onOpenChange={(next) => { if (!next) setOperation(null) }}
        onRemoved={() => { setOperation(null); onOpenChange(false) }}
      />
    </>
  )
}

export function DetailRow({ label, value, mono }: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border py-1.5 text-sm last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span className={mono ? 'max-w-[65%] break-all text-right font-mono text-xs' : 'max-w-[65%] text-right text-xs'}>{value}</span>
    </div>
  )
}
