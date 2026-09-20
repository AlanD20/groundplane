import { useState } from 'react'
import { useStore } from '@/lib/store'
import type { Environment } from '@/lib/types'
import { valkeyAuthenticationDetails } from '@/lib/valkey-authentication'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'

export function AttachFormDialog({ env, open, onOpenChange }: { env: Environment; open: boolean; onOpenChange: (v: boolean) => void }) {
  const store = useStore()
  const available = store.backingProjects.filter((g) => {
    const service = g.environments?.[0]?.services[0]
    return service?.runtimeIntent === 'running'
  })
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
  const custom = !!adapter?.custom
  const customProvisioning = custom && !!svc?.hooks?.attach
  const authenticationDetails = valkeyAuthenticationDetails(svc?.authentication)
  const authenticationUnavailable = svc?.adapter === 'valkey:9' && !authenticationDetails
  const needsDatabase = adapter?.requires.database ?? true
  const attachName = name.trim().toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-')
  const alreadyAttached = env.attaches.filter((a) => a.projectId === gid)
  const credentialOwners = alreadyAttached.filter(
    (attach) => attach.backingServiceId === svc?.id && attach.credential.mode === 'new' && attach.status === 'healthy',
  )
  const grantOptions = env.attaches.filter((a) => a.projectId === gid && a.database !== '—')
  const factRows = custom
    ? (svc?.hooks?.facts ?? []).map((fact) => fact.key)
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
            {custom ? (
              <>
                Attaching grants a <span className="font-medium text-foreground">specific service</span> network access to{' '}
                <span className="font-mono">{g?.name}</span>.{' '}
                {customProvisioning ? 'A new credential owner runs the configured attach hook. Facts become available only after successful provisioning.' : 'No provisioning hook is configured; the consumer only joins the backing network.'}
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
            {attachName && !custom && needsDatabase && (
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
                {custom
                  ? customProvisioning ? 'each new owner runs the custom attach hook; an existing owner reuses its facts' : 'each Attach connects one consumer, without provisioning'
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
          {(!custom || (customProvisioning && factRows.length > 0)) && (
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
          {!custom && credentialMode === 'new' && grantOptions.length > 0 && (
            <div className="flex flex-col gap-1.5">
              <Label>Also grant access to (other attaches' databases, same role)</Label>
              <div className="flex flex-wrap gap-2">
                {grantOptions.map((grant) => (
                  <label key={grant.id} className="flex items-center gap-1.5 rounded-lg border border-border bg-surface px-2.5 py-1.5 text-xs">
                    <Checkbox

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
          {custom && !customProvisioning ? (
            <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2 text-xs text-muted-foreground">
              <span>
                <span className="font-medium text-foreground">Network-only attach.</span> No facts or credentials
                are generated. Groundplane manages the backing container and connects this consumer to its network.
              </span>
            </div>
          ) : authenticationUnavailable ? (
            <p role="alert" className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
              The Controller did not return this Valkey backing instance&apos;s authentication mode. Refresh before attaching.
            </p>
          ) : (
            (
              <div className="flex flex-col gap-1.5">
                <Label>Facts available after successful provisioning (not injected automatically)</Label>
                <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
                  {factRows.map((key) => (
                    <div key={key} className="flex items-center justify-between gap-2 font-mono text-[11px]">
                      <span className="text-muted-foreground">{key}</span>
                      <span className="text-foreground">{custom ? 'returned by attach hook' : 'resolved by Controller'}</span>
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
