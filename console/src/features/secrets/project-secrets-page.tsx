'use client'

import { useState } from 'react'
import { Link } from 'react-router-dom'
import { useRequiredParams } from '@/lib/router'
import { ArrowLeft, KeyRound, Plus } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { MetaPill } from '@/components/common/meta-pill'
import { EmptyState } from '@/components/common/empty-state'
import { ReusableSecretOwnerList } from './reusable-secret-owner-list'
export default function ProjectSecretsPage() {
  const params = useRequiredParams('tenant', 'project')
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const project = store.getProject(params.tenant, params.project)
  const [open, setOpen] = useState(false)
  const [kind, setKind] = useState<'env' | 'file'>('env')
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [path, setPath] = useState('')
  const [creating, setCreating] = useState(false)
  const [mutationError, setMutationError] = useState<string | null>(null)

  function clearDraft() {
    setKey('')
    setValue('')
    setPath('')
  }
  if (!tenant || !project || project.tenantId !== tenant.id) {
    return (
      <EmptyState
        icon={<KeyRound />}
        title="Project not found"
        description={`${params.tenant}/${params.project} does not exist.`}
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

  const projectId = project.id
  const scoped = store.reusableSecrets.filter((secret) => secret.scope === 'project' && secret.projectId === projectId)
  const inherited = store.reusableSecrets.filter((secret) => secret.scope === 'platform')
  const error = mutationError ?? store.secretError
  async function create() {
    setCreating(true)
    setMutationError(null)
    try {
      await store.createReusableSecret({
        projectId,
        key: key.trim(),
        kind,
        path: kind === 'file' ? path.trim() : undefined,
        value,
      })
      setOpen(false)
      setKey('')
      setValue('')
      setPath('')
    } catch (cause) {
      setValue('')
      setMutationError(cause instanceof Error ? cause.message : 'Unable to create Secret')
    } finally {
      setCreating(false)
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Secrets"
        description="Reusable encrypted material scoped to this project. Resolution is project first, then platform fallback."
        icon={<KeyRound />}
        meta={<MetaPill icon={<KeyRound />}>{scoped.length} project-scoped entries</MetaPill>}
        actions={
          <Button onClick={() => setOpen(true)}>
            <Plus className="size-4" /> New secret
          </Button>
        }
      />

      {error && <p className="text-sm text-destructive">{error}</p>}

      <Card>
        <CardContent className="flex flex-col">
          <div className="flex items-center justify-between py-3">
            <h2 className="text-sm font-semibold">Secrets in scope</h2>
            <span className="text-xs text-muted-foreground">defined here or inherited from the platform scope</span>
          </div>
          <ReusableSecretOwnerList
            secrets={scoped}
            loading={store.reusableSecretsLoading}
            emptyTitle="No project Secrets"
            emptyDescription="Resolution falls back to matching platform Secrets."
            scopeLabel="defined here"
            onRemove={(secret) => store.removeReusableSecret(secret.id)}
            onRefresh={() => store.refreshReusableSecrets()}
            onReveal={(secret) => store.revealReusableSecret(secret.id)}
          />
        </CardContent>
      </Card>

      <Card>
        <CardContent className="flex flex-col">
          <div className="flex items-center justify-between py-3">
            <h2 className="text-sm font-semibold">Inherited from platform</h2>
            <span className="text-xs text-muted-foreground">used only when this project has no matching key</span>
          </div>
          <ReusableSecretOwnerList
            secrets={inherited}
            loading={store.reusableSecretsLoading}
            emptyTitle="No platform fallback Secrets"
            scopeLabel="platform fallback"
            onReveal={(secret) => store.revealReusableSecret(secret.id)}
          />
        </CardContent>
      </Card>

      <Drawer
        open={open}
        onOpenChange={(next) => {
          setOpen(next)
          if (!next) clearDraft()
        }}
      >
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New secret · {project.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label>Type</Label>
              <div className="flex gap-2">
                {(['env', 'file'] as const).map((candidate) => (
                  <Button key={candidate} variant={kind === candidate ? 'default' : 'outline'} size="sm" onClick={() => setKind(candidate)}>
                    {candidate === 'env' ? 'env variable' : 'file secret'}
                  </Button>
                ))}
              </div>
            </div>
            {kind === 'env' ? (
              <>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="ps-key">Key</Label>
                  <Input id="ps-key" value={key} onChange={(event) => setKey(event.target.value)} placeholder="ACME_ORG_TOKEN" autoFocus />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="ps-value">Value</Label>
                  <Input id="ps-value" value={value} onChange={(event) => setValue(event.target.value)} placeholder="••••" />
                </div>
              </>
            ) : (
              <>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="pf-key">Name</Label>
                  <Input id="pf-key" value={key} onChange={(event) => setKey(event.target.value)} placeholder="acme-ca" autoFocus />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="pf-path">Path relative to each environment volume</Label>
                  <Input id="pf-path" value={path} onChange={(event) => setPath(event.target.value)} placeholder="config/acme/ca.pem" />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="pf-content">Content</Label>
                  <Textarea
                    id="pf-content"
                    value={value}
                    onChange={(event) => setValue(event.target.value)}
                    rows={6}
                    spellCheck={false}
                    className="font-mono text-xs"
                    placeholder={'-----BEGIN CERTIFICATE-----\n…\n-----END CERTIFICATE-----'}
                  />
                </div>
              </>
            )}
            <p className="text-xs text-muted-foreground">
              Stored encrypted in the Controller. Project values override matching platform keys.
            </p>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setOpen(false)
                clearDraft()
              }}
              disabled={creating}
            >
              Cancel
            </Button>
            <Button
              disabled={creating || !key.trim() || (kind === 'file' && !path.trim())}
              onClick={() => void create()}
            >
              {creating ? 'Saving…' : 'Save secret'}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
    </div>
  )
}
