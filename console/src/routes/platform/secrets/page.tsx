'use client'

import { useState } from 'react'
import { KeyRound, Plus } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { Label } from '@/components/ui/label'
import { DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Drawer, DrawerContent } from '@/components/ui/drawer'
import { ReusableSecretOwnerList } from '@/features/secrets/reusable-secret-owner-list'
export default function PlatformSecretsPage() {
  const store = useStore()
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

  const reusableSecrets = store.reusableSecrets.filter((secret) => secret.scope === 'platform')
  const error = mutationError ?? store.secretError
  async function create() {
    setCreating(true)
    setMutationError(null)
    try {
      await store.createReusableSecret({
        platform: true,
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
        title="Secret store"
        description="Reusable encrypted env variables and files owned by the platform. Values are fetched only when explicitly revealed."
        icon={<KeyRound />}
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
            <h2 className="text-sm font-semibold">Platform reusable Secrets</h2>
            <span className="text-xs text-muted-foreground">Controller-managed metadata; plaintext is never retained in the store</span>
          </div>
          <ReusableSecretOwnerList
            secrets={reusableSecrets}
            loading={store.reusableSecretsLoading}
            emptyTitle="No platform Secrets yet"
            scopeLabel="platform scope"
            onRemove={(secret) => store.removeReusableSecret(secret.id)}
            onRefresh={() => store.refreshReusableSecrets()}
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
            <DialogTitle>New platform Secret</DialogTitle>
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
                  <Label htmlFor="s-key">Key</Label>
                  <Input id="s-key" value={key} onChange={(event) => setKey(event.target.value)} placeholder="APP_KEY" autoFocus />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="s-value">Value</Label>
                  <Input id="s-value" value={value} onChange={(event) => setValue(event.target.value)} placeholder="••••" />
                </div>
              </>
            ) : (
              <>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="f-key">Name</Label>
                  <Input id="f-key" value={key} onChange={(event) => setKey(event.target.value)} placeholder="identity-tls-ca" autoFocus />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="f-path">Path relative to each environment volume</Label>
                  <Input id="f-path" value={path} onChange={(event) => setPath(event.target.value)} placeholder="config/identity-tls/ca.pem" />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="f-content">Content</Label>
                  <Textarea
                    id="f-content"
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
              Stored encrypted in the Controller and materialized by the Agent only when referenced.
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
