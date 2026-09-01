'use client'

import { useEffect, useMemo, useState } from 'react'
import { Cpu, Pencil, Plus, RotateCcw, Trash2 } from 'lucide-react'
import { useRequiredParams } from '@/lib/router'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { Badge } from '@/components/ui/badge'
import { EmptyState } from '@/components/common/empty-state'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import type { Runner } from '@/lib/types'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'

export default function TenantRunnersPage() {
  const params = useRequiredParams('tenant')
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const tenantId = tenant?.id ?? ''
  const projects = useMemo(
    () => store.tenantProjects.filter((project) => project.tenantId === tenantId),
    [store.tenantProjects, tenantId],
  )
  const projectIds = useMemo(() => projects.map((project) => project.id).sort(), [projects])
  const projectKey = projectIds.join(',')
  const [editing, setEditing] = useState<Runner | null>(null)
  const [slug, setSlug] = useState('')
  const [saving, setSaving] = useState(false)
  const [editError, setEditError] = useState<string | null>(null)
  const [removing, setRemoving] = useState<Runner | null>(null)
  const [creating, setCreating] = useState(false)
  const [createSlug, setCreateSlug] = useState('')
  const [createOwner, setCreateOwner] = useState('tenant')
  const [githubUrl, setGithubUrl] = useState('')
  const [labels, setLabels] = useState('')
  const [registrationToken, setRegistrationToken] = useState('')
  const [createError, setCreateError] = useState<string | null>(null)
  const [submittingCreate, setSubmittingCreate] = useState(false)
  const [retrying, setRetrying] = useState<Runner | null>(null)
  const [retryToken, setRetryToken] = useState('')
  const [retryError, setRetryError] = useState<string | null>(null)
  const [submittingRetry, setSubmittingRetry] = useState(false)
  const [acceptedTaskId, setAcceptedTaskId] = useState<string | null>(null)

  useEffect(() => {
    if (tenantId === '') return
    void store.refreshRunners(tenantId, projectIds)
  }, [projectKey, store.refreshRunners, tenantId])

  if (!tenant) {
    if (store.tenantsLoading) return <EmptyState icon={<Cpu />} title="Loading tenant" />
    return <EmptyState icon={<Cpu />} title="Tenant not found" />
  }

  const projectLabels = new Map(projects.map((project) => [project.id, project.slug]))
  const runners = store.runners.filter((runner) => runner.tenantId === tenant.id)
  const normalizedSlug = slug.trim()
  const validSlug = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(normalizedSlug) && !normalizedSlug.includes('--')
  const slugTaken = runners.some((runner) => runner.id !== editing?.id && runner.slug === normalizedSlug)

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow={`Tenant · ${tenant.name}`}
        title="Runners"
        description={`Authoritative GitHub self-hosted Runner registrations observed by the Controller · ${runners.length} / 5 allocated`}
        icon={<Cpu />}
      />

      <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border border-border bg-card px-4 py-3">
        <div className="text-sm text-muted-foreground">
          Each Runner receives a dedicated rootless Docker daemon, host identity, and isolated /29.
        </div>
        <Button disabled={runners.length >= 5} onClick={() => { setCreating(true); setCreateError(null) }}>
          <Plus className="size-4" /> Add Runner
        </Button>
      </div>

      {acceptedTaskId && (
        <div className="rounded-lg border border-success/30 bg-success/10 px-4 py-3 text-sm">
          Controller Task accepted: <span className="font-mono">{acceptedTaskId}</span>
        </div>
      )}

      {store.runnerError ? (
        <EmptyState icon={<Cpu />} title="Unable to load Runners" description={store.runnerError} />
      ) : store.runnersLoading ? (
        <div className="rounded-lg border border-border bg-card px-4 py-6 text-sm text-muted-foreground">
          Loading Runners from the Controller…
        </div>
      ) : runners.length === 0 ? (
        <EmptyState
          icon={<Cpu />}
          title="No runners"
          description="No Tenant- or Project-scoped Runner records exist for this Tenant."
        />
      ) : (
        <div className="flex flex-col gap-1.5">
          {runners.map((runner) => (
            <div
              key={runner.id}
              className="flex flex-col gap-3 rounded-lg border border-border bg-card px-3 py-2.5 sm:flex-row sm:items-center sm:justify-between"
            >
              <div className="flex min-w-0 items-center gap-3">
                <span className={`size-2 shrink-0 rounded-full ${runner.online ? 'bg-success' : 'bg-muted-foreground'}`} />
                <div className="flex min-w-0 flex-col">
                  <span className="truncate text-sm font-medium">{runner.slug}</span>
                  <span className="truncate font-mono text-xs text-muted-foreground">{runner.id} · {runner.name}</span>
                  <span className="font-mono text-xs text-muted-foreground">
                    {runner.projectId
                      ? `project · ${projectLabels.get(runner.projectId) ?? runner.projectId}`
                      : `tenant · ${tenant.slug}`}
                  </span>
                </div>
              </div>
              <div className="flex flex-wrap items-center gap-2">
				<Badge variant="outline">{runner.lifecycle}</Badge>
                {runner.labels.map((label) => (
                  <Badge key={label} variant="secondary">{label}</Badge>
                ))}
                <span className={`text-xs font-medium ${runner.online ? 'text-success' : 'text-muted-foreground'}`}>
                  {runner.online ? 'online' : 'offline'}
                </span>
				<Button
				  size="sm"
				  variant="ghost"
				  disabled={runner.lifecycle !== 'failed'}
				  onClick={() => { setRetrying(runner); setRetryToken(''); setRetryError(null) }}
				>
				  <RotateCcw className="size-3.5" /> Retry
				</Button>
				<Button
				  size="sm"
				  variant="ghost"
				  disabled={runner.lifecycle === 'provisioning' || runner.lifecycle === 'deleting'}
				  onClick={() => {
					setEditing(runner)
					setSlug(runner.slug)
					setEditError(null)
				  }}
				>
				  <Pencil className="size-3.5" /> Edit slug
				</Button>
				<Button
				  size="sm"
				  variant="ghost"
				  disabled={runner.lifecycle === 'provisioning'}
				  title={runner.lifecycle === 'provisioning' ? 'Wait for Runner provisioning to finish' : 'Remove managed Runner'}
				  onClick={() => setRemoving(runner)}
				>
				  <Trash2 className="size-3.5" /> Remove
				</Button>
              </div>
            </div>
          ))}
        </div>
      )}

      <Dialog open={creating} onOpenChange={(open) => { if (!open && !submittingCreate) { setCreating(false); setRegistrationToken('') } }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Runner</DialogTitle>
            <DialogDescription>
              Register one GitHub organization or repository Runner. The short-lived token is sent once and never stored.
            </DialogDescription>
          </DialogHeader>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault()
              const normalizedLabels = labels.split(',').map((label) => label.trim()).filter(Boolean)
              if (!createSlug.trim() || !githubUrl.trim() || !registrationToken) return
              void (async () => {
                setSubmittingCreate(true)
                setCreateError(null)
                try {
                  const taskId = await store.createRunner({
                    slug: createSlug.trim(),
                    tenantId: createOwner === 'tenant' ? tenant.id : undefined,
                    projectId: createOwner === 'tenant' ? undefined : createOwner,
                    githubUrl: githubUrl.trim(),
                    labels: normalizedLabels,
                    registrationToken,
                  })
                  setAcceptedTaskId(taskId)
                  setRegistrationToken('')
                  setCreating(false)
                  await store.refreshRunners(tenant.id, projectIds)
                } catch (error) {
                  setCreateError(error instanceof Error ? error.message : 'Unable to create Runner')
                } finally {
                  setSubmittingCreate(false)
                }
              })()
            }}
          >
            <div className="space-y-2">
              <Label htmlFor="runner-create-slug">Slug</Label>
              <Input id="runner-create-slug" value={createSlug} onChange={(event) => setCreateSlug(event.target.value)} autoFocus />
            </div>
            <div className="space-y-2">
              <Label htmlFor="runner-create-owner">Owner</Label>
              <select
                id="runner-create-owner"
                className="h-10 w-full rounded-md border border-input bg-background px-3 text-sm"
                value={createOwner}
                onChange={(event) => setCreateOwner(event.target.value)}
              >
                <option value="tenant">Tenant · {tenant.slug}</option>
                {projects.map((project) => <option key={project.id} value={project.id}>Project · {project.slug}</option>)}
              </select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="runner-create-github">GitHub URL</Label>
              <Input id="runner-create-github" value={githubUrl} onChange={(event) => setGithubUrl(event.target.value)} placeholder="https://github.com/example/repository" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="runner-create-labels">Labels</Label>
              <Input id="runner-create-labels" value={labels} onChange={(event) => setLabels(event.target.value)} placeholder="build, deployment" />
              <p className="text-xs text-muted-foreground">Optional, comma-separated. Groundplane adds the standard GitHub labels.</p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="runner-create-token">Registration token</Label>
              <Input id="runner-create-token" type="password" autoComplete="off" value={registrationToken} onChange={(event) => setRegistrationToken(event.target.value)} />
              {createError && <p className="text-sm text-destructive">{createError}</p>}
            </div>
            <DialogFooter>
              <Button type="button" variant="outline" disabled={submittingCreate} onClick={() => { setCreating(false); setRegistrationToken('') }}>Cancel</Button>
              <Button type="submit" disabled={submittingCreate || !createSlug.trim() || !githubUrl.trim() || !registrationToken}>
                {submittingCreate ? 'Dispatching...' : 'Create Runner'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={retrying !== null} onOpenChange={(open) => { if (!open && !submittingRetry) { setRetrying(null); setRetryToken('') } }}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Retry Runner creation</DialogTitle>
            <DialogDescription>
              Reuse the Runner id, quota, host slot, and subnet with a fresh short-lived GitHub registration token.
            </DialogDescription>
          </DialogHeader>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault()
              if (!retrying || !retryToken) return
              void (async () => {
                setSubmittingRetry(true)
                setRetryError(null)
                try {
                  const taskId = await store.retryRunner(retrying.id, retryToken)
                  setAcceptedTaskId(taskId)
                  setRetryToken('')
                  setRetrying(null)
                  await store.refreshRunners(tenant.id, projectIds)
                } catch (error) {
                  setRetryError(error instanceof Error ? error.message : 'Unable to retry Runner')
                } finally {
                  setSubmittingRetry(false)
                }
              })()
            }}
          >
            <div className="space-y-2">
              <Label htmlFor="runner-retry-token">Fresh registration token</Label>
              <Input id="runner-retry-token" type="password" autoComplete="off" value={retryToken} onChange={(event) => setRetryToken(event.target.value)} autoFocus />
              {retryError && <p className="text-sm text-destructive">{retryError}</p>}
            </div>
            <DialogFooter>
              <Button type="button" variant="outline" disabled={submittingRetry} onClick={() => { setRetrying(null); setRetryToken('') }}>Cancel</Button>
              <Button type="submit" disabled={submittingRetry || !retryToken}>
                {submittingRetry ? 'Dispatching...' : 'Retry Runner'}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

	  <Dialog open={editing !== null} onOpenChange={(open) => { if (!open && !saving) setEditing(null) }}>
		<DialogContent>
		  <DialogHeader>
			<DialogTitle>Edit Runner slug</DialogTitle>
			<DialogDescription>
			  Changes only the Groundplane label. GitHub name, registration, owner, and runtime stay unchanged.
			</DialogDescription>
		  </DialogHeader>
		  <form
			className="space-y-4"
			onSubmit={(event) => {
			  event.preventDefault()
			  if (!editing || !validSlug || slugTaken || normalizedSlug === editing.slug) return
			  void (async () => {
				setSaving(true)
				setEditError(null)
				try {
				  await store.renameRunner(editing.id, normalizedSlug)
				  setEditing(null)
				} catch (error) {
				  setEditError(error instanceof Error ? error.message : 'Unable to edit Runner slug')
				} finally {
				  setSaving(false)
				}
			  })()
			}}
		  >
			<div className="space-y-2">
			  <Label htmlFor="runner-slug">Slug</Label>
			  <Input id="runner-slug" value={slug} onChange={(event) => setSlug(event.target.value)} autoFocus />
			  {slugTaken && <p className="text-sm text-destructive">This Tenant already has a Runner with that slug.</p>}
			  {editError && <p className="text-sm text-destructive">{editError}</p>}
			</div>
			<DialogFooter>
			  <Button type="button" variant="outline" disabled={saving} onClick={() => setEditing(null)}>Cancel</Button>
			  <Button type="submit" disabled={saving || !validSlug || slugTaken || normalizedSlug === editing?.slug}>
				{saving ? 'Saving...' : 'Save slug'}
			  </Button>
			</DialogFooter>
		  </form>
		</DialogContent>
	  </Dialog>
	  <TaskRunnerDialog
		open={removing !== null}
		onOpenChange={(open) => { if (!open) setRemoving(null) }}
		title={`Remove Runner · ${removing?.slug ?? ''}`}
		description="Stop and remove the local Runner runtime, then release its Tenant quota, dedicated subnet, host identity, and record. GitHub deregistration remains manual."
		type="remove"
		target={removing?.id ?? tenant.id}
		workspace={params.tenant}
		destructive
		confirmText={removing?.slug ?? ''}
		startLabel="Remove Runner"
		executionCopy="The Controller will remove this managed Runner:"
		steps={[
		  { label: 'Stop and remove the local Runner runtime', state: 'pending' },
		  { label: 'Release Runner record and reserved allocations', state: 'pending' },
		]}
		onDispatch={() => removing ? store.removeRunner(removing.id) : Promise.reject(new Error('No Runner selected'))}
		onCommit={() => {
			setRemoving(null)
			void store.refreshRunners(tenant.id, projectIds).catch(() => undefined)
		}}
	  />
    </div>
  )
}
