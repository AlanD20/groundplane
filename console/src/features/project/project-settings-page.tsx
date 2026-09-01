'use client'

import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Boxes, Fingerprint, Pencil, Save, Settings, Trash2 } from 'lucide-react'
import { EmptyState } from '@/components/common/empty-state'
import { MetaPill } from '@/components/common/meta-pill'
import { PageHeader } from '@/components/common/page-header'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { useRequiredParams } from '@/lib/router'
import { useStore } from '@/lib/store'
import type { TaskStep } from '@/lib/types'

function normalizeSlug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '')
}

export default function ProjectSettingsPage() {
  const params = useRequiredParams('tenant', 'project')
  const navigate = useNavigate()
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const project = store.getProject(params.tenant, params.project)
  const [name, setName] = useState(project?.name ?? '')
  const [slug, setSlug] = useState(project?.slug ?? '')
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [mutationError, setMutationError] = useState<string | null>(null)
  const [mutating, setMutating] = useState(false)

  if (!tenant || !project) {
    if (store.tenantsLoading || store.projectsLoading) return <EmptyState icon={<Boxes />} title="Loading project" />
    return <EmptyState icon={<Boxes />} title="Project not found" description={store.projectError ?? undefined} />
  }

  const environments = project.environments ?? []
  const environmentIds = new Set(environments.map((environment) => environment.id))
  const secretCount = store.reusableSecrets.filter(
    (secret) => secret.scope === 'project' && secret.projectId === project.id,
  ).length
  const connectorCount = store.connectors.filter((connector) => environmentIds.has(connector.scopeRef)).length
  const attachCount = environments.reduce((count, environment) => count + environment.attaches.length, 0)
  const normalizedSlug = normalizeSlug(slug)
  const slugTaken = store.tenantProjects.some(
    (candidate) =>
      candidate.id !== project.id && candidate.tenantId === tenant.id && candidate.slug === normalizedSlug,
  )
  const deleteSteps: TaskStep[] = [
    { label: `Detach ${attachCount} backing attach${attachCount === 1 ? '' : 'es'}`, state: 'pending' },
    { label: `Destroy ${environments.length} environment${environments.length === 1 ? '' : 's'} and their volumes`, state: 'pending' },
    { label: `Delete ${secretCount} secret${secretCount === 1 ? '' : 's'} and ${connectorCount} connector${connectorCount === 1 ? '' : 's'}`, state: 'pending' },
    { label: `Remove project ${project.id} from desired state`, state: 'pending' },
  ]

  return (
	<div className={`flex flex-col gap-6 ${project.deletionTaskId ? 'pointer-events-none opacity-60' : ''}`}>
      <PageHeader
        eyebrow={`Project · ${tenant.slug}/${project.slug}`}
        title="Project settings"
        description="Edit the display name, rename the URL label, or delete this project and everything it owns. Stable references always use the project id."
        icon={<Settings />}
        meta={
          <>
            <MetaPill icon={<Fingerprint />}>{project.id}</MetaPill>
            <MetaPill icon={<Boxes />}>{environments.length} environments</MetaPill>
          </>
        }
      />

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Pencil className="size-4 text-muted-foreground" /> Display name
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="project-name">Name</Label>
            <Input id="project-name" value={name} onChange={(event) => setName(event.target.value)} />
            <p className="text-xs text-muted-foreground">The human-readable project name. Changing it does not change URLs or references.</p>
          </div>
          <div className="flex justify-end">
            <Button
              size="sm"
              disabled={mutating || !name.trim() || name.trim() === project.name}
              onClick={() => void (async () => {
                const nextName = name.trim()
                setMutating(true)
                setMutationError(null)
                try {
                  const updated = await store.editProject(project.id, nextName)
                } catch (error) {
                  setMutationError(error instanceof Error ? error.message : 'Unable to update project')
                } finally {
                  setMutating(false)
                }
              })()}
            >
              <Save className="size-4" /> Save name
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Fingerprint className="size-4 text-muted-foreground" /> Identity and URL
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="project-id">Project id</Label>
            <Input id="project-id" value={project.id} readOnly className="font-mono text-muted-foreground" />
            <p className="text-xs text-muted-foreground">This stable id owns environments and scoped records. It never changes.</p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="project-slug">Slug (URL label)</Label>
            <Input
              id="project-slug"
              value={slug}
              onChange={(event) => setSlug(normalizeSlug(event.target.value))}
              className="font-mono"
              aria-invalid={slugTaken || undefined}
            />
            {slugTaken ? (
              <p className="text-xs text-destructive">That slug is already used by another project in {tenant.name}.</p>
            ) : (
              <p className="text-xs text-muted-foreground">Renaming changes the canonical URL only. Existing stable-id references remain intact.</p>
            )}
          </div>
          <div className="flex justify-end">
            <Button
              size="sm"
              disabled={mutating || !normalizedSlug || normalizedSlug === project.slug || slugTaken}
              onClick={() => void (async () => {
                const previousSlug = project.slug
                setMutating(true)
                setMutationError(null)
                try {
                  const renamed = await store.renameProject(project.id, normalizedSlug)
                  navigate(`/t/${tenant.slug}/${renamed.slug}/settings`, { replace: true })
                } catch (error) {
                  setMutationError(error instanceof Error ? error.message : 'Unable to rename project')
                } finally {
                  setMutating(false)
                }
              })()}
            >
              <Pencil className="size-4" /> Rename slug
            </Button>
          </div>
          {mutationError && <p role="alert" className="text-xs text-destructive">{mutationError}</p>}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-destructive">
            <Trash2 className="size-4" /> Danger zone
          </CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <p className="max-w-3xl text-xs text-muted-foreground">
            Deleting this project removes {environments.length} environment{environments.length === 1 ? '' : 's'},
            their volumes and attaches, {secretCount} scoped secret{secretCount === 1 ? '' : 's'}, and {connectorCount}
            connector{connectorCount === 1 ? '' : 's'}. Durable activity history is retained. This cannot be undone.
          </p>
          <div>
			<Button variant="destructive" size="sm" disabled={project.deletionTaskId !== null} onClick={() => setDeleteOpen(true)}>
              <Trash2 className="size-4" /> Delete project
            </Button>
          </div>
        </CardContent>
      </Card>

      <TaskRunnerDialog
        open={deleteOpen}
        onOpenChange={setDeleteOpen}
        title={`Delete project ${project.name}`}
        description={`This permanently removes ${project.slug} and everything it owns. Type the project slug to confirm.`}
		type="remove"
        target={project.id}
        workspace={tenant.slug}
        destructive
        confirmText={project.slug}
        startLabel="Delete project"
        steps={deleteSteps}
		onDispatch={() => store.deleteProject(project.id)}
		onCommit={() => window.location.assign(`/t/${tenant.slug}`)}
      />
    </div>
  )
}
