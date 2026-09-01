'use client'

import { useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { ArrowLeft, GitCompareArrows, History, Pencil, Rocket, ShieldCheck, Trash2, Workflow } from 'lucide-react'
import { EmptyState } from '@/components/common/empty-state'
import { MetaPill } from '@/components/common/meta-pill'
import { PageHeader } from '@/components/common/page-header'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useRequiredParams } from '@/lib/router'
import { useStore } from '@/lib/store'
import { ReleaseGroupFormDrawer } from './release-group-form-drawer'
import { DeployReleaseGroupDialog, RollbackReleaseGroupDialog } from './release-group-operation-dialogs'
import { activeMemberTag, releaseGroupPolicyLabel, releaseGroupTag } from './release-group-projections'

export default function ReleaseGroupDetailPage() {
  const params = useRequiredParams('tenant', 'project', 'env', 'id')
  const store = useStore()
  const navigate = useNavigate()
  const environment = store.getEnvironment(params.tenant, params.project, params.env)
  const group = environment?.releaseGroups.find((candidate) => candidate.id === params.id)
  const [editing, setEditing] = useState(false)
  const [deploying, setDeploying] = useState(false)
  const [rollingBack, setRollingBack] = useState(false)
  const [removing, setRemoving] = useState(false)
  const listPath = `/t/${params.tenant}/${params.project}/${params.env}?tab=release-groups`

  if (!environment || !group) {
    return (
      <EmptyState
        icon={<Workflow />}
        title="Release group not found"
        description={`${params.id} does not exist in ${params.tenant}/${params.project}/${params.env}.`}
        action={<Link to={listPath}><Button variant="outline"><ArrowLeft className="size-4" /> Back to environment</Button></Link>}
      />
    )
  }

  const tag = releaseGroupTag(environment, group)
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow={<Link to={listPath} className="inline-flex items-center gap-1 hover:text-foreground"><ArrowLeft className="size-3" /> {environment.name} release groups</Link>}
        title={group.name}
        description={group.id}
        icon={<Workflow />}
        meta={
          <>
            <MetaPill icon={<GitCompareArrows />}>{group.order.length} ordered services</MetaPill>
            <MetaPill icon={<ShieldCheck />}>{releaseGroupPolicyLabel(group.onFailure)}</MetaPill>
            <MetaPill icon={<Rocket />}>{tag === 'mixed member tags' ? tag : `tag ${tag}`}</MetaPill>
          </>
        }
        actions={
          <>
            <Button variant="outline" onClick={() => setEditing(true)}><Pencil className="size-4" /> Edit</Button>
            <Button onClick={() => setDeploying(true)}><Rocket className="size-4" /> Deploy</Button>
          </>
        }
      />
      <div className="grid gap-4 lg:grid-cols-[minmax(0,1.5fr)_minmax(18rem,0.7fr)]">
        <Card>
          <CardHeader><CardTitle>Release order</CardTitle><p className="text-xs text-muted-foreground">The Controller advances only after each member succeeds.</p></CardHeader>
          <CardContent className="flex flex-col gap-2">
            {group.order.map((service, index) => (
              <div key={service} className="flex items-center gap-3 rounded-xl border border-border bg-surface/50 p-3">
                <span className="flex size-7 shrink-0 items-center justify-center rounded-full border border-primary/30 bg-primary/10 font-mono text-xs text-primary">{index + 1}</span>
                <div className="min-w-0 flex-1"><p className="font-mono text-sm font-medium">{service}</p><p className="text-xs text-muted-foreground">Member service record remains independent.</p></div>
                <div className="flex flex-col items-end gap-1">
                  <Badge variant="primary">{activeMemberTag(environment, service) ?? 'no active release'}</Badge>
                  <Badge variant="outline">{environment.services.find((candidate) => candidate.name === service)?.strategy ?? 'missing'}</Badge>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
        <div className="flex flex-col gap-4">
          <Card><CardHeader><CardTitle>Failure behavior</CardTitle></CardHeader><CardContent className="flex flex-col gap-2 text-sm"><Badge variant={group.onFailure === 'switch_back' ? 'success' : 'warning'} className="w-fit">{group.onFailure}</Badge><p className="text-muted-foreground">{group.onFailure === 'switch_back' ? 'Completed members switch back if a later member fails.' : 'Completed members stay active if a later member fails.'}</p></CardContent></Card>
          <Card><CardHeader><CardTitle>Operations</CardTitle></CardHeader><CardContent className="flex flex-col gap-2"><Button variant="outline" onClick={() => setRollingBack(true)}><History className="size-4" /> Roll back group</Button><Button variant="destructive" onClick={() => setRemoving(true)}><Trash2 className="size-4" /> Remove group</Button><p className="text-xs text-muted-foreground">Removing the group never removes its member services.</p></CardContent></Card>
        </div>
      </div>
      <ReleaseGroupFormDrawer env={environment} group={group} open={editing} onOpenChange={setEditing} />
      <DeployReleaseGroupDialog env={environment} group={group} open={deploying} onOpenChange={setDeploying} />
      <RollbackReleaseGroupDialog env={environment} group={group} open={rollingBack} onOpenChange={setRollingBack} />
      <TaskRunnerDialog
        open={removing}
        onOpenChange={setRemoving}
        title={`Remove ${group.name}`}
        description="Remove this release-group desired-state record. Member services and their runtime state are preserved."
        type="destroy"
        target={group.id}
        workspace={params.tenant}
        destructive
		confirmText={group.name}
		startLabel="Remove group"
		steps={[{ label: 'Validate release group is not locked by another task', state: 'pending' }, { label: 'Remove release group desired state', state: 'pending' }, { label: 'Preserve member service records', state: 'pending' }]}
		onDispatch={() => store.removeReleaseGroup(environment.id, group.id)}
		onCommit={() => {
			void store.refreshEnvironmentReleases(environment.id)
				.catch(() => undefined)
				.finally(() => navigate(listPath, { replace: true }))
		}}
      />
    </div>
  )
}
