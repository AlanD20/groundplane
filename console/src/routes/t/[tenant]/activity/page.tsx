'use client'

import { useEffect, useMemo, useState } from 'react'
import { useRequiredParams } from '@/lib/router'
import { Activity, RefreshCw } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { TaskJournalItem } from '@/components/common/task-journal-item'
import { MetaPill } from '@/components/common/meta-pill'
import { EmptyState } from '@/components/common/empty-state'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import type { TaskJournalScope } from '@/lib/types'

export default function TenantActivityPage() {
  const params = useRequiredParams('tenant')
  const store = useStore()
  const tenant = store.getTenant(params.tenant)
  const [filter, setFilter] = useState('all')
  const projects = useMemo(
    () => tenant ? store.tenantProjects.filter((project) => project.tenantId === tenant.id) : [],
    [store.tenantProjects, tenant?.id],
  )
  const options = useMemo(() => {
    const result = [{ value: 'all', label: 'All tenant activity' }]
    for (const project of projects) {
      result.push({ value: `project:${project.id}`, label: `Project / ${project.slug}` })
      for (const environment of project.environments ?? []) {
        result.push({
          value: `environment:${environment.id}`,
          label: `Environment / ${project.slug} / ${environment.name}`,
        })
      }
    }
    return result
  }, [projects])
  const effectiveFilter = options.some((option) => option.value === filter) ? filter : 'all'
  const scope = useMemo<TaskJournalScope | null>(() => {
    if (!tenant) return null
    const separator = effectiveFilter.indexOf(':')
    if (separator < 0) return { kind: 'workspace', workspace: tenant.id }
    const kind = effectiveFilter.slice(0, separator)
    const id = effectiveFilter.slice(separator + 1)
    if (kind === 'project') return { kind: 'project', projectId: id }
    if (kind === 'environment') return { kind: 'environment', environmentId: id }
    return { kind: 'workspace', workspace: tenant.id }
  }, [effectiveFilter, tenant?.id])
  const journal = scope ? store.getTaskJournal(scope) : null

  useEffect(() => {
    if (scope) void store.loadTaskJournal('activity', scope).catch(() => undefined)
  }, [scope, store.loadTaskJournal])

  if (store.tenantsLoading) {
    return <div role="status" className="py-10 text-center text-sm text-muted-foreground">loading tenant…</div>
  }
  if (store.tenantError) {
    return <div role="alert" className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">{store.tenantError}</div>
  }
  if (!tenant) return <EmptyState icon={<Activity />} title="Tenant not found" />
  if (!journal || !scope) return null

  const entries = journal.entries

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Activity"
        description="Task history for this Tenant and its descendants. Project and Environment filters only narrow this journal."
        icon={<Activity />}
        meta={
          <div className="flex items-center gap-2">
            <MetaPill icon={<Activity />}>{entries.length} tasks</MetaPill>
            <Select value={effectiveFilter} onValueChange={setFilter} options={options} className="min-w-64" />
            <Button variant="outline" size="sm" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('activity', scope).catch(() => undefined)}>
              <RefreshCw className="size-4" /> Refresh
            </Button>
          </div>
        }
      />
      <div className="flex flex-col gap-2.5">
        {journal.loading && <div role="status" className="py-10 text-center text-sm text-muted-foreground">loading tenant tasks…</div>}
        {journal.loadError && (
          <div role="alert" className="flex items-center justify-between gap-3 rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
            <span>{journal.loadError}</span>
            <Button variant="outline" size="sm" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('activity', scope, journal.failedCursor ?? undefined).catch(() => undefined)}>Retry</Button>
          </div>
        )}
        {entries.map((a) => (
          <TaskJournalItem key={a.id} entry={a} scope={scope} />
        ))}
        {journal.loaded && !journal.loadError && entries.length === 0 && <div className="py-10 text-center text-sm text-muted-foreground">no activity yet</div>}
        {journal.nextCursor && !journal.loadError && (
          <Button variant="outline" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('activity', scope, journal.nextCursor!).catch(() => undefined)}>
            {journal.loadingMore ? 'Loading…' : 'Load more'}
          </Button>
        )}
      </div>
    </div>
  )
}
