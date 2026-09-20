'use client'

import { useEffect, useMemo, useState } from 'react'
import { Activity, RefreshCw } from 'lucide-react'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { TaskJournalItem } from '@/components/common/task-journal-item'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import type { TaskJournalScope } from '@/lib/types'

function activityScope(filter: string): TaskJournalScope {
  const separator = filter.indexOf(':')
  if (separator < 0) return { kind: 'all' }
  const kind = filter.slice(0, separator)
  const id = filter.slice(separator + 1)
  if (kind === 'tenant') return { kind: 'workspace', workspace: id }
  if (kind === 'project') return { kind: 'project', projectId: id }
  if (kind === 'environment') return { kind: 'environment', environmentId: id }
  return { kind: 'all' }
}

export default function PlatformActivityPage() {
  const store = useStore()
  const [filter, setFilter] = useState('all')
  const [status, setStatus] = useState('all')
  const scope = useMemo(() => activityScope(filter), [filter])
  const journal = store.getTaskJournal(scope)
  const entries = journal.entries.filter(entry => status === 'all' || entry.status === status)
  const options = useMemo(() => {
    const result = [{ value: 'all', label: 'All activity' }]
    for (const tenant of store.tenants) {
      result.push({ value: `tenant:${tenant.id}`, label: `Tenant / ${tenant.slug}` })
    }
    for (const project of [...store.tenantProjects, ...store.backingProjects]) {
      const tenant = store.tenants.find((candidate) => candidate.id === project.tenantId)
      const owner = tenant?.slug ?? 'backing'
      result.push({ value: `project:${project.id}`, label: `Project / ${owner} / ${project.slug}` })
      for (const environment of project.environments ?? []) {
        result.push({
          value: `environment:${environment.id}`,
          label: `Environment / ${owner} / ${project.slug} / ${environment.name}`,
        })
      }
    }
    return result
  }, [store.backingProjects, store.tenantProjects, store.tenants])

  useEffect(() => {
    void store.loadTaskJournal('activity', scope).catch(() => undefined)
  }, [scope, store.loadTaskJournal])
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Activity"
        description="All platform and tenant task history. Tenant, Project, and Environment filters only narrow this journal."
        icon={<Activity />}
      />
      <div className="flex flex-wrap items-end gap-3 rounded-xl border border-border bg-card p-4" aria-label="Activity filters">
            <label className="flex min-w-0 flex-1 flex-col gap-1.5 text-xs text-muted-foreground">
              Owner
              <Select aria-label="Activity owner" value={filter} onValueChange={setFilter} options={options} />
            </label>
            <label className="flex flex-col gap-1.5 text-xs text-muted-foreground">
              Status of loaded tasks
              <Select aria-label="Status of loaded tasks" value={status} onValueChange={setStatus} className="min-w-48"
                options={['all', 'pending', 'running', 'completed', 'failed', 'timed_out', 'aborted'].map(value => ({value, label: value === 'all' ? 'All statuses' : value.replaceAll('_', ' ')}))} />
            </label>
            <Button variant="outline" size="sm" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('activity', scope).catch(() => undefined)}>
              <RefreshCw className="size-4" /> Refresh
            </Button>
      </div>
      <div className="flex flex-col gap-2.5">
        {journal.loaded && <p className="text-xs text-muted-foreground" role="status">{entries.length} matching · {journal.entries.length} loaded{journal.nextCursor ? ' · more available' : ''}</p>}
        {journal.loading && <div role="status" className="py-10 text-center text-sm text-muted-foreground">loading activity…</div>}
        {journal.loadError && (
          <div role="alert" className="flex items-center justify-between gap-3 rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
            <span>{journal.loadError}</span>
            <Button variant="outline" size="sm" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('activity', scope, journal.failedCursor ?? undefined).catch(() => undefined)}>Retry</Button>
          </div>
        )}
        {entries.map((a) => (
          <TaskJournalItem key={a.id} entry={a} scope={scope} />
        ))}
        {journal.loaded && !journal.loadError && entries.length === 0 && <div className="py-10 text-center text-sm text-muted-foreground">No loaded tasks match these filters.{journal.nextCursor ? ' Load more to search the next page.' : ''}</div>}
        {journal.nextCursor && !journal.loadError && (
          <Button variant="outline" disabled={journal.loading || journal.loadingMore} onClick={() => void store.loadTaskJournal('activity', scope, journal.nextCursor!).catch(() => undefined)}>
            {journal.loadingMore ? 'Loading…' : 'Load more'}
          </Button>
        )}
      </div>
    </div>
  )
}
