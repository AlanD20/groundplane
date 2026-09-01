import type { ActivityEntry } from '@/lib/types'

function formatTimestamp(value: string | null | undefined, empty: string): string {
  if (!value) return empty
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: 'medium',
    timeStyle: 'short',
    timeZone: 'UTC',
  }).format(date) + ' UTC'
}

function owner(entry: ActivityEntry): string {
  const parts = [entry.workspaceType === 'platform' ? 'platform' : `tenant:${entry.tenantId ?? entry.workspace}`]
  if (entry.projectId) parts.push(`project:${entry.projectId}`)
  if (entry.environmentId) parts.push(`environment:${entry.environmentId}`)
  return parts.join(' / ')
}

export function TaskJournalMetadata({ entry }: { entry: ActivityEntry }) {
  return (
    <div className="flex flex-col gap-2 text-[11px] text-muted-foreground">
      <div className="flex flex-wrap gap-x-4 gap-y-1">
        <span><span className="font-medium text-foreground/70">Owner</span> <span className="font-mono">{owner(entry)}</span></span>
        <span><span className="font-medium text-foreground/70">Actor</span> <span className="font-mono">{entry.actor}</span></span>
      </div>
      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 sm:grid-cols-4">
        <TaskTime label="Created" value={formatTimestamp(entry.createdAt ?? entry.ts, 'unknown')} />
        <TaskTime label="Updated" value={formatTimestamp(entry.updatedAt ?? entry.ts, 'unknown')} />
        <TaskTime label="Started" value={formatTimestamp(entry.startedAt, 'not started')} />
        <TaskTime label="Finished" value={formatTimestamp(entry.finishedAt, 'not finished')} />
      </dl>
    </div>
  )
}

function TaskTime({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="font-medium text-foreground/70">{label}</dt>
      <dd className="truncate font-mono" title={value}>{value}</dd>
    </div>
  )
}
