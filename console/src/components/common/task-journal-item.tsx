'use client'

import { Button } from '@/components/ui/button'

import { useState } from 'react'
import { ArrowUpRight, Search } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useStore } from '@/lib/store'
import { ActivityIcon } from '@/components/common/activity-icon'
import { StatusBadge } from '@/components/common/status-badge'
import { TaskDetailDrawer } from '@/components/common/task-detail-drawer'
import { TaskJournalMetadata } from '@/components/common/task-journal-metadata'
import { resolveTaskOperationSurface } from '@/lib/task-navigation'
import type { ActivityEntry, TaskJournalScope } from '@/lib/types'

// The row always inspects the Task. Resource navigation is a separate link,
// including an explicitly labelled journal fallback when the target is gone.
export function TaskJournalItem({ entry, scope }: { entry: ActivityEntry; scope: TaskJournalScope }) {
  const store = useStore()
  const [open, setOpen] = useState(false)
  const destination = resolveTaskOperationSurface(entry, store)

  const body = (
    <>
      <ActivityIcon type={entry.type} status={entry.status} />
      <div className="flex min-w-0 flex-1 flex-col gap-1">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <span className="text-sm font-medium">{entry.title}</span>
          <span className="truncate font-mono text-xs text-muted-foreground">{entry.target}</span>
          {entry.taskState && (
            <span className="rounded-full border border-border bg-surface px-2 py-0.5 font-mono text-[10px] uppercase tracking-wide text-muted-foreground">
              {entry.taskState}
            </span>
          )}
        </div>
        {entry.note && <p className="text-xs text-muted-foreground">{entry.note}</p>}
        <TaskJournalMetadata entry={entry} />
      </div>
      <div className="flex shrink-0 items-center gap-3">
        <StatusBadge status={entry.status} />
      </div>
    </>
  )

  return (
    <>
      <div className="flex w-full items-stretch rounded-xl border border-border bg-card transition-colors hover:border-ring/60">
        <Button variant="ghost" size="content"
          type="button"
          onClick={() => setOpen(true)}
          className="min-w-0 flex-1 items-start justify-start gap-3 rounded-l-xl rounded-r-none p-3.5 text-left hover:bg-surface/40"
          aria-label={`Inspect Task ${entry.id}`}
          aria-haspopup="dialog"
        >
          {body}
          <Search className="mt-0.5 size-3.5 shrink-0 text-muted-foreground" />
        </Button>
        {destination && (
          <Button variant="outline" size="sm"
            render={<Link to={destination.href} />}
            nativeButton={false}
            className="m-2 ml-0 self-center"
            aria-label={`${destination.label} for Task ${entry.id}`}
            title={destination.label}
          >
            <ArrowUpRight className="size-3.5" />
            <span className="hidden sm:inline">{destination.fallback ? 'Open journal' : 'Open page'}</span>
          </Button>
        )}
      </div>
      {open && (
        <TaskDetailDrawer
          entry={entry}
          scope={scope}
          surface="activity"
          onOpenChange={setOpen}
        />
      )}
    </>
  )
}
