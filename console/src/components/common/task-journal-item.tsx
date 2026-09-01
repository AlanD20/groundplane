'use client'

import { useState } from 'react'
import { ChevronRight, Search } from 'lucide-react'
import { Link } from 'react-router-dom'
import { useStore } from '@/lib/store'
import { ActivityIcon } from '@/components/common/activity-icon'
import { StatusBadge } from '@/components/common/status-badge'
import { TaskDetailDrawer } from '@/components/common/task-detail-drawer'
import { TaskJournalMetadata } from '@/components/common/task-journal-metadata'
import { resolveTaskOperationSurface } from '@/lib/task-navigation'
import type { ActivityEntry, TaskJournalScope } from '@/lib/types'

// The activity journal IS the task stream: every journal entry is a task
// (taskState + steps), and every task lands in the journal. Clicking an
// entry navigates to the exact surface that task belongs to — the target
// environment's Tasks tab, the backing service page, or the platform-infra
// task section. Targets that resolve to nothing stay non-clickable.
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
        {destination ? (
          <Link
            to={destination.href}
            className="flex min-w-0 flex-1 items-start gap-3 rounded-l-xl p-3.5 transition-colors hover:bg-surface/40"
            title={destination.label}
          >
            {body}
            <ChevronRight className="mt-0.5 size-4 shrink-0 text-muted-foreground/50" />
          </Link>
        ) : (
          <div className="flex min-w-0 flex-1 items-start gap-3 p-3.5">{body}</div>
        )}
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation()
            setOpen(true)
          }}
          className="m-2 ml-0 flex shrink-0 items-center gap-1.5 self-center rounded-lg border border-border bg-surface px-2.5 py-2 text-xs font-medium text-muted-foreground transition-colors hover:border-ring/60 hover:text-foreground"
          aria-label={`Inspect Task ${entry.id}`}
        >
          <Search className="size-3.5" />
          <span className="hidden sm:inline">Inspect</span>
        </button>
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
