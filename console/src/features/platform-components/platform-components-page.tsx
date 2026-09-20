'use client'

import { useEffect, useState } from 'react'
import {
  Activity,
  Boxes,
  ChevronRight,
  Network,
  RefreshCw,
  Server,
  Settings2,
} from 'lucide-react'
import { Link } from 'react-router-dom'
import { useStore } from '@/lib/store'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge, StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { TaskDetailDrawer as AuthoritativeTaskDrawer } from '@/components/common/task-detail-drawer'
import type { ActivityEntry, TaskJournalScope } from '@/lib/types'

const platformTaskScope: TaskJournalScope = { kind: 'workspace', workspace: 'platform' }

const kindIcon: Record<string, React.ReactNode> = {
  coredns: <Network className="size-4 text-muted-foreground" />,
}

export default function PlatformInfraPage() {
  const store = useStore()
  const {
    platform,
    platformComponentsLoading,
    platformComponentError,
    refreshPlatformComponents,
  } = store
  const [filter, setFilter] = useState('all')
  const [open, setOpen] = useState<ActivityEntry | null>(null)
  const platformJournal = store.getTaskJournal(platformTaskScope)
  const platformTasks = platformJournal.entries

  useEffect(() => {
    void store.loadTaskJournal('tasks', platformTaskScope).catch(() => undefined)
  }, [store.loadTaskJournal])

  const filters: { key: string; label: string; match: (t: ActivityEntry) => boolean }[] = [
    { key: 'all', label: 'All', match: () => true },
    { key: 'inflight', label: 'In-flight', match: (t) => t.status === 'running' },
    { key: 'queued', label: 'Queued', match: (t) => t.status === 'pending' },
    { key: 'completed', label: 'Completed', match: (t) => t.status === 'completed' },
    { key: 'failed', label: 'Failed', match: (t) => t.status === 'failed' || t.status === 'timed_out' || t.status === 'aborted' },
  ]
  const visible = platformTasks.filter((t) => filters.find((f) => f.key === filter)?.match(t) ?? true)

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Components"
        description="Platform components and their configuration."
        icon={<Server />}
        meta={
          <MetaPill icon={<Boxes />}>{platform.project}</MetaPill>
        }
      />

      <div className="grid gap-6 lg:grid-cols-2">
        {platformComponentsLoading && (
          <div role="status" className="rounded-xl border border-border bg-card p-6 text-sm text-muted-foreground">
            Loading Platform Components…
          </div>
        )}
        {!platformComponentsLoading && platformComponentError && (
          <div role="alert" className="flex items-center justify-between gap-3 rounded-xl border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
            <span>{platformComponentError}</span>
            <Button variant="outline" size="sm" onClick={() => void refreshPlatformComponents().catch(() => undefined)}>
              Retry
            </Button>
          </div>
        )}
        {!platformComponentsLoading && !platformComponentError && platform.components.map((c) => (
          <Link
            key={c.id}
            to={`/platform/components/${c.kind}`}
            className="rounded-xl border border-border bg-card text-card-foreground shadow-sm transition-colors hover:border-ring/60"
          >
            <CardHeader>
              <CardTitle className="flex items-center justify-between text-sm">
                <span className="flex items-center gap-2">
                  {kindIcon[c.kind]}
                  {c.name}
                </span>
                <StatusBadge status={c.status} />
              </CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-1.5 text-sm">
              <Row label="Image" value={`${c.image}:${c.version}`} mono />
              <Row label="Runtime" value={c.runtime} />
              {c.hostNetwork ? <Row label="Network" value="host network" mono /> : null}
              <div className="mt-1">
                <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Mounts</span>
                {c.mounts.map((m) => (
                  <div key={m} className="font-mono text-xs text-muted-foreground">
                    {m}
                  </div>
                ))}
              </div>
              <div className="mt-1 flex flex-col gap-0.5">
                {c.notes.map((n) => (
                  <span key={n} className="text-xs text-muted-foreground">
                    · {n}
                  </span>
                ))}
              </div>
              <span className="mt-1 inline-flex items-center gap-1 text-xs font-medium text-primary">
                <Settings2 className="size-3.5" /> Settings
              </span>
            </CardContent>
          </Link>
        ))}
      </div>

      {/* Platform component tasks */}
      <Card>
        <CardHeader className="flex-row items-center justify-between gap-3">
          <CardTitle className="flex items-center gap-2">
            <Activity className="size-4 text-muted-foreground" /> Tasks · Platform components
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant={platformTasks.some((t) => t.status === 'running' || t.status === 'pending') ? 'success' : 'muted'}>
              {platformTasks.filter((t) => t.status === 'running' || t.status === 'pending').length > 0
                ? 'in-flight'
                : 'idle'}
            </Badge>
            <Button variant="outline" size="sm" disabled={platformJournal.loading || platformJournal.loadingMore} onClick={() => void store.loadTaskJournal('tasks', platformTaskScope).catch(() => undefined)}>
              <RefreshCw className="size-4" /> Refresh
            </Button>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          <div className="flex items-center justify-between gap-2">
            <p className="text-xs text-muted-foreground">
              The complete Platform workspace journal. It is the same ordered page and cursor used by Platform Activity;
              target, component, and operation never filter membership.
            </p>
            <div className="flex shrink-0 items-center gap-0.5 rounded-lg border border-border bg-surface p-0.5">
              {filters.map((f) => (
                <Button variant="ghost" size="content"
                  key={f.key}
                  type="button"
                  onClick={() => setFilter(f.key)}
                  className={
                    filter === f.key
                      ? 'rounded-md bg-primary px-2 py-1 text-[11px] font-medium text-primary-foreground'
                      : 'rounded-md px-2 py-1 text-[11px] font-medium text-muted-foreground transition-colors hover:text-foreground'
                  }
                >
                  {f.label}
                </Button>
              ))}
            </div>
          </div>
          {platformJournal.loading && (
            <div role="status" className="text-xs text-muted-foreground">loading platform tasks…</div>
          )}
          {platformJournal.loadError && (
            <div role="alert" className="flex items-center justify-between gap-2 text-xs text-destructive">
              <span>{platformJournal.loadError}</span>
              <Button variant="outline" size="sm" disabled={platformJournal.loading || platformJournal.loadingMore} onClick={() => void store.loadTaskJournal('tasks', platformTaskScope, platformJournal.failedCursor ?? undefined).catch(() => undefined)}>Retry</Button>
            </div>
          )}
          {!platformJournal.loading && visible.length === 0 ? (
            <div className="text-xs text-muted-foreground">no tasks match this filter</div>
          ) : (
            <div className="flex flex-col gap-1.5">
              {visible.map((t) => (
                <Button variant="ghost" size="content"
                  key={t.id}
                  type="button"
                  onClick={() => setOpen(t)}
                  className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-left transition-colors hover:border-ring/60 hover:bg-surface/60"
                >
                  <div className="flex min-w-0 items-center gap-2 text-sm">
                    <StatusDot status={t.status} />
                    <span className="truncate font-medium">{t.title}</span>
                  </div>
                  <div className="flex shrink-0 items-center gap-2">
                    {t.note && <span className="hidden max-w-[260px] truncate font-mono text-[11px] text-muted-foreground lg:inline">{t.note}</span>}
                    <span className="shrink-0 font-mono text-[10px] text-muted-foreground">
                      {t.status}
                    </span>
                    <ChevronRight className="size-3.5 text-muted-foreground/50" />
                  </div>
                </Button>
              ))}
            </div>
          )}
          {platformJournal.nextCursor && !platformJournal.loadError && (
            <Button variant="outline" disabled={platformJournal.loading || platformJournal.loadingMore} onClick={() => void store.loadTaskJournal('tasks', platformTaskScope, platformJournal.nextCursor!).catch(() => undefined)}>
              {platformJournal.loadingMore ? 'Loading…' : 'Load more'}
            </Button>
          )}
        </CardContent>
        {open && (
          <AuthoritativeTaskDrawer
            entry={open}
            scope={platformTaskScope}
            surface="tasks"
            onOpenChange={(value) => !value && setOpen(null)}
          />
        )}
      </Card>
    </div>
  )
}

function Row({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-2">
      <span className="text-muted-foreground">{label}</span>
      <span className={mono ? 'font-mono text-xs' : 'text-xs'}>{value}</span>
    </div>
  )
}
