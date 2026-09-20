'use client'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/components/ui/table'

import { useEffect, useState } from 'react'
import {
  Activity,
  Boxes,
  ChevronRight,
  GitBranch,
  Network,
  Plus,
  RefreshCw,
  Server,
  Settings2,
  Workflow,
} from 'lucide-react'
import { Link } from 'react-router-dom'
import { useStore } from '@/lib/store'
import { formatAgentLabels, formatLastReportAt } from '@/lib/agent-read-model'
import { PageHeader } from '@/components/common/page-header'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { StatusBadge, StatusDot } from '@/components/common/status-badge'
import { MetaPill } from '@/components/common/meta-pill'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { PlatformAgentActions } from '@/features/platform-agent/platform-agent-actions'
import { TaskDetailDrawer as AuthoritativeTaskDrawer } from '@/components/common/task-detail-drawer'
import type { ActivityEntry, TaskJournalScope } from '@/lib/types'

const platformTaskScope: TaskJournalScope = { kind: 'workspace', workspace: 'platform' }

const kindIcon: Record<string, React.ReactNode> = {
  agent: <Workflow className="size-4 text-muted-foreground" />,
  coredns: <Network className="size-4 text-muted-foreground" />,
  controller: <GitBranch className="size-4 text-muted-foreground" />,
}

export default function PlatformInfraPage() {
  const store = useStore()
  const {
    platform,
    host,
    platformComponentsLoading,
    platformComponentError,
    refreshPlatformComponents,
    agentsLoading,
    agentError,
    refreshAgents,
    joinAgent,
  } = store
  const [filter, setFilter] = useState('all')
  const [open, setOpen] = useState<ActivityEntry | null>(null)
  const [joinOpen, setJoinOpen] = useState(false)
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
        description="Platform runtime managed by the Controller. The local Agent executes assigned work; it never owns or recreates its own container."
        icon={<Server />}
        meta={
          <MetaPill icon={<Boxes />}>{platform.project}</MetaPill>
        }
      />

      {/* Controller-owned local Agent lifecycle */}
      <Card>
        <CardHeader>
          <CardTitle>Agent lifecycle</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-3 sm:grid-cols-3">
            {[
              ['01', 'Operator requests join, update, or removal'],
              ['02', 'Controller creates, replaces, or stops the local Agent container'],
              ['03', 'Agent pulls tasks and reports execution status'],
            ].map(([number, label]) => (
              <div key={number} className="flex items-start gap-3 rounded-lg border border-border bg-surface px-3 py-3">
                <span className="font-mono text-xs font-semibold text-primary">{number}</span>
                <span className="text-xs text-muted-foreground">{label}</span>
              </div>
            ))}
          </div>
          <p className="mt-3 text-xs text-muted-foreground">
            The Controller is the lifecycle authority. The Agent executes assigned reconciliation work and reports progress; it never replaces or restarts itself.
          </p>
        </CardContent>
      </Card>

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

      {/* Agents — foundation for multi-host */}
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle>Agents</CardTitle>
          <Button
            size="sm"
            disabled={agentsLoading || agentError !== null || platform.agents.length > 0}
            title={
              agentsLoading
                ? 'Loading the local Agent'
                : agentError
                  ? 'Agent state is unavailable'
                  : platform.agents.length > 0
                    ? 'The MVP supports one local Agent'
                    : 'Join the local Agent'
            }
            onClick={() => setJoinOpen(true)}
          >
            <Plus className="size-3.5" /> {platform.agents.length > 0 ? 'Agent joined' : 'Join Agent'}
          </Button>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table className="w-full text-sm">
              <TableHeader>
                <TableRow className="border-b border-border text-left text-xs font-semibold text-muted-foreground">
                  <TableHead className="py-2 pr-4">Host</TableHead>
                  <TableHead className="py-2 pr-4">Version</TableHead>
                  <TableHead className="py-2 pr-4">Labels</TableHead>
                  <TableHead className="py-2 pr-4">In-flight tasks</TableHead>
                  <TableHead className="py-2 pr-4">Last report</TableHead>
                  <TableHead className="py-2 pr-4">Status</TableHead>
                  <TableHead className="py-2 text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {platform.agents.map((a) => (
                  <TableRow key={a.id} className="border-b border-border last:border-0">
                    <TableCell className="py-2 pr-4 font-mono text-xs">
                      <Link to={`/platform/agents/${a.id}`} className="font-medium text-primary hover:underline">
                        {a.host}
                      </Link>
                    </TableCell>
                    <TableCell className="py-2 pr-4 font-mono text-xs">{a.version ?? '—'}</TableCell>
                    <TableCell className="py-2 pr-4 font-mono text-xs">{formatAgentLabels(a.labels).join(', ')}</TableCell>
                    <TableCell className="py-2 pr-4 text-xs">{a.inFlight}</TableCell>
                    <TableCell className="py-2 pr-4 text-xs text-muted-foreground">{formatLastReportAt(a.lastReportAt)}</TableCell>
                    <TableCell className="py-2 pr-4">
                      <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                        <StatusDot status={a.status} /> {a.status}
                      </span>
                    </TableCell>
                    <TableCell className="py-2 text-right">
                      <PlatformAgentActions agent={a} compact allowRemove />
                    </TableCell>
                  </TableRow>
                ))}
                {agentsLoading && (
                  <TableRow>
                    <TableCell colSpan={7} className="py-6 text-center text-xs text-muted-foreground">
                      Loading the local Agent…
                    </TableCell>
                  </TableRow>
                )}
                {!agentsLoading && agentError && (
                  <TableRow>
                    <TableCell colSpan={7} className="py-6 text-center text-xs text-destructive">
                      {agentError}
                    </TableCell>
                  </TableRow>
                )}
                {!agentsLoading && !agentError && platform.agents.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={7} className="py-6 text-center text-xs text-muted-foreground">
                      No local Agent is joined. Join it to let the Controller create and manage the Agent container.
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
          <p className="mt-3 max-w-3xl text-xs text-muted-foreground">
            One local Agent in the MVP. The Controller creates and removes its container; labels remain Controller-owned
            task-dispatch configuration. Environment placement across multiple Agents is outside the MVP.
          </p>
          <div className="mt-2 flex items-center gap-1.5 text-xs text-muted-foreground">
            <RefreshCw className="size-3.5" />
            Docker {host?.docker ?? 'unavailable'} · agent pull interval is Controller-owned config
          </div>
        </CardContent>
      </Card>

      <TaskRunnerDialog
        open={joinOpen}
        onOpenChange={(next) => {
          setJoinOpen(next)
          if (!next) void refreshAgents()
        }}
        title="Join local Agent"
        description="Creates the single local Agent record and starts its Controller-managed container. No credentials are exposed through this action."
        type="create"
        target={host?.hostname ?? 'unavailable'}
        workspace="platform"
        startLabel="Join Agent"
        executionCopy="The Controller will create and start the local Agent:"
        steps={[
          { label: 'Create local Agent record', state: 'pending' },
          { label: 'Start Controller-managed Agent container', state: 'pending' },
          { label: 'Wait for the first healthy report', state: 'pending' },
        ]}
        onDispatch={async () => (await joinAgent()).task_id}
      />

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
