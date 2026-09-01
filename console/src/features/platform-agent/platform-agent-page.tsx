'use client'

import { ArrowLeft, Workflow } from 'lucide-react'
import { Link, useParams } from 'react-router-dom'
import { PageHeader } from '@/components/common/page-header'
import { MetaPill } from '@/components/common/meta-pill'
import { StatusBadge } from '@/components/common/status-badge'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { useStore } from '@/lib/store'
import { formatAgentLabels, formatLastReportAt } from '@/lib/agent-read-model'
import { AgentConfigCard } from '@/features/platform-agent/agent-config-card'
import { PlatformAgentActions } from '@/features/platform-agent/platform-agent-actions'

export default function PlatformAgentPage() {
  const { id } = useParams<{ id: string }>()
  const { platform, agentsLoading, agentError } = useStore()
  const agent = platform.agents.find((candidate) => candidate.id === id)

  if (agentsLoading) {
    return <p className="py-10 text-sm text-muted-foreground">Loading Agent…</p>
  }

  if (!agent) {
    return (
      <div className="flex flex-col items-start gap-4 py-10">
        <p className={agentError ? 'text-sm text-destructive' : 'text-sm text-muted-foreground'}>
          {agentError ?? 'Unknown Agent.'}
        </p>
        <Link to="/platform/host" className="inline-flex items-center gap-1.5 text-sm font-medium text-primary hover:underline">
          <ArrowLeft className="size-4" /> Back to Host
        </Link>
      </div>
    )
  }

  return (
    <div className="flex flex-col gap-6">
      <Link to="/platform/host" className="inline-flex w-fit items-center gap-1.5 text-xs font-medium text-muted-foreground transition-colors hover:text-foreground">
        <ArrowLeft className="size-3.5" /> Host
      </Link>

      <PageHeader
        title={agent.host}
        description="Controller-managed local Agent"
        icon={<Workflow />}
        meta={
          <>
            <MetaPill icon={<Workflow />}>{agent.id}</MetaPill>
            <StatusBadge status={agent.status} />
          </>
        }
        actions={
          <PlatformAgentActions agent={agent} allowRemove />
        }
      />

      <div className="grid gap-6 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Workflow className="size-4 text-muted-foreground" /> Runtime
            </CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-1.5 text-sm">
            <Row label="Agent ID" value={agent.id} mono />
            <Row label="Enrollment task" value={agent.enrollmentTaskId} mono />
            <Row label="Host" value={agent.host} mono />
            <Row label="Version" value={agent.version ?? '—'} mono />
            <Row label="Status" value={<StatusBadge status={agent.status} />} />
            <Row label="First ready" value={formatLastReportAt(agent.readyAt)} />
            <Row label="Last report" value={formatLastReportAt(agent.lastReportAt)} />
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Dispatch</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-3 text-sm">
            <Row label="In-flight tasks" value={String(agent.inFlight)} />
            <div className="flex flex-col gap-1.5 border-t border-border pt-3">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground/70">Labels</span>
              <div className="flex flex-wrap gap-1.5">
                {formatAgentLabels(agent.labels).map((label) => (
                  <span key={label} className="rounded-md border border-border bg-surface px-2 py-1 font-mono text-xs text-muted-foreground">
                    {label}
                  </span>
                ))}
                {Object.keys(agent.labels).length === 0 && <span className="text-xs text-muted-foreground">No labels</span>}
              </div>
            </div>
            <p className="border-t border-border pt-3 text-xs text-muted-foreground">
              The Controller owns this Agent record and container lifecycle. The Agent executes assigned work and reports status.
            </p>
          </CardContent>
        </Card>
      </div>

      <AgentConfigCard agentId={agent.id} />
    </div>
  )
}

function Row({ label, value, mono }: { label: string; value: React.ReactNode; mono?: boolean }) {
  return (
    <div className="flex items-center justify-between gap-3 border-b border-border py-1.5 last:border-0">
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className={mono ? 'break-all text-right font-mono text-xs' : 'text-right text-xs'}>{value}</span>
    </div>
  )
}
