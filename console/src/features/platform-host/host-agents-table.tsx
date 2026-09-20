import { useState } from 'react'
import { Link } from 'react-router-dom'
import { Plus } from 'lucide-react'
import { useStore } from '@/lib/store'
import { formatAgentLabels, formatLastReportAt } from '@/lib/agent-read-model'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from '@/components/ui/table'
import { Button } from '@/components/ui/button'
import { StatusDot } from '@/components/common/status-badge'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { PlatformAgentActions } from '@/features/platform-agent/platform-agent-actions'

export function HostAgentsTable() {
  const { platform, host, agentsLoading, agentError, refreshAgents, joinAgent } = useStore()
  const [joinOpen, setJoinOpen] = useState(false)
  return <>
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
                      <Link to={`/platform/host/agents/${a.id}`} className="font-medium text-primary hover:underline">
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

  </>
}

