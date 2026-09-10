'use client'

import { useState } from 'react'
import { RefreshCw, Trash2 } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { TaskRunnerDialog } from '@/components/common/task-runner-dialog'
import { useStore } from '@/lib/store'
import type { PlatformAgent } from '@/lib/types'

export function PlatformAgentActions({
  agent,
  compact = false,
  allowRemove = false,
}: {
  agent: PlatformAgent
  compact?: boolean
  allowRemove?: boolean
}) {
  const { agentsLoading, refreshAgents, updateAgent, removeAgent } = useStore()
  const [updating, setUpdating] = useState(false)
  const [removing, setRemoving] = useState(false)
  const updateDisabled = agentsLoading || agent.inFlight > 0

  return (
    <>
      <div className="inline-flex items-center gap-1">
        <Button
          variant={compact ? 'ghost' : 'outline'}
          size={compact ? 'icon-xs' : 'sm'}
          disabled={updateDisabled}
          className={compact ? 'text-muted-foreground' : undefined}
          title={agent.inFlight > 0 ? 'Agent update requires zero in-flight tasks' : `Update Agent on ${agent.host}`}
          onClick={() => setUpdating(true)}
        >
          <RefreshCw className="size-3.5" />
          {!compact ? 'Update Agent' : null}
        </Button>
        {allowRemove ? (
          <Button
            variant={compact ? 'ghost' : 'outline'}
            size={compact ? 'icon-xs' : 'sm'}
            className={compact ? 'text-muted-foreground hover:text-destructive' : 'text-destructive'}
            title={`Remove Agent on ${agent.host}`}
            onClick={() => setRemoving(true)}
          >
            <Trash2 className="size-3.5" />
            {!compact ? 'Remove Agent' : null}
          </Button>
        ) : null}
      </div>

      <TaskRunnerDialog
        open={updating}
        onOpenChange={(open) => {
          setUpdating(open)
          if (!open) void refreshAgents()
        }}
        title={`Update Agent · ${agent.host}`}
        description="Replaces this idle Agent with the last qualified Controller release's pinned Agent image, or bootstrap agent.image before any qualified release. The Controller restores the previous digest if the replacement does not become Ready."
        type="update"
        target={agent.id}
        workspace="platform"
        startLabel="Update Agent"
        executionCopy="The Controller will replace the selected local Agent:"
        steps={[
          { label: 'Fence assignments and verify the Agent is idle', state: 'pending' },
          { label: 'Rotate Agent generation and channel token', state: 'pending' },
          { label: 'Replace the container and wait for authenticated Ready', state: 'pending' },
          { label: 'Restore the previous digest if readiness fails', state: 'pending' },
        ]}
        onDispatch={async () => (await updateAgent(agent.id)).task_id}
      />

      <TaskRunnerDialog
        open={removing}
        onOpenChange={(open) => {
          setRemoving(open)
          if (!open) void refreshAgents()
        }}
        title={`Remove Agent · ${agent.host}`}
        description="Stops the Controller-managed Agent container and removes only this local Agent record after the task completes."
        type="destroy"
        target={agent.id}
        workspace="platform"
        destructive
        confirmText={agent.host}
        startLabel="Remove Agent"
        executionCopy="The Controller will stop and remove the local Agent:"
        steps={[
          { label: 'Dispatch local Agent removal', state: 'pending' },
          { label: 'Stop the Controller-managed Agent container', state: 'pending' },
          { label: 'Remove the local Agent record', state: 'pending' },
        ]}
        onDispatch={async () => (await removeAgent(agent.id)).task_id}
      />
    </>
  )
}
