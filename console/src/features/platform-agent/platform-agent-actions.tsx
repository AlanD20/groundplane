"use client";

import { useState } from "react";
import { RefreshCw, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Link } from "react-router-dom";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { useStore } from "@/lib/store";
import type { PlatformAgent } from "@/lib/types";

export function PlatformAgentActions({
  agent,
  compact = false,
  allowRemove = false,
}: {
  agent: PlatformAgent;
  compact?: boolean;
  allowRemove?: boolean;
}) {
  const { refreshAgents, removeAgent } = useStore();
  const [removing, setRemoving] = useState(false);

  return (
    <>
      <div className="inline-flex items-center gap-1">
        <Button
          nativeButton={false}
          render={<Link to="/platform/settings?tab=updates" />}
          variant={compact ? "ghost" : "outline"}
          size={compact ? "icon-xs" : "sm"}
          aria-label="Software updates"
          title="Software updates"
        >
          <RefreshCw className="size-3.5" />
          {!compact ? "Software updates" : null}
        </Button>
        {allowRemove ? (
          <Button
            variant={compact ? "ghost" : "outline"}
            size={compact ? "icon-xs" : "sm"}
            className={
              compact
                ? "text-muted-foreground hover:text-destructive"
                : "text-destructive"
            }
            title={`Remove Agent on ${agent.host}`}
            onClick={() => setRemoving(true)}
          >
            <Trash2 className="size-3.5" />
            {!compact ? "Remove Agent" : null}
          </Button>
        ) : null}
      </div>

      <TaskRunnerDialog
        open={removing}
        onOpenChange={(open) => {
          setRemoving(open);
          if (!open) void refreshAgents();
        }}
        title={`Remove Agent · ${agent.host}`}
        description="Stops the Controller-managed Agent container and removes only this local Agent record after the task completes."
        type="destroy"
        target={agent.id}
        workspace="platform"
        destructive
        confirmText={agent.host}
        startLabel="Remove Agent"
        onDispatch={async () => (await removeAgent(agent.id)).task_id}
      />
    </>
  );
}
