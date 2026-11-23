import { useState } from "react";
import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { requestTask } from "@/features/task/api";
import { taskFromAPI } from "@/features/task/journal-model";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import type { ActivityEntry, Environment } from "@/lib/types";
import type { DeploymentGroup } from "./deployment-groups";
import { ReleaseHistoryRow } from "./release-history-row";

export function DeploymentGroupRow({
  group,
  env,
}: {
  group: DeploymentGroup;
  env: Environment;
}) {
  const [task, setTask] = useState<ActivityEntry | null>(null);
  const [unavailable, setUnavailable] = useState(false);
  useVisibleRefresh(
    async (signal) => {
      try {
        const response = await requestTask(group.taskId, signal, { limit: 1 });
        if (!signal.aborted) {
          setTask(taskFromAPI(response.task));
          setUnavailable(false);
        }
      } catch {
        if (!signal.aborted) setUnavailable(true);
      }
    },
    { key: group.taskId, enabled: !task?.finishedAt },
  );
  const count = new Set(group.releases.map((release) => release.service)).size;
  const serving = group.releases.filter(
    (release) => release.status === "active",
  ).length;
  const fallback = group.releases.every(
    (release) => release.operationKind === "rollback",
  )
    ? "Rollback"
    : "Deployment";
  return (
    <div className={workspaceSectionClassName(false)}>
      <Disclosure
        summaryClassName="py-1"
        summary={
          <span className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-3">
            <span className="min-w-0 space-y-1">
              <span className="block text-sm font-medium">
                {task?.title ?? fallback}
              </span>
              <span className="block text-xs text-muted-foreground">
                {formatTimestamp(
                  task?.createdAt ?? group.when,
                  "Time unavailable",
                )}{" "}
                · {count} {count === 1 ? "Service" : "Services"} · {serving}{" "}
                serving
              </span>
            </span>
            {task ? (
              <StatusBadge status={task.status} />
            ) : (
              <span className="text-xs text-muted-foreground">
                {unavailable ? "Task details unavailable" : "Loading Task…"}
              </span>
            )}
          </span>
        }
      >
        <div className="mt-4 space-y-3 border-t border-border pt-4">
          {group.releases.map((release) => (
            <ReleaseHistoryRow key={release.id} release={release} env={env} />
          ))}
        </div>
      </Disclosure>
      <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
        <span>
          {unavailable && task
            ? "Task status could not be refreshed"
            : "Expand to inspect Service releases"}
        </span>
        <TaskLink taskId={group.taskId} />
      </div>
    </div>
  );
}
