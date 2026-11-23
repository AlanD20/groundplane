import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { Link } from "react-router-dom";
import { StatusBadge } from "@/components/common/status-badge";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { requestTask } from "@/features/task/api";
import { taskFromAPI } from "@/features/task/journal-model";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import type { ActivityEntry } from "@/lib/types";
import type { DeploymentGroup } from "./deployment-groups";

export function DeploymentGroupRow({
  group,
  to,
}: {
  group: DeploymentGroup;
  to: string;
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
    <Link
      to={to}
      className={`${workspaceSectionClassName(false)} flex items-center gap-3 transition-colors hover:bg-muted/60 focus-visible:outline-2 focus-visible:outline-ring`}
    >
      <span className="min-w-0 flex-1 space-y-1">
        <span className="block break-words text-sm font-medium">
          {task?.title ?? fallback}
        </span>
        <span className="block text-xs text-muted-foreground">
          {formatTimestamp(task?.createdAt ?? group.when, "Time unavailable")}
          {" · "}
          {count} {count === 1 ? "Service" : "Services"} · {serving} serving
        </span>
      </span>
      {task ? (
        <StatusBadge status={task.status} />
      ) : (
        unavailable && (
          <span className="text-xs text-muted-foreground">
            Status unavailable
          </span>
        )
      )}
      <ChevronRight
        aria-hidden
        className="size-4 shrink-0 text-muted-foreground"
      />
    </Link>
  );
}
