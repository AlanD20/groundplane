import { useState } from "react";
import { Select } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { WorkInspection } from "./work-inspection";
import { WorkCheckEvidence } from "./work-check-evidence";
import type { TaskResponse } from "./api";
import {
  ResourcePanel,
  AdvancedDetails,
} from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { operationLabel } from "./operation-label";

export function TaskExecution({
  detail,
  onChanged,
}: {
  detail: TaskResponse;
  onChanged?: () => void;
}) {
  const [filter, setFilter] = useState("all");
  const attention = detail.work.items.filter(
    (work) => work.phase === "attention" || work.outcome === "failed",
  );
  const waiting = detail.work.items.filter((work) => work.phase === "waiting");
  const visible = detail.work.items.filter((work) => {
    switch (filter) {
      case "active":
        return (
          work.phase === "running" ||
          work.phase === "assigned" ||
          work.phase === "ready"
        );
      case "waiting":
        return work.phase === "waiting";
      case "attention":
        return work.phase === "attention" || work.outcome === "failed";
      case "succeeded":
        return work.outcome === "succeeded";
      case "cleanup":
        return work.cleanup !== "none";
      default:
        return true;
    }
  });
  if (detail.task.history_expired)
    return (
      <p className="text-sm text-muted-foreground">
        Execution details are no longer retained.
      </p>
    );
  if (detail.preparation) {
    const preparation = detail.preparation;
    return (
      <ResourcePanel
        title="Preparation stopped"
        actions={<StatusBadge status={preparation.outcome} />}
      >
        <p className="text-sm">{preparation.summary}</p>
        <div className="space-y-1 text-sm">
          <h3 className="font-medium">Next action</h3>
          <p>{preparation.next_action}</p>
        </div>
        <AdvancedDetails title="Technical details">
          <p className="break-words">Reason: {preparation.reason}</p>
          <p>
            Finished:{" "}
            <time dateTime={preparation.finished_at}>
              {new Date(preparation.finished_at).toLocaleString()}
            </time>
          </p>
        </AdvancedDetails>
      </ResourcePanel>
    );
  }
  if (detail.work.items.length === 0)
    return (
      <p className="text-sm text-muted-foreground">
        {detail.task.finished_at
          ? "This Task has finished. There are no work items on this page."
          : "Work is being prepared. No work items have been published yet."}
      </p>
    );
  return (
    <section aria-label="Task work" className="min-w-0 space-y-4">
      {detail.work.items.length > 1 &&
        (attention.length > 0 || waiting.length > 0) && (
          <section
            aria-label="Resources needing attention"
            className="space-y-3 rounded-lg border border-border bg-muted/30 p-3"
          >
            {[
              {
                value: "attention",
                label: "Needs attention",
                items: attention,
              },
              { value: "waiting", label: "Waiting", items: waiting },
            ].map(
              (group) =>
                group.items.length > 0 && (
                  <div key={group.value} className="min-w-0 space-y-1">
                    <Button
                      variant="outline"
                      size="sm"
                      aria-pressed={filter === group.value}
                      onClick={() => setFilter(group.value)}
                    >
                      {group.label} · {group.items.length}
                    </Button>
                    <p className="break-words text-sm">
                      {group.items
                        .map(
                          (work) =>
                            work.target_name || operationLabel(work.operation),
                        )
                        .join(", ")}
                    </p>
                  </div>
                ),
            )}
          </section>
        )}
      {detail.work.items.length > 1 && (
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="text-sm font-medium">Resource work</h2>
          <Select
            aria-label="Filter work on this page"
            className="ml-auto w-48"
            value={filter}
            onValueChange={setFilter}
            options={[
              { value: "all", label: "All on this page" },
              { value: "active", label: "Active" },
              { value: "waiting", label: "Waiting" },
              { value: "attention", label: "Needs attention" },
              { value: "succeeded", label: "Succeeded" },
              { value: "cleanup", label: "With cleanup" },
            ]}
          />
        </div>
      )}
      {visible.length === 0 && (
        <p className="text-sm text-muted-foreground">
          No work on this page matches this filter.
        </p>
      )}
      {visible.map((work) => (
        <div key={work.id} className="space-y-3">
          <WorkInspection
            taskId={detail.task.id}
            work={work}
            revision={detail.work.revision}
            targetName={
              work.target_name ||
              (work.target_id === detail.task.target_id
                ? detail.task.target_name
                : undefined)
            }
            onChanged={onChanged}
          />
          <WorkCheckEvidence task={detail.task} work={work} />
        </div>
      ))}
    </section>
  );
}
