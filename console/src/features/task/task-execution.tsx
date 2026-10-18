import { WorkInspection } from "./work-inspection";
import { WorkCheckEvidence } from "./work-check-evidence";
import type { TaskResponse } from "./api";
import {
  ResourcePanel,
  AdvancedDetails,
} from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";

export function TaskExecution({
  detail,
  onChanged,
}: {
  detail: TaskResponse;
  onChanged?: () => void;
}) {
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
      {detail.work.items.map((work) => (
        <div key={work.id} className="space-y-3">
          <WorkInspection
            taskId={detail.task.id}
            work={work}
            revision={detail.work.revision}
            onChanged={onChanged}
          />
          <WorkCheckEvidence task={detail.task} work={work} />
        </div>
      ))}
    </section>
  );
}
