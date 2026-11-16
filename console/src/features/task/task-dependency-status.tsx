import { TaskReferenceLink } from "./task-reference-link";
import { StatusBadge } from "@/components/common/status-badge";
import type { TaskResponse } from "./api";

export function TaskDependencyStatus({ task }: { task: TaskResponse["task"] }) {
  const after = task.after ?? [];
  if (!after.length && !task.pending_preparation) return null;
  const decision = task.dependencies;
  const failed = decision?.failed;
  const wait = task.preparation_wait;
  return (
    <section
      aria-label="Whole-Task dependencies"
      className="min-w-0 space-y-3 rounded-lg border border-border p-3"
    >
      <h3 className="text-sm font-medium">
        {wait && task.pending_preparation
          ? "Preparation waiting"
          : failed
            ? "Blocked by a prerequisite"
            : task.pending_preparation && !decision?.ready && after.length
              ? "Waiting for Tasks"
              : task.pending_preparation
                ? "Preparing accepted action"
                : "Prerequisites"}
      </h3>
      {task.pending_preparation && !task.finished_at && (
        <p className="text-sm text-muted-foreground">
          {after.length && !decision?.ready
            ? "Waiting for these Tasks to finish successfully."
            : "The Controller is preparing this accepted action."}
        </p>
      )}
      {wait && (
        <div className="space-y-1 text-sm">
          <p>{wait.detail}</p>
          {wait.until && (
            <p className="text-xs text-muted-foreground">
              Deadline:{" "}
              <time dateTime={wait.until}>
                {new Date(wait.until).toLocaleString()}
              </time>
            </p>
          )}
        </div>
      )}
      {failed && (
        <p className="flex flex-wrap items-center gap-2 text-sm">
          This action cannot run because{" "}
          <TaskReferenceLink taskId={failed.task_id} /> reported{" "}
          <StatusBadge status={failed.outcome} />.
        </p>
      )}
      {!!after.length && (
        <ul className="space-y-2">
          {after.map((id) => (
            <li key={id} className="flex flex-wrap items-center gap-2">
              <TaskReferenceLink taskId={id} />
              {failed?.task_id === id ? (
                <StatusBadge status={failed.outcome} />
              ) : decision?.waiting?.includes(id) ? (
                <span className="text-xs text-muted-foreground">
                  Waiting to finish
                </span>
              ) : decision?.ready ? (
                <span className="text-xs text-muted-foreground">
                  Completed successfully
                </span>
              ) : null}
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
