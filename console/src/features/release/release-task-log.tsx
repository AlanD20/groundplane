import { useEffect, useState } from "react";
import { requestTask, type TaskResponse } from "@/features/task/api";
import { TaskExecution } from "@/features/task/task-execution";
import { TaskLink } from "@/components/common/task-link";
import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { taskFromAPI } from "@/features/task/journal-model";
import { operationLabel } from "@/features/task/operation-label";
import { TaskDependencyStatus } from "@/features/task/task-dependency-status";
import { WorkCheckEvidence } from "@/features/task/work-check-evidence";
import { WorkInspection } from "@/features/task/work-inspection";

export function ReleaseTaskLog({
  taskId,
  serviceId,
}: {
  taskId: string;
  serviceId: string;
}) {
  const [detail, setDetail] = useState<TaskResponse | null>(null);
  const [error, setError] = useState<string>();
  const [reload, setReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    setError(undefined);
    const refresh = async () => {
      let terminal = false;
      try {
        const next = await requestTask(taskId, controller.signal);
        if (controller.signal.aborted) return;
        setDetail(next);
        setError(undefined);
        terminal = !!next.task.finished_at;
      } catch (cause) {
        if (controller.signal.aborted) return;
        setError(
          cause instanceof Error
            ? cause.message
            : "Unable to load deployment execution",
        );
      }
      if (!terminal && !controller.signal.aborted)
        timer = setTimeout(refresh, 2000);
    };
    void refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [taskId, reload]);
  const task = detail?.task.id === taskId ? detail : null;
  return (
    <div className="min-w-0 space-y-3">
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      {task ? (
        <DeploymentExecution
          detail={task}
          serviceId={serviceId}
          onChanged={() => setReload((value) => value + 1)}
        />
      ) : (
        !error && (
          <p role="status" className="text-sm text-muted-foreground">
            Loading deployment execution…
          </p>
        )
      )}
    </div>
  );
}

function DeploymentExecution({
  detail,
  serviceId,
  onChanged,
}: {
  detail: TaskResponse;
  serviceId: string;
  onChanged: () => void;
}) {
  const task = detail.task;
  const counts = task.counts;
  const summary = (
    [
      ["Failed", counts.failed],
      ["Needs attention", counts.attention],
      ["Running", counts.running],
      ["Ready or assigned", counts.ready + counts.assigned],
      ["Waiting", counts.waiting],
      ["Succeeded", counts.succeeded],
      ["Cancelled", counts.cancelled],
      ["Superseded", counts.superseded],
    ] as const
  ).filter(([, count]) => count > 0);
  const direct = detail.work.items.filter(
    (work) => work.target_id === serviceId,
  );
  const other = detail.work.items.filter(
    (work) => work.target_id !== serviceId,
  );
  const otherProblems = other.filter(
    (work) => work.phase === "attention" || work.outcome === "failed",
  ).length;
  const showOverview =
    detail.work.items.length > 0 &&
    !task.history_expired &&
    !detail.preparation;
  return (
    <section aria-label="Deployment execution" className="min-w-0 space-y-4">
      <header className="flex flex-wrap items-start justify-between gap-2">
        <div className="space-y-1">
          <h3 className="text-sm font-semibold">Deployment execution</h3>
          <p className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
            <span>Originating Task · {taskFromAPI(task).title}</span>
            <StatusBadge status={task.status} />
          </p>
        </div>
        <TaskLink taskId={task.id}>Open full Task</TaskLink>
      </header>
      {summary.length > 0 && (
        <ul
          aria-label="Whole Task results"
          className="flex flex-wrap gap-x-4 gap-y-1 border-b border-border pb-3 text-xs"
        >
          {summary.map(([label, count]) => (
            <li key={label} className="text-muted-foreground">
              <strong
                className={`font-semibold tabular-nums ${label === "Failed" ? "text-destructive" : label === "Needs attention" ? "text-warning" : "text-foreground"}`}
              >
                {count}
              </strong>{" "}
              {label}
            </li>
          ))}
        </ul>
      )}
      {showOverview ? (
        <>
          <TaskDependencyStatus task={task} />
          {direct.length > 0 && (
            <section
              aria-label="Operations for this Service"
              className="space-y-2"
            >
              <h4 className="text-xs font-medium">
                Operations for this Service
              </h4>
              <DeploymentWorkItems
                detail={detail}
                items={direct}
                onChanged={onChanged}
              />
            </section>
          )}
          {other.length > 0 && (
            <Disclosure
              summary={
                <span className="flex min-w-0 flex-1 flex-wrap items-center gap-x-2 gap-y-1 text-xs font-medium">
                  Other work in this Task
                  <span className="text-muted-foreground">{other.length}</span>
                  {otherProblems > 0 && (
                    <span className="text-destructive">
                      {otherProblems} with issues
                    </span>
                  )}
                </span>
              }
              summaryClassName="py-2"
            >
              <DeploymentWorkItems
                detail={detail}
                items={other}
                onChanged={onChanged}
              />
            </Disclosure>
          )}
          {(detail.work.more || detail.work.scan_limited) && (
            <p className="text-xs text-muted-foreground">
              Open the full Task to see the remaining operations.
            </p>
          )}
        </>
      ) : (
        <TaskExecution detail={detail} onChanged={onChanged} />
      )}
    </section>
  );
}

function DeploymentWorkItems({
  detail,
  items,
  onChanged,
}: {
  detail: TaskResponse;
  items: TaskResponse["work"]["items"];
  onChanged: () => void;
}) {
  return (
    <ul className="divide-y divide-border rounded-lg border border-border">
      {items.map((work) => (
        <li key={work.id}>
          <Disclosure
            className="px-3"
            summaryClassName="py-2.5"
            summary={
              <span className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-2">
                <span className="min-w-0 text-sm font-medium [overflow-wrap:anywhere]">
                  {operationLabel(work.operation)}
                  {work.target_name && (
                    <span className="ml-2 text-xs font-normal text-muted-foreground">
                      {" "}
                      {work.target_name}
                    </span>
                  )}
                </span>
                <StatusBadge status={work.outcome || work.phase} />
              </span>
            }
          >
            <div className="space-y-3 border-t border-border">
              <WorkInspection
                inline
                taskId={detail.task.id}
                work={work}
                revision={detail.work.revision}
                onChanged={onChanged}
              />
              <WorkCheckEvidence task={detail.task} work={work} />
            </div>
          </Disclosure>
        </li>
      ))}
    </ul>
  );
}
