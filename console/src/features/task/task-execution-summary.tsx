import { StatusBadge } from "@/components/common/status-badge";
import type { ActivityEntry } from "@/lib/types";
import { CircleAlert, CircleCheck, Clock3, LoaderCircle } from "lucide-react";
import { useEffect, useState } from "react";
import type { TaskEventResponse } from "./journal-model";
import { TaskReadinessDetails } from "./task-readiness";
import { TaskConfiguredHealthchecks } from "./task-configured-healthchecks";
import { taskDuration } from "./task-duration";
import {
  TaskFailureDetails,
  TaskRecoveryDetails,
} from "./task-failure-details";

export function TaskExecutionSummary({
  task,
  events,
}: {
  task: ActivityEntry;
  events: TaskEventResponse[];
}) {
  const [now, setNow] = useState(Date.now);
  const active = task.status === "pending" || task.status === "running";
  useEffect(() => {
    setNow(Date.now());
    if (!active || !task.startedAt) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [task.id, task.startedAt, active]);

  const steps = task.steps ?? [];
  const nativeEvent = events.filter((event) => event.controller_operation).at(-1);
  const latestFailure = events
    .filter((event) => ["failed", "timed_out", "aborted"].includes(event.state))
    .at(-1);
  const failedStep = task.failure?.step_id
    ? steps.find((step) => step.label === task.failure?.step_id)
    : !task.recovery && latestFailure
      ? steps.find((step) => step.label === latestFailure.step_id)
      : undefined;
  const recovery = task.recovery;
  const runningStep = steps.find(
    (step) =>
      step.state === "running" &&
      (!recovery || recovery.step_ids?.includes(step.label)),
  );
  const completed = steps.filter((step) => step.state === "done").length;
  const failureEvent = task.failure?.step_id
    ? events.find(
        (event) =>
          event.step_id === task.failure?.step_id &&
          ["failed", "timed_out", "aborted"].includes(event.state),
      )
    : !recovery
      ? latestFailure
      : undefined;
  const lastSuccess = failureEvent
    ? events
        .filter(
          (event) =>
            event.state === "completed" &&
            event.sequence < failureEvent.sequence,
        )
        .at(-1)
    : undefined;
  const lastSuccessStep = lastSuccess
    ? steps.find((step) => step.label === lastSuccess.step_id)
    : undefined;
  const failedAction = failedStep?.action ?? failureEvent?.controller_operation?.name;
  const runningAction = nativeEvent?.state === "running"
    ? nativeEvent.controller_operation?.name
    : runningStep?.action;
  const lastSuccessAction = lastSuccessStep?.action ?? lastSuccess?.controller_operation?.name;
  const started = task.startedAt ? Date.parse(task.startedAt) : undefined;
  const finished = task.finishedAt ? Date.parse(task.finishedAt) : undefined;
  const elapsed =
    started === undefined
      ? undefined
      : Math.max(0, ((finished ?? now) - started) / 1000);
  const deadline = task.preparation
    ? Date.parse(task.preparation.deadline)
    : started !== undefined && task.timeoutSeconds
      ? started + task.timeoutSeconds * 1000
      : undefined;
  const failed = task.status === "failed" || task.status === "timed_out";
  const heading =
    recovery && recovery.state !== "completed"
      ? recovery.state === "proof_required"
        ? "Recovery needs proof"
        : "Recovering after an unsuccessful operation"
      : task.status === "pending"
        ? task.preparation
          ? {
            network_queued: "Waiting to prepare internal networks",
            network_preparing: "Preparing internal networks",
            network_reconciling: "Checking interrupted network preparation",
            network_proof_required: "Network preparation needs proof",
            network_failed: "Network preparation failed",
            dns_waiting: "Waiting for CoreDNS",
            dns_failed: "CoreDNS activation failed",
            ready: "Ready for workload deployment",
          }[task.preparation.state]
          : "Queued for execution"
        : task.status === "running"
          ? runningAction ?? "Execution in progress"
          : task.status === "completed"
            ? "Completed"
            : task.status === "aborted"
              ? "Aborted"
              : task.status === "timed_out"
                ? "Time limit reached"
                : failedAction
                  ? `Failed: ${failedAction}`
                  : "Execution failed";
  const Icon = active
    ? LoaderCircle
    : task.status === "completed"
      ? CircleCheck
      : CircleAlert;
  const summary = !task.failure && !recovery ? task.preparation?.summary ?? task.resultSummary : undefined;

  return (
    <div className="space-y-3 rounded-xl border border-border bg-card p-4">
      <div className="flex items-start gap-3">
        <Icon
          aria-hidden
          className={`mt-0.5 size-4 shrink-0 ${active ? "text-primary" : failed ? "text-destructive" : "text-muted-foreground"}`}
        />
        <div className="min-w-0 flex-1 space-y-1">
          <h3 className="break-words text-sm font-medium [overflow-wrap:anywhere]">
            {heading}
          </h3>
          {summary && (
            <p className="break-words text-sm text-muted-foreground [overflow-wrap:anywhere]">
              {summary}
            </p>
          )}
          {task.preparation?.failure && (
            <TaskFailureDetails failure={task.preparation.failure} steps={[]} label="Network preparation failure" />
          )}
          {!task.failure && !summary && failed && (
            <p className="text-xs text-muted-foreground">
              No specific failure reason was recorded.
            </p>
          )}
          {active && runningStep?.target && (
            <p className="break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
              {runningStep.target}
            </p>
          )}
          {active && runningStep?.healthWait && (
            <TaskReadinessDetails
              value={runningStep.healthWait}
              receivedAt={runningStep.healthWaitReceivedAt}
            />
          )}
          {(active || failed) && (
            <TaskConfiguredHealthchecks
              checks={(active ? runningStep : failedStep)?.healthchecks}
            />
          )}
          {active &&
            runningStep?.healthWaitReceivedAt &&
            now - Date.parse(runningStep.healthWaitReceivedAt) > 60000 && (
              <p className="text-xs text-warning">
                Last readiness update was{" "}
                {taskDuration(
                  (now - Date.parse(runningStep.healthWaitReceivedAt)) / 1000,
                )}{" "}
                ago. Check the Agent connection.
              </p>
            )}
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-xs text-muted-foreground">
        {elapsed !== undefined && (
          <span className="inline-flex items-center gap-1.5 tabular-nums">
            <Clock3 aria-hidden className="size-3.5" />
            {task.finishedAt ? "Elapsed" : "Running for"}{" "}
            {taskDuration(elapsed)}
          </span>
        )}
        {steps.length > 0 && (
          <span>
            {completed} of {steps.length} stages completed
          </span>
        )}
        {task.preparation && <span>{task.preparation.network_count} internal DNS networks</span>}
        {active && !recovery && deadline !== undefined && (
          <time
            dateTime={new Date(deadline).toISOString()}
            title="Forward execution deadline; recovery may continue beyond it"
          >
            Deadline {new Date(deadline).toLocaleTimeString()}
          </time>
        )}
      </div>
      {steps.length > 0 && (
        <div
          role="progressbar"
          aria-label="Completed execution stages"
          aria-valuemin={0}
          aria-valuemax={steps.length}
          aria-valuenow={completed}
          className="h-1 overflow-hidden rounded-full bg-surface"
        >
          <div
            className="h-full bg-primary transition-[width] duration-300 motion-reduce:transition-none"
            style={{ width: `${(completed / steps.length) * 100}%` }}
          />
        </div>
      )}
      {task.failure && (
        <TaskFailureDetails
          failure={task.failure}
          steps={steps}
          label={recovery ? "Original failure" : "Failure"}
        />
      )}
      <TaskRecoveryDetails task={task} />
      {(failed || task.status === "aborted") && lastSuccessAction && (
        <StageReference
          label="Last recorded success"
          action={lastSuccessAction}
          target={lastSuccessStep?.target}
        />
      )}
      {task.reconciliationRequired && !recovery && (
        <div
          role="alert"
          className="space-y-2 rounded-lg border border-warning/30 bg-warning/10 p-3 text-xs"
        >
          <StatusBadge
            status="degraded"
            label="Effects not yet accounted for"
          />
          <p>
            GP has not proved cleanup or restoration. Completion of the original
            action must not be assumed.
          </p>
        </div>
      )}
    </div>
  );
}

function StageReference({ label, action, target }: { label: string; action: string; target?: string }) {
  return (
    <div className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="break-words [overflow-wrap:anywhere]">
        {action}
        {target ? ` — ${target}` : ""}
      </span>
    </div>
  );
}
