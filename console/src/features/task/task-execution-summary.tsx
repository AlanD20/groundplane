import { StatusBadge } from "@/components/common/status-badge";
import type { ActivityEntry, TaskStep } from "@/lib/types";
import { CircleAlert, CircleCheck, Clock3, LoaderCircle } from "lucide-react";
import { useEffect, useState } from "react";
import type { TaskEventResponse } from "./journal-model";

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
  const latestFailure = events
    .filter((event) => ["failed", "timed_out", "aborted"].includes(event.state))
    .at(-1);
  const failedStep = latestFailure
    ? steps.find((step) => step.label === latestFailure.step_id)
    : steps.find((step) => step.state === "failed");
  const runningStep = steps.find((step) => step.state === "running");
  const completed = steps.filter((step) => step.state === "done").length;
  const lastSuccess = events
    .filter((event) => event.state === "completed")
    .at(-1);
  const lastSuccessStep = lastSuccess
    ? steps.find((step) => step.label === lastSuccess.step_id)
    : undefined;
  const started = task.startedAt ? Date.parse(task.startedAt) : undefined;
  const finished = task.finishedAt ? Date.parse(task.finishedAt) : undefined;
  const elapsed =
    started === undefined
      ? undefined
      : Math.max(0, ((finished ?? now) - started) / 1000);
  const deadline =
    started !== undefined && task.timeoutSeconds
      ? started + task.timeoutSeconds * 1000
      : undefined;
  const failed = task.status === "failed" || task.status === "timed_out";
  const heading =
    task.status === "pending"
      ? "Queued for execution"
      : task.status === "running"
        ? (runningStep?.action ?? "Execution in progress")
        : task.status === "completed"
          ? "Completed"
          : task.status === "aborted"
            ? "Aborted"
            : task.status === "timed_out"
              ? "Time limit reached"
              : failedStep?.action
                ? `Failed: ${failedStep.action}`
                : "Execution failed";
  const Icon = active
    ? LoaderCircle
    : task.status === "completed"
      ? CircleCheck
      : CircleAlert;
  const summary =
    failed || task.status === "aborted"
      ? task.failureSummary
      : task.resultSummary;

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
          {!summary && failed && (
            <p className="text-xs text-muted-foreground">
              No specific failure reason was recorded.
            </p>
          )}
          {active && runningStep?.target && (
            <p className="break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
              {runningStep.target}
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
        {active && deadline !== undefined && (
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
      {(failed || task.status === "aborted") && failedStep && (
        <StageReference label="Stopped stage" step={failedStep} />
      )}
      {(failed || task.status === "aborted") && lastSuccessStep && (
        <StageReference label="Last recorded success" step={lastSuccessStep} />
      )}
      {task.reconciliationRequired && (
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

function StageReference({ label, step }: { label: string; step: TaskStep }) {
  return (
    <div className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 text-xs">
      <span className="text-muted-foreground">{label}</span>
      <span className="break-words [overflow-wrap:anywhere]">
        {step.action ?? "Action not recorded"}
        {step.target ? ` — ${step.target}` : ""}
      </span>
    </div>
  );
}

export function taskDuration(seconds: number) {
  const whole = Math.floor(seconds);
  if (whole < 1) return "<1s";
  if (whole < 60) return `${whole}s`;
  const minutes = Math.floor(whole / 60);
  const remainder = whole % 60;
  if (minutes < 60)
    return remainder ? `${minutes}m ${remainder}s` : `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}
