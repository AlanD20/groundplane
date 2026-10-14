import { Disclosure } from "@/components/common/disclosure";
import type { ActivityEntry, TaskStep } from "@/lib/types";

export function TaskFailureDetails({
  failure,
  steps,
  label,
}: {
  failure: NonNullable<ActivityEntry["failure"]>;
  steps: TaskStep[];
  label: string;
}) {
  const step = steps.find((value) => value.label === failure.step_id);
  return (
    <div className="space-y-2 rounded-lg border border-destructive/20 bg-destructive/5 p-3 text-xs">
      <h4 className="font-medium text-destructive">{label}</h4>
      <p className="break-words [overflow-wrap:anywhere]">{failure.summary}</p>
      {step && (
        <p className="break-words text-muted-foreground [overflow-wrap:anywhere]">
          {step.action ?? "Action not recorded"}
          {step.target ? ` — ${step.target}` : ""}
        </p>
      )}
      {(failure.code || failure.step_id || failure.exit_code !== undefined) && (
        <Disclosure summary="Failure details">
          <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-muted-foreground">
            <dt>Outcome</dt>
            <dd>{failure.status.replaceAll("_", " ")}</dd>
            {failure.code && (
              <>
                <dt>Code</dt>
                <dd className="break-all font-mono">{failure.code}</dd>
              </>
            )}
            {failure.exit_code !== undefined && (
              <>
                <dt>Exit code</dt>
                <dd>{failure.exit_code}</dd>
              </>
            )}
            {failure.step_id && (
              <>
                <dt>Step ID</dt>
                <dd className="break-all font-mono">{failure.step_id}</dd>
              </>
            )}
          </dl>
        </Disclosure>
      )}
    </div>
  );
}

export function TaskRecoveryDetails({ task }: { task: ActivityEntry }) {
  const recovery = task.recovery;
  if (!recovery) return null;
  const labels = {
    pending: "Recovery queued",
    running: "Recovery in progress",
    awaiting_confirmation: "Waiting for recovery confirmation",
    proof_required: "Recovery needs proof",
    retry_wait: "Waiting before recovery retry",
    automatic_retries_stopped: "Automatic recovery stopped",
    completed: "Recovery verified",
  };
  return (
    <div className="space-y-3 rounded-lg border border-border p-3 text-xs">
      <h4 className="font-medium">{labels[recovery.state]}</h4>
      <p className="break-words text-muted-foreground [overflow-wrap:anywhere]">
        {recovery.summary}
      </p>
      {recovery.automatic_failures > 0 && (
        <p className="text-muted-foreground">
          Failed recovery attempts: {recovery.automatic_failures}
        </p>
      )}
      {recovery.retry_after && (
        <p className="text-muted-foreground">
          Next eligible attempt:{" "}
          <time dateTime={recovery.retry_after}>
            {new Date(recovery.retry_after).toLocaleString()}
          </time>
        </p>
      )}
      {recovery.completed_steps !== undefined &&
        recovery.total_steps !== undefined && (
          <p className="text-muted-foreground">
            {recovery.completed_steps} of {recovery.total_steps} recovery stages
            completed
          </p>
        )}
      {recovery.state !== "completed" && recovery.deadline && (
        <p className="text-muted-foreground">
          Recovery deadline{" "}
          <time dateTime={recovery.deadline}>
            {new Date(recovery.deadline).toLocaleString()}
          </time>
        </p>
      )}
      {recovery.last_failure && (
        <TaskFailureDetails
          failure={recovery.last_failure}
          steps={task.steps ?? []}
          label="Last recorded recovery failure"
        />
      )}
    </div>
  );
}
