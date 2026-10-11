import { useState } from "react";
import { Check, Circle, LoaderCircle, PlugZap, X } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { Disclosure } from "@/components/common/disclosure";
import { TaskLink } from "@/components/common/task-link";
import { useBackingCheck } from "./use-backing-check";

export function BackingAccessCheck({
  environmentId,
  attachId,
  grants,
  disabled,
}: {
  environmentId: string;
  attachId: string;
  grants: { id: string; name: string }[];
  disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [grantId, setGrantId] = useState("");
  return (
    <Disclosure
      summary={
        <span className="flex items-center gap-2 text-xs font-medium">
          <PlugZap className="size-3.5" />
          Test access
        </span>
      }
      open={open}
      onOpenChange={setOpen}
    >
      <div className="pt-3 space-y-3">
        {grants.length > 0 && (
          <Select
            value={grantId || "own"}
            aria-label="Database to check"
            onValueChange={(value) => setGrantId(value === "own" ? "" : value)}
            options={[
              { value: "own", label: "Own database" },
              ...grants.map((grant) => ({
                value: grant.id,
                label: grant.name,
              })),
            ]}
          />
        )}
        <BackingCheckPanel
          environmentId={environmentId}
          attachId={attachId}
          grantId={grantId}
          open={open}
          disabled={disabled}
        />
      </div>
    </Disclosure>
  );
}

const failures: Record<string, string> = {
  connection:
    "The consumer could not connect to the Backing endpoint. Check its network and the running Backing Service.",
  authentication:
    "The Backing Service rejected this consumer's captured credentials.",
  database_access:
    "Authentication succeeded, but the selected database or command is not accessible.",
  timeout: "The check reached its time limit.",
  canceled: "The check was canceled.",
  protocol: "The endpoint did not return the expected protocol response.",
  interrupted:
    "Execution was interrupted. Cleanup does not rerun the query; start a new check after cleanup.",
  journal: "GP could not persist the check result.",
  runtime:
    "The captured consumer is no longer running; check its current deployment.",
  ownership: "GP could not prove ownership of the temporary helper.",
  cleanup: "The temporary helper could not be removed.",
  execution: "The Agent could not execute the fixed access helper.",
};
const names: Record<string, string> = {
  network: "Connect to endpoint",
  authentication: "Authenticate consumer",
  database_access: "Read-only database access",
};

export function BackingCheckPanel({
  environmentId,
  attachId,
  grantId,
  taskId,
  open,
  disabled = false,
}: {
  environmentId: string;
  attachId?: string;
  grantId?: string;
  taskId?: string;
  open: boolean;
  disabled?: boolean;
}) {
  const check = useBackingCheck(environmentId, open, attachId, grantId, taskId);
  const result = check.result;
  return (
    <section
      aria-label="Backing access check"
      className="flex flex-col gap-3 rounded-lg border border-border bg-surface p-4"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">Consumer access check</h3>
        {attachId && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={
              disabled || check.sending || check.loading || check.active
            }
            onClick={() => void check.run()}
          >
            {check.sending ? (
              <LoaderCircle className="size-3.5 animate-spin" />
            ) : (
              <PlugZap className="size-3.5" />
            )}
            Test access
          </Button>
        )}
      </div>
      {disabled && (
        <p className="text-xs text-muted-foreground">
          The Attach must be ready before its access can be checked.
        </p>
      )}
      {check.loading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading last check…
        </p>
      )}
      {check.taskId && <TaskLink taskId={check.taskId} />}
      {check.error && (
        <p role="alert" className="text-xs text-destructive">
          {check.error}
        </p>
      )}
      {check.error && check.taskId && (
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={check.refresh}
        >
          Refresh result
        </Button>
      )}
      {!check.taskId && !check.loading && (
        <p className="text-xs text-muted-foreground">
          Uses the deployed consumer's network and configured credentials. Runs
          a read-only query; does not change or deploy anything.
        </p>
      )}
      {result && (
        <>
          <div className="flex flex-wrap gap-2 items-center">
            <Badge
              variant={
                result.stale
                  ? "warning"
                  : result.outcome === "passed"
                    ? "success"
                    : result.failure
                      ? "danger"
                      : "muted"
              }
            >
              {result.stale
                ? "Result stale"
                : result.cleanup_required
                  ? "Cleanup needed"
                  : result.phase === "transferred"
                    ? "Cleanup transferred"
                    : result.outcome === "passed"
                      ? "Access verified"
                      : result.outcome === "interrupted"
                        ? "Interrupted"
                        : result.outcome === "failed"
                          ? "Check failed"
                          : "In progress"}
            </Badge>
            <span className="text-xs text-muted-foreground">
              {new Date(
                result.finished_at ?? result.created_at,
              ).toLocaleString()}
            </span>
          </div>
          {result.stale && (
            <p className="text-xs text-muted-foreground">
              Credentials or runtime changed. This result does not verify the
              current configuration.
            </p>
          )}
          {result.failure && (
            <p className="text-xs text-destructive">
              {failures[result.failure] ?? "The access check did not complete."}
            </p>
          )}
          <ol className="flex flex-col gap-2">
            {Object.entries(names).map(([key, name]) => {
              const stage = result.stages?.find((stage) => stage.name === key);
              const Icon =
                stage?.outcome === "passed"
                  ? Check
                  : stage?.outcome === "failed"
                    ? X
                    : Circle;
              return (
                <li key={key} className="flex items-center gap-2 text-xs">
                  <Icon className="size-3.5 shrink-0" />
                  <span className="flex-1">{name}</span>
                  <span className="text-muted-foreground">
                    {stage
                      ? stage.outcome === "skipped"
                        ? "Skipped"
                        : `${stage.elapsed_ms} ms`
                      : result.outcome
                        ? "Not reached"
                        : "Waiting"}
                  </span>
                </li>
              );
            })}
          </ol>
          <p className="text-xs text-muted-foreground break-all">
            {result.configuration.host}:{result.configuration.port}
            {result.configuration.database
              ? ` / ${result.configuration.database}`
              : ""}
            {result.configuration.username
              ? ` as ${result.configuration.username}`
              : ""}
          </p>
          <Disclosure
            summary={
              <span className="text-xs font-medium">Execution details</span>
            }
          >
            <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 pt-2 text-xs">
              <dt>Location</dt>
              <dd>{result.execution_location}</dd>
              <dt>Container</dt>
              <dd className="break-all">{result.configuration.container_id}</dd>
              <dt>Authentication</dt>
              <dd>{result.configuration.authentication}</dd>
              <dt>Attach revision</dt>
              <dd>{result.configuration.attach_revision}</dd>
              <dt>Credential revision</dt>
              <dd>{result.configuration.facts_revision}</dd>
            </dl>
          </Disclosure>
          {result.helper_name && (
            <p className="text-xs text-muted-foreground break-all">
              Temporary helper: {result.helper_name}
            </p>
          )}
          {result.cleanup_task_id && (
            <TaskLink taskId={result.cleanup_task_id}>
              Open cleanup task
            </TaskLink>
          )}
          {result.cleanup_required && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={check.sending}
              onClick={() => void check.cleanup()}
            >
              Clean up helper — do not rerun check
            </Button>
          )}
          <p className="text-xs text-muted-foreground">
            Verifies access at this time, not all application permissions or
            container health.
          </p>
        </>
      )}
    </section>
  );
}
