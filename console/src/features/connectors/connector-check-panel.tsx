import {
  Check,
  Circle,
  LoaderCircle,
  PlugZap,
  RefreshCw,
  X,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { TaskLink } from "@/components/common/task-link";
import { Disclosure } from "@/components/common/disclosure";
import { CopyButton } from "@/components/common/copy-button";
import { type ConnectorCheckRequest, type ConnectorCheckResult } from "./api";
import { useConnectorCheck } from "./use-connector-check";

export function ConnectorCheckPanel({
  environmentId,
  identity,
  open,
  getRequest,
  disabled = false,
  taskId,
  connectorId,
}: {
  environmentId: string;
  identity: string;
  open: boolean;
  disabled?: boolean;
  taskId?: string;
  connectorId?: string;
  getRequest?: () => Promise<ConnectorCheckRequest>;
}) {
  const check = useConnectorCheck(
    environmentId,
    identity,
    open,
    taskId,
    connectorId,
  );
  return (
    <section
      className="flex flex-col gap-3 rounded-lg border border-border bg-surface p-4"
      aria-label="Connection check"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">Connection check</h3>
        {getRequest && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={
              disabled || check.sending || check.active || check.loadingLatest
            }
            onClick={() => void check.run(getRequest)}
          >
            <PlugZap className="size-3.5" />
            {check.sending
              ? "Submitting…"
              : check.active
                ? "Checking…"
                : "Test connection"}
          </Button>
        )}
      </div>
      {check.loadingLatest ? (
        <p className="text-xs text-muted-foreground" role="status">
          Loading last saved check…
        </p>
      ) : (
        !check.taskId && (
          <p className="text-xs text-muted-foreground">
            Checks object and multipart permissions from the Controller. Testing
            does not save these settings.
          </p>
        )
      )}
      {check.taskId && <TaskLink taskId={check.taskId} />}
      {check.active && !check.error && (
        <p
          className="flex items-center gap-2 text-xs text-muted-foreground"
          role="status"
        >
          <LoaderCircle className="size-3.5 animate-spin" />
          {!check.result
            ? "Reading connection check progress"
            : check.result.phase === "ready"
              ? "Queued on Controller"
              : check.result?.phase === "cleaning"
                ? "Cleaning up owned test objects"
                : "Checking configured storage permissions"}
        </p>
      )}
      {check.error && (
        <div className="flex flex-wrap items-center gap-2" role="alert">
          <p className="text-xs text-destructive">{check.error}</p>
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={check.sending || check.loadingLatest}
            onClick={check.refresh}
          >
            Refresh result
          </Button>
        </div>
      )}
      {check.result && (
        <ConnectorCheckResults result={check.result} stale={!!check.stale} />
      )}
      {check.result?.cleanup_required && (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="self-start"
          disabled={check.sending}
          onClick={() => void check.retryCleanup()}
        >
          <RefreshCw className="size-3.5" />
          Retry cleanup only
        </Button>
      )}
    </section>
  );
}

const failures: Record<string, string> = {
  timeout: "The check timed out.",
  canceled: "The check was canceled.",
  permissions: "Authentication or required storage permissions were denied.",
  connection: "The configured storage endpoint could not be reached.",
  ownership:
    "The test object's ownership did not match. GP did not delete unverified content.",
  journal: "GP could not record the check result.",
  interrupted:
    "Execution was interrupted. Cleanup does not certify storage permissions.",
};

export function ConnectorCheckResults({
  result,
  stale,
}: {
  result: ConnectorCheckResult;
  stale: boolean;
}) {
  const passed = result.outcome === "passed" && result.phase === "finished";
  const stages = [
    "Write object",
    "Read and verify object",
    "Create and upload multipart part",
    "Delete object and abort multipart; verify cleanup",
  ];
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <Badge
          variant={
            stale
              ? "warning"
              : passed
                ? "success"
                : result.failure
                  ? "danger"
                  : "muted"
          }
        >
          {stale
            ? "Result stale"
            : result.phase === "transferred"
              ? "Cleanup transferred"
              : passed
                ? "Permissions verified"
                : result.cleanup_required
                  ? "Cleanup needed"
                  : result.outcome === "interrupted"
                    ? "Interrupted check"
                    : result.outcome === "failed"
                      ? "Check failed"
                      : "In progress"}
        </Badge>
        {result.draft && <Badge variant="outline">Unsaved settings</Badge>}
        <span className="text-xs text-muted-foreground">
          {result.execution_location} ·{" "}
          {new Date(result.finished_at ?? result.created_at).toLocaleString()}
        </span>
      </div>
      {stale && (
        <p className="text-xs text-muted-foreground">
          Settings or credential sources changed. Run a new check for the
          current inputs.
        </p>
      )}
      {result.failure && (
        <p className="text-xs text-destructive">
          {failures[result.failure] ?? "The check did not complete."}
        </p>
      )}
      <ol className="flex flex-col gap-2">
        {stages.map((name) => {
          const stage = result.stages?.find((stage) => stage.name === name);
          const Icon = stage ? (stage.succeeded ? Check : X) : Circle;
          return (
            <li key={name} className="flex items-start gap-2 text-xs">
              <Icon
                className={`mt-0.5 size-3.5 shrink-0 ${stage ? (stage.succeeded ? "text-success" : "text-destructive") : "text-muted-foreground"}`}
              />
              <span className="min-w-0 flex-1">
                {name}
                {stage?.failure && (
                  <span className="mt-1 block text-muted-foreground">
                    {failures[stage.failure] ?? "Stage failed."}
                  </span>
                )}
              </span>
              <span className="shrink-0 text-muted-foreground">
                {stage ? `${stage.elapsed_ms} ms` : "Not recorded"}
              </span>
            </li>
          );
        })}
      </ol>
      {result.cleanup_required && (
        <div className="flex flex-col gap-1 text-xs">
          <p className="text-warning">
            Temporary test content may remain. Retry uses the original captured
            credentials and does not repeat the probe.
          </p>
          {[
            result.object_key,
            result.multipart_key,
            ...(result.upload_ids ?? []),
          ]
            .filter(Boolean)
            .map((value) => (
              <CopyButton
                key={value}
                value={value!}
                className="h-auto max-w-full justify-start whitespace-normal break-all font-mono text-xs"
              >
                {value}
              </CopyButton>
            ))}
        </div>
      )}
      {result.cleanup_task_id && (
        <TaskLink taskId={result.cleanup_task_id}>
          Open cleanup attempt
        </TaskLink>
      )}
      <Disclosure
        summary="Tested configuration"
        summaryClassName="text-xs font-medium"
      >
        <dl className="mt-2 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-xs">
          {Object.entries({
            Endpoint: result.configuration.endpoint,
            Bucket: result.configuration.bucket,
            Prefix: result.configuration.prefix || "(none)",
            Region: result.configuration.region,
            Addressing: result.configuration.path_style
              ? "Path style"
              : "Virtual hosted",
            Revision: result.connector_revision || "Unsaved draft",
          }).map(([key, value]) => (
            <div key={key} className="contents">
              <dt className="text-muted-foreground">{key}</dt>
              <dd className="break-all">{value}</dd>
            </div>
          ))}
        </dl>
        {(result.secret_sources ?? []).map((source) => (
          <p
            key={source.credential}
            className="mt-2 break-all text-xs text-muted-foreground"
          >
            {source.credential}: {source.secret_id}, revision {source.revision}
          </p>
        ))}
      </Disclosure>
      {passed && (
        <p className="text-xs text-muted-foreground">
          Verified at that time. This is not a Backup/Restore qualification or
          permanent availability guarantee.
        </p>
      )}
    </div>
  );
}
