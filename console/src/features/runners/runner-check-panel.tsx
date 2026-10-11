import {
  Check,
  Circle,
  LoaderCircle,
  PlugZap,
  RefreshCw,
  X,
} from "lucide-react";
import {
  ResourcePanel,
  AdvancedDetails,
} from "@/components/common/resource-panel";
import { DetailRow } from "@/components/common/detail-row";
import { TaskLink } from "@/components/common/task-link";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useRunnerCheck } from "./use-runner-check";

const stages = [
  ["github", "GitHub network access"],
  ["rootless_docker", "Rootless Docker daemon"],
  ["gp_api", "Private GP API"],
  ["registry", "Registry authentication"],
] as const;
const failures: Record<string, string> = {
  connection:
    "Cannot connect. Check Runner networking and the endpoint's availability.",
  timeout: "The connection did not finish within its time limit.",
  canceled: "The check was canceled.",
  tls: "TLS validation failed. Check the endpoint certificate and Runner trust bundle.",
  authentication: "The endpoint rejected the Runner's configured credentials.",
  permission: "The endpoint denied this read request.",
  http_response: "The endpoint returned an unexpected HTTP status.",
  protocol: "The endpoint did not return the expected API response.",
  not_rootless: "The selected Docker daemon is not rootless.",
  credentials_unavailable:
    "The Runner's registry credentials or private CA could not be read.",
  interrupted:
    "Execution was interrupted. Start a new check; GP does not replay it.",
  runtime_changed:
    "Runner runtime changed during the check. Start a new check against its current runtime.",
  execution_failed:
    "The Runner helper could not complete. Check runtime status and the installed Runner image.",
  journal: "GP could not persist a complete result. Start a new check.",
};

export function RunnerCheckPanel({
  runnerId,
  taskId,
  open,
  disabled = false,
}: {
  runnerId: string;
  taskId?: string;
  open: boolean;
  disabled?: boolean;
}) {
  const check = useRunnerCheck(runnerId, open, taskId);
  const result = check.result;
  return (
    <ResourcePanel
      title="Connection check"
      actions={
        !taskId && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={
              check.sending ||
              check.loading ||
              (!check.uncertain && (disabled || check.active))
            }
            onClick={() => void check.run()}
          >
            {check.sending ? (
              <LoaderCircle className="size-3.5 animate-spin" />
            ) : (
              <PlugZap className="size-3.5" />
            )}
            {check.uncertain ? "Resolve submission" : "Test connections"}
          </Button>
        )
      }
    >
      {!taskId && (
        <p className="text-xs text-muted-foreground">
          Runs read-only checks inside the registered Runner. Does not register,
          restart, build or push.
        </p>
      )}
      {disabled && !taskId && (
        <p className="text-xs text-muted-foreground">
          Finish registration before testing connections.
        </p>
      )}
      {check.loading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading last check…
        </p>
      )}
      {check.uncertain && (
        <p role="status" className="text-xs text-muted-foreground">
          Acceptance is unknown. Resolve the same submission before starting
          another check.
        </p>
      )}
      {check.taskId && <TaskLink taskId={check.taskId} />}
      {check.error && (
        <p role="alert" className="text-xs text-destructive">
          {check.error}
        </p>
      )}
      {check.error && check.taskId && (
        <Button variant="outline" size="sm" onClick={check.refresh}>
          <RefreshCw className="size-3.5" /> Refresh result
        </Button>
      )}
      {result && (
        <>
          <div className="flex flex-wrap items-center gap-2">
            <Badge
              variant={
                result.stale
                  ? "warning"
                  : result.outcome === "passed"
                    ? "success"
                    : result.outcome
                      ? "danger"
                      : "muted"
              }
            >
              {result.stale
                ? "Result stale"
                : result.outcome === "passed"
                  ? "Connections verified"
                  : result.outcome === "failed"
                    ? "Check failed"
                    : result.outcome === "interrupted"
                      ? "Interrupted"
                      : "In progress"}
            </Badge>
            <span className="text-xs text-muted-foreground">
              {formatTimestamp(
                result.finished_at ?? result.created_at,
                "Not recorded",
              )}
            </span>
          </div>
          {result.stale && (
            <p className="text-xs text-muted-foreground">
              Runner configuration or runtime changed. This result does not
              verify the current setup.
            </p>
          )}
          {result.outcome === "interrupted" && (
            <p className="text-xs text-destructive">
              {failures[result.failure ?? "interrupted"]}
            </p>
          )}
          <ol className="space-y-3">
            {stages.map(([key, name]) => {
              const stage = result.stages.find((item) => item.name === key);
              const Icon =
                stage?.outcome === "passed"
                  ? Check
                  : stage?.outcome === "failed"
                    ? X
                    : Circle;
              return (
                <li key={key} className="space-y-1 text-xs">
                  <div className="flex items-center gap-2">
                    <Icon className="size-3.5 shrink-0" />
                    <span className="flex-1 font-medium">{name}</span>
                    <span className="text-muted-foreground">
                      {stage
                        ? `${stage.outcome === "passed" ? "Passed" : "Failed"} · ${stage.elapsed_ms} ms`
                        : result.outcome
                          ? "Not reached"
                          : "Waiting"}
                    </span>
                  </div>
                  {stage?.failure && (
                    <p className="pl-5 text-destructive">
                      {failures[stage.failure]}
                    </p>
                  )}
                  {!!stage?.http_status && (
                    <p className="pl-5 text-muted-foreground">
                      HTTP {stage.http_status}
                    </p>
                  )}
                </li>
              );
            })}
          </ol>
          <AdvancedDetails title="Execution details">
            <DetailRow
              label="Execution location"
              value={result.execution_location}
            />
            <DetailRow label="GitHub" value={result.configuration.github_url} />
            <DetailRow
              label="GP API"
              value={result.configuration.controller_endpoint}
            />
            <DetailRow
              label="Runner image"
              value={
                <span className="break-all">
                  {result.configuration.image_reference}
                </span>
              }
            />
            <DetailRow
              label="Runtime generation"
              value={result.configuration.runtime_epoch}
            />
            <DetailRow
              label="Runner revision"
              value={result.configuration.runner_revision}
            />
          </AdvancedDetails>
          <p className="text-xs text-muted-foreground">
            GitHub verifies a network path; Registry verifies authenticated read
            access. Passing does not certify registration, push permissions or a
            CI/CD deployment.
          </p>
        </>
      )}
    </ResourcePanel>
  );
}
