import { useState } from "react";
import { LoaderCircle, Network, RefreshCw } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  AdvancedDetails,
  ResourcePanel,
} from "@/components/common/resource-panel";
import { DetailRow } from "@/components/common/detail-row";
import { CompactReference } from "@/components/common/compact-reference";
import { TaskLink } from "@/components/common/task-link";
import { DNSCheckAnswers } from "./dns-check-answers";
import { DNSCheckQueries } from "./dns-check-queries";
import { useDNSCheck } from "./use-dns-check";
import type { DNSCheckQuery } from "./dns-check-api";

const failures: Record<string, string> = {
  dns_response:
    "One or more queries did not resolve. See each server's response below.",
  runtime_unavailable:
    "The captured CoreDNS container is not serving. Check its current runtime before starting another check.",
  configuration_changed:
    "CoreDNS changed during this check. Start a new check against the current applied configuration.",
  query_selection_invalid:
    "A query does not match its applied forwarding domain. Choose a name within that domain, or use the default queries.",
  execution_failed:
    "The Agent could not complete the DNS queries. Start a new check after checking its status.",
  agent_unavailable:
    "The assigned Agent is disconnected or no longer owns this check.",
  interrupted:
    "The check was interrupted. GP will not rerun queries automatically; start a new check.",
  canceled: "The check was canceled.",
  timeout: "The check reached its time limit.",
  journal: "GP could not persist the DNS result.",
};

export function DNSCheckPanel({
  componentId,
  taskId,
  open,
  domains = [],
  disabled = false,
}: {
  componentId: string;
  taskId?: string;
  open: boolean;
  domains?: string[];
  disabled?: boolean;
}) {
  const [queries, setQueries] = useState<DNSCheckQuery[]>([]);
  const check = useDNSCheck(componentId, open, taskId);
  const result = check.result;
  const locked =
    disabled ||
    check.sending ||
    check.loading ||
    check.active ||
    check.uncertain;
  return (
    <ResourcePanel
      title="DNS connection check"
      actions={
        !taskId && (
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={
              disabled || check.sending || check.loading || check.active
            }
            onClick={() => void check.run(queries)}
          >
            {check.sending ? (
              <LoaderCircle className="size-3.5 animate-spin" />
            ) : (
              <Network className="size-3.5" />
            )}
            {check.uncertain ? "Resolve submission" : "Test DNS"}
          </Button>
        )
      }
    >
      {!taskId && (
        <>
          <p className="text-xs text-muted-foreground">
            Queries the applied CoreDNS resolver and each configured upstream.
            Does not save, reload or repair configuration.
          </p>
          {disabled && (
            <p className="text-xs text-muted-foreground">
              Enable CoreDNS and finish its configuration Task before testing.
            </p>
          )}
          <DNSCheckQueries
            domains={domains}
            queries={queries}
            onChange={setQueries}
            disabled={locked}
          />
        </>
      )}
      {check.loading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading last DNS check…
        </p>
      )}
      {check.taskId && <TaskLink taskId={check.taskId} />}
      {check.uncertain && (
        <p role="status" className="text-xs text-warning">
          Submission was not confirmed. Resolve submission uses the same
          captured request; it will not launch a second check.
        </p>
      )}
      {check.error && (
        <div role="alert" className="space-y-2 text-xs text-destructive">
          <p>{check.error}</p>
          {check.taskId && (
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={check.refresh}
            >
              <RefreshCw className="size-3.5" />
              Refresh result
            </Button>
          )}
        </div>
      )}
      {!check.taskId && !check.loading && (
        <p className="text-xs text-muted-foreground">
          No retained DNS check. Testing is explicit; opening this page never
          runs queries.
        </p>
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
                  ? "Queries resolved"
                  : result.outcome === "interrupted"
                    ? "Interrupted"
                    : result.outcome === "failed"
                      ? "Check failed"
                      : result.phase === "ready"
                        ? "Queued"
                        : "Checking DNS"}
            </Badge>
            <time
              className="text-xs text-muted-foreground"
              dateTime={
                result.observed_at ?? result.finished_at ?? result.created_at
              }
            >
              {new Date(
                result.observed_at ?? result.finished_at ?? result.created_at,
              ).toLocaleString()}
            </time>
          </div>
          {result.stale && (
            <p className="text-xs text-muted-foreground">
              Configuration or Agent state changed. This result does not verify
              the current resolver.
            </p>
          )}
          {result.failure && (
            <p className="text-xs text-destructive">
              {failures[result.failure] ??
                `Check failed (${result.failure}). Open the Task for execution details.`}
            </p>
          )}
          {!!result.answers?.length && (
            <DNSCheckAnswers answers={result.answers} taskId={result.task_id} />
          )}
          {!result.answers?.length && (
            <p className="text-xs text-muted-foreground">
              {result.phase === "finished"
                ? "No query responses were recorded."
                : "Waiting for the Agent's bounded DNS queries."}
            </p>
          )}
          <AdvancedDetails title="Applied configuration checked">
            <DetailRow
              label="Execution location"
              value={result.execution_location}
            />
            <DetailRow
              label="Component revision"
              value={result.configuration.component_revision}
            />
            <DetailRow
              label="Applied configuration Task"
              value={<TaskLink taskId={result.configuration.applied_task_id} />}
            />
            <DetailRow
              label="Render generation"
              value={result.configuration.render_generation}
            />
            <DetailRow
              label="Agent"
              value={
                <CompactReference
                  value={result.configuration.agent_id}
                  label="Agent ID"
                />
              }
            />
            <DetailRow
              label="Agent generation"
              value={result.configuration.agent_generation}
            />
            <DetailRow
              label="Image"
              value={
                <CompactReference
                  value={result.configuration.image_reference}
                  label="CoreDNS image"
                />
              }
            />
            <DetailRow
              label="Configuration digest"
              value={
                <CompactReference
                  value={result.configuration.artifact_sha256}
                  label="Configuration digest"
                />
              }
            />
            <DetailRow
              label="Query overrides"
              value={
                result.configuration.queries?.length
                  ? result.configuration.queries
                      .map(
                        (query) =>
                          `${query.name} (${query.type}) via ${query.domain}`,
                      )
                      .join(", ")
                  : "Default queries"
              }
            />
          </AdvancedDetails>
          <p className="text-xs text-muted-foreground">
            A recorded response proves this query at that time, not permanent
            availability. NXDOMAIN and no-answer responses are shown separately
            from connection failures.
          </p>
        </>
      )}
    </ResourcePanel>
  );
}
