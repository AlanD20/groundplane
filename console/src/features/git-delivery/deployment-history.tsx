import { ArrowUpRight, GitCommitHorizontal } from "lucide-react";
import { useState } from "react";
import { CompactReference } from "@/components/common/compact-reference";
import { Disclosure } from "@/components/common/disclosure";
import { EmptyState } from "@/components/common/empty-state";
import { ResourcePanel } from "@/components/common/resource-panel";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { Button } from "@/components/ui/button";
import { formatTimestamp } from "@/lib/format-timestamp";
import { gitHubUrl } from "./github-url";
import type { GitDeployment, GitDeploymentPhase } from "./types";

const issueLabels: Record<string, string> = {
  intent_changed:
    "A newer deployment or a setup change superseded this request.",
  workflow_changed:
    "The workflow ref moved. Review the new workflow and save the deployment setup before trying again.",
  authorization_changed:
    "GitHub access or Runner eligibility changed during this deployment.",
  dispatch_uncertain: "Waiting for GitHub to confirm the workflow dispatch.",
  dispatch_unverifiable: "Groundplane could not verify the workflow dispatch.",
  dispatch_unconfirmed:
    "GitHub did not confirm the dispatch. Check the repository’s Actions history.",
  workflow_identity_mismatch:
    "The GitHub run did not match the reviewed workflow and commit.",
  workflow_unverifiable:
    "Groundplane could not verify the GitHub workflow run.",
  build_failed: "The GitHub build failed. Check its logs for details.",
  build_result_unverifiable:
    "Groundplane could not verify the GitHub build result.",
  build_result_mismatch:
    "The GitHub build result did not match the deployment request.",
  fetch_admission_blocked:
    "Groundplane could not confirm a task to fetch the image.",
  deploy_admission_blocked:
    "Groundplane could not start or confirm the deploy task.",
  deploy_admission_unverifiable:
    "Groundplane is still verifying whether the deploy task was accepted.",
  fetch_task_unverifiable:
    "Groundplane could not verify the fetch task result.",
  deploy_task_unverifiable:
    "Groundplane could not verify the deploy task result.",
  fetch_task_needs_attention: "The fetch task needs attention.",
  deploy_task_needs_attention: "The deploy task needs attention.",
  fetch_failed: "The fetch task failed. Open the task for details.",
  deploy_failed: "The deploy task failed. Open the task for details.",
};

const phaseDisplay: Record<
  GitDeploymentPhase,
  [status: string, label: string]
> = {
  queued: ["queued", "Queued"],
  dispatching: ["preparing", "Starting GitHub build"],
  building: ["running", "Building"],
  awaiting_approval: ["waiting", "Awaiting approval"],
  fetch_pending: ["waiting", "Waiting to fetch image"],
  fetching: ["running", "Fetching image"],
  deploy_pending: ["waiting", "Waiting to deploy"],
  deploying: ["running", "Deploying"],
  succeeded: ["succeeded", "Deployed"],
  failed: ["failed", "Failed"],
  superseded: ["superseded", "Superseded"],
  attention_required: ["attention_required", "Needs attention"],
};

function DeploymentRow({
  deployment,
  busy,
  approving,
  onApprove,
}: {
  deployment: GitDeployment;
  busy: boolean;
  approving: boolean;
  onApprove: () => void;
}) {
  const [status, label] = phaseDisplay[deployment.phase] ?? [
    "unknown",
    "Status unavailable",
  ];
  const runUrl = gitHubUrl(deployment.githubRunUrl);
  const [expanded, setExpanded] = useState<boolean>();
  return (
    <Disclosure
      className={workspaceSectionClassName(false)}
      summaryClassName="py-1"
      open={expanded ?? deployment.phase === "awaiting_approval"}
      onOpenChange={setExpanded}
      summary={
        <>
          <span className="flex min-w-0 flex-1 flex-col gap-1">
            <span className="flex min-w-0 items-center gap-2 text-sm font-medium">
              <GitCommitHorizontal
                aria-hidden
                className="size-4 shrink-0 text-muted-foreground"
              />
              <code className="truncate">
                {deployment.commit.slice(0, 12) || "Commit pending"}
              </code>
            </span>
            <span className="text-xs text-muted-foreground">
              {formatTimestamp(deployment.createdAt, "Time unavailable")}
            </span>
          </span>
          <StatusBadge status={status} label={label} className="shrink-0" />
        </>
      }
    >
      <div className="mt-3 space-y-3 border-t border-border pt-3">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          {runUrl && (
            <a
              href={runUrl}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-1 text-sm text-primary underline underline-offset-4"
            >
              Build on GitHub
              <ArrowUpRight aria-hidden className="size-3.5" />
            </a>
          )}
          {deployment.fetchTaskId && (
            <TaskLink taskId={deployment.fetchTaskId}>Fetch task</TaskLink>
          )}
          {deployment.deployTaskId && (
            <TaskLink taskId={deployment.deployTaskId}>Deploy task</TaskLink>
          )}
        </div>
        <div className="flex flex-wrap gap-x-5 gap-y-2">
          {deployment.commit && (
            <CompactReference label="Commit" value={deployment.commit} />
          )}
          {deployment.imageDigest && (
            <CompactReference
              label="Image digest"
              value={deployment.imageDigest}
            />
          )}
          {deployment.releaseId && (
            <CompactReference label="Release" value={deployment.releaseId} />
          )}
          {deployment.runner && (
            <span className="text-xs text-muted-foreground">
              Runner pool{" "}
              <span className="text-foreground">{deployment.runner}</span>
            </span>
          )}
          {deployment.githubRunnerId && (
            <CompactReference
              label="GitHub Runner ID"
              value={deployment.githubRunnerId}
            />
          )}
        </div>
        {deployment.issue && (
          <p className="text-sm text-destructive" title={deployment.issue}>
            {issueLabels[deployment.issue] ??
              "Groundplane reported a deployment issue."}
          </p>
        )}
        {deployment.phase === "awaiting_approval" && (
          <div className="flex flex-wrap items-center gap-3 rounded-lg border border-border p-3">
            <p className="min-w-0 flex-1 text-sm">
              The build is ready. Approve this commit to continue deployment.
            </p>
            <Button type="button" size="sm" disabled={busy} onClick={onApprove}>
              {approving ? "Approving…" : "Approve deployment"}
            </Button>
          </div>
        )}
      </div>
    </Disclosure>
  );
}

export function DeploymentHistory({
  deployments,
  busy,
  approvingId,
  onApprove,
  loadingOlder,
  onLoadOlder,
}: {
  deployments: readonly GitDeployment[];
  busy: boolean;
  approvingId?: string;
  onApprove: (id: string) => void;
  loadingOlder?: boolean;
  onLoadOlder?: () => void;
}) {
  return (
    <ResourcePanel
      title="GitHub deployment history"
      actions={
        onLoadOlder && (
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={busy || loadingOlder}
            onClick={onLoadOlder}
          >
            {loadingOlder ? "Loading…" : "Load older deployments"}
          </Button>
        )
      }
    >
      {deployments.length ? (
        <ul className="space-y-2">
          {deployments.map((deployment) => (
            <li key={deployment.id}>
              <DeploymentRow
                deployment={deployment}
                busy={busy}
                approving={deployment.id === approvingId}
                onApprove={() => onApprove(deployment.id)}
              />
            </li>
          ))}
        </ul>
      ) : (
        <EmptyState embedded title="No GitHub deployments" />
      )}
    </ResourcePanel>
  );
}
