import { useRef, useState } from "react";
import { GitBranch, RefreshCw } from "lucide-react";
import { EmptyState } from "@/components/common/empty-state";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { BindingEditor } from "./binding-editor";
import { DeploymentHistory } from "./deployment-history";
import type {
  GitDeliveryBinding,
  GitDeliveryPanelProps,
  GitRunnerChoice,
} from "./types";

export type { GitDeliveryPanelProps } from "./types";

function BindingSummary({
  binding,
  repositoryName,
  effectiveRunner,
}: {
  binding: GitDeliveryBinding;
  repositoryName?: string;
  effectiveRunner?: GitRunnerChoice;
}) {
  const values = [
    ["Repository", repositoryName ?? binding.repositoryId],
    ["Source branch", binding.branch],
    ["Workflow", `${binding.workflowId} · ${binding.workflowRef}`],
    ["Dockerfile", binding.dockerfile],
    ["Build context", binding.buildContext],
    [
      "Runner",
      binding.inheritRunnerDefault
        ? effectiveRunner
          ? `${effectiveRunner.label} · ${effectiveRunner.scopeKind} repository default`
          : "Repository default"
        : binding.runnerLabel,
    ],
    ["Trigger", binding.automatic ? "Push to source branch" : "Manual deploy"],
    [
      "Approval",
      binding.requireApproval ? "Required after build" : "Deploy after build",
    ],
    ...(effectiveRunner?.group || binding.runnerGroup
      ? [["Runner group", effectiveRunner?.group || binding.runnerGroup]]
      : []),
  ];
  return (
    <dl className="grid min-w-0 gap-x-5 gap-y-4 sm:grid-cols-2 lg:grid-cols-3">
      {values.map(([label, value]) => (
        <div key={label} className="min-w-0">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className="mt-1 break-words text-sm">{value}</dd>
        </div>
      ))}
    </dl>
  );
}

type Pending = {
  kind: "deploy" | "connect" | "refresh" | "approve" | "older";
  id?: string;
};

/** Service workspace surface. The route owns permissions, query state and mutations. */
export function GitDeliveryPanel(props: GitDeliveryPanelProps) {
  const {
    serviceName,
    environmentName,
    binding,
    connections,
    repositories,
    deployments,
    loading,
    error,
    canDeploy,
    deployUnavailableReason,
    onConnect,
    onRefresh,
    onBeginEdit,
    onLoadOlder,
    loadingOlder,
    onDeploy,
    onApprove,
  } = props;
  const [editing, setEditing] = useState(false);
  const [pending, setPending] = useState<Pending>();
  const [actionError, setActionError] = useState("");
  const pendingRequest = useRef(false);
  function beginEdit() {
    onBeginEdit?.();
    setEditing(true);
  }
  const connected = connections.some(
    (connection) => connection.status === "connected",
  );
  const savedConnection = connections.find(
    (connection) => connection.id === binding?.connectionId,
  );
  const repository = repositories?.find(
    (item) =>
      item.id === binding?.repositoryId &&
      item.connectionId === binding.connectionId,
  );
  const deployable = Boolean(
    binding &&
    savedConnection?.status === "connected" &&
    canDeploy === true &&
    !loading &&
    !error &&
    !editing,
  );

  async function invoke(
    action: Pending,
    effect: () => void | Promise<void>,
    fallback: string,
  ) {
    if (pendingRequest.current) return;
    pendingRequest.current = true;
    setPending(action);
    setActionError("");
    try {
      await effect();
    } catch (cause) {
      setActionError(cause instanceof Error ? cause.message : fallback);
    } finally {
      pendingRequest.current = false;
      setPending(undefined);
    }
  }

  const refresh = onRefresh
    ? () =>
        void invoke(
          { kind: "refresh" },
          onRefresh,
          "Could not refresh GitHub deployments.",
        )
    : undefined;

  return (
    <div className="space-y-4">
      <ResourcePanel
        title="GitHub deployment"
        actions={
          <>
            {refresh && (
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={Boolean(pending) || loading}
                onClick={refresh}
              >
                <RefreshCw aria-hidden className="size-4" />
                {pending?.kind === "refresh" ? "Refreshing…" : "Refresh"}
              </Button>
            )}
            {!editing && binding && (
              <>
                {onConnect && savedConnection?.status !== "connected" && (
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={Boolean(pending)}
                    onClick={() =>
                      void invoke(
                        { kind: "connect" },
                        onConnect,
                        "Could not open GitHub connections.",
                      )
                    }
                  >
                    Manage GitHub connections
                  </Button>
                )}
                {connected && (
                  <Button
                    type="button"
                    size="sm"
                    variant="outline"
                    disabled={Boolean(pending)}
                    onClick={beginEdit}
                  >
                    Edit setup
                  </Button>
                )}
                <Button
                  type="button"
                  size="sm"
                  disabled={!deployable || Boolean(pending)}
                  onClick={() =>
                    void invoke(
                      { kind: "deploy" },
                      onDeploy,
                      "Could not start the GitHub deployment.",
                    )
                  }
                >
                  {pending?.kind === "deploy"
                    ? "Starting…"
                    : "Deploy from GitHub"}
                </Button>
              </>
            )}
          </>
        }
      >
        <p className="text-sm text-muted-foreground">
          Builds deploy to{" "}
          <strong className="text-foreground">{serviceName}</strong> in{" "}
          <strong className="text-foreground">{environmentName}</strong>.
        </p>
        {actionError && (
          <p role="alert" className="text-sm text-destructive">
            {actionError}
          </p>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        {loading && !binding && !editing ? (
          <EmptyState embedded loading title="Loading GitHub configuration…" />
        ) : editing ? (
          <InlineEditorRegion editing>
            <BindingEditor
              {...props}
              onSave={async (next) => {
                await props.onSave(next);
                setEditing(false);
              }}
              onCancel={() => setEditing(false)}
            />
          </InlineEditorRegion>
        ) : binding ? (
          <>
            <BindingSummary
              binding={binding}
              repositoryName={repository?.name}
              effectiveRunner={props.effectiveRunner}
            />
            {savedConnection?.status !== "connected" ? (
              <p role="status" className="text-sm text-muted-foreground">
                Restore access to the saved GitHub connection before deploying.
              </p>
            ) : !deployable && !loading ? (
              <p role="status" className="text-sm text-muted-foreground">
                {deployUnavailableReason ||
                  "Deployment readiness is unavailable."}
              </p>
            ) : null}
          </>
        ) : !error ? (
          <EmptyState
            embedded
            icon={<GitBranch aria-hidden />}
            title="Set up GitHub deployment"
            description={
              connected
                ? "Choose a repository, branch, and Runner for this Service."
                : "Install a GitHub App with access to the repository you want to deploy."
            }
            action={
              connected ? (
                <Button type="button" onClick={beginEdit}>
                  Configure deployment
                </Button>
              ) : onConnect ? (
                <Button
                  type="button"
                  disabled={Boolean(pending)}
                  onClick={() =>
                    void invoke(
                      { kind: "connect" },
                      onConnect,
                      "Could not connect GitHub.",
                    )
                  }
                >
                  {pending?.kind === "connect"
                    ? "Connecting…"
                    : "Connect GitHub"}
                </Button>
              ) : undefined
            }
          />
        ) : null}
      </ResourcePanel>
      {(binding || deployments.length > 0) && (
        <DeploymentHistory
          deployments={deployments}
          busy={Boolean(pending)}
          approvingId={pending?.kind === "approve" ? pending.id : undefined}
          loadingOlder={loadingOlder}
          onLoadOlder={
            onLoadOlder
              ? () =>
                  void invoke(
                    { kind: "older" },
                    onLoadOlder,
                    "Could not load older deployments.",
                  )
              : undefined
          }
          onApprove={(id) =>
            void invoke(
              { kind: "approve", id },
              () => onApprove(id),
              "Could not approve the deployment.",
            )
          }
        />
      )}
    </div>
  );
}
