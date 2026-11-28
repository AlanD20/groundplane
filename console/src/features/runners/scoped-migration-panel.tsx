import { useEffect, useId, useRef, useState } from "react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { requestTask } from "@/features/task/api";
import type { Runner } from "@/lib/types";
import {
  positiveInteger,
  readConnections,
  readRepositories,
  type Connection,
  type Scope,
} from "@/features/git-delivery/api-runtime";
import type { GitHubRepository } from "@/features/git-delivery/types";
import {
  pendingScopedMigration,
  readScopedMigration,
  releaseScopedMigration,
  scopedMigrationSubmission,
  startScopedMigration,
  type MigrationObservation,
} from "./scoped-migration-api";

const phases = {
  legacy: "Legacy access",
  registration: "Awaiting GitHub verification",
  pending: "Migration requested",
  cutover: "Scoped access cutover",
  complete: "Scoped access enabled",
};

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

export function ScopedMigrationPanel({ runner }: { runner: Runner }) {
  const id = useId();
  const pending = pendingScopedMigration(runner.id);
  const [observation, setObservation] = useState<MigrationObservation>();
  const [observing, setObserving] = useState(true);
  const [readError, setReadError] = useState("");
  const [epoch, setEpoch] = useState(0);
  const [editing, setEditing] = useState(Boolean(pending));
  const [observedRevision, setObservedRevision] = useState(
    pending?.revision ?? "",
  );
  const [busy, setBusy] = useState(false);
  const [writeError, setWriteError] = useState("");
  const submitInProgress = useRef(false);
  const [taskId, setTaskId] = useState<string>();
  const [taskNeedsReview, setTaskNeedsReview] = useState(false);
  const [connections, setConnections] = useState<Connection[]>();
  const [connectionId, setConnectionId] = useState(
    pending?.body.connection_id ?? "",
  );
  const [connectionsError, setConnectionsError] = useState("");
  const [connectionsEpoch, setConnectionsEpoch] = useState(0);
  const [repositories, setRepositories] = useState<GitHubRepository[]>();
  const [repositoryId, setRepositoryId] = useState(
    pending ? String(pending.body.repository_id) : "",
  );
  const [repositoriesError, setRepositoriesError] = useState("");
  const [repositoriesEpoch, setRepositoriesEpoch] = useState(0);
  const connection = connections?.find((item) => item.id === connectionId);
  const { environmentId, projectId, tenantId } = runner;

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    let timer: ReturnType<typeof setTimeout>;
    async function observe() {
      try {
        const next = await readScopedMigration(runner.id, signal);
        if (signal.aborted) return;
        setObservation(next);
        setReadError("");
        if (
          next.status.phase === "pending" ||
          next.status.phase === "cutover"
        ) {
          if (next.status.task_id) {
            const { task } = await requestTask(next.status.task_id, signal, {
              limit: 1,
            });
            if (signal.aborted) return;
            const stopped = Boolean(
              task.finished_at || task.status === "attention_required",
            );
            setTaskNeedsReview(stopped);
            if (stopped) return;
          }
          timer = setTimeout(observe, 5_000);
        }
      } catch (cause) {
        if (!signal.aborted)
          setReadError(
            errorMessage(cause, "Could not load Runner migration status."),
          );
      } finally {
        if (!signal.aborted) setObserving(false);
      }
    }
    void observe();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [runner.id, epoch]);

  useEffect(() => {
    if (!editing || pending) return;
    const controller = new AbortController();
    const { signal } = controller;
    const scopes: Scope[] = [{ kind: "tenant", id: tenantId }];
    if (projectId) scopes.unshift({ kind: "project", id: projectId });
    if (environmentId)
      scopes.unshift({ kind: "environment", id: environmentId });
    void Promise.all(
      scopes.map((scope) =>
        readConnections(scope, signal).catch((cause) => {
          if (cause instanceof ControllerRequestError && cause.status === 403)
            return [];
          throw cause;
        }),
      ),
    ).then(
      (lists) => {
        if (signal.aborted) return;
        setConnections(lists.flat().filter((item) => !item.revoked_at));
        setConnectionsError("");
      },
      (cause) => {
        if (!signal.aborted)
          setConnectionsError(
            errorMessage(cause, "Could not load GitHub connections."),
          );
      },
    );
    return () => controller.abort();
  }, [connectionsEpoch, editing, environmentId, pending, projectId, tenantId]);

  useEffect(() => {
    if (!editing || !connection || pending) return;
    const controller = new AbortController();
    const { signal } = controller;
    const registration = runner.githubUrl.toLowerCase();
    void readRepositories(connection, signal).then(
      (list) => {
        if (signal.aborted) return;
        setRepositories(
          list.filter(
            (repository) =>
              registration ===
                `https://github.com/${repository.name.toLowerCase()}` ||
              registration ===
                `https://github.com/${repository.name.split("/")[0].toLowerCase()}`,
          ),
        );
        setRepositoriesError("");
      },
      (cause) => {
        if (!signal.aborted)
          setRepositoriesError(
            errorMessage(cause, "Could not load repositories."),
          );
      },
    );
    return () => controller.abort();
  }, [connection, editing, pending, repositoriesEpoch, runner.githubUrl]);

  function refresh() {
    setObserving(true);
    setEpoch((current) => current + 1);
  }

  function toggleEditor() {
    if (!editing) {
      setObservedRevision(observation?.revision ?? "");
      setWriteError("");
    }
    setEditing(!editing);
  }

  async function submit() {
    if (
      submitInProgress.current ||
      (!pending &&
        (!available ||
          observing ||
          readError ||
          revisionChanged ||
          !connectionId ||
          !selectedRepository ||
          !observedRevision))
    )
      return;
    submitInProgress.current = true;
    setBusy(true);
    setWriteError("");
    try {
      const submission =
        pending ??
        scopedMigrationSubmission(runner.id, observedRevision, {
          connection_id: connectionId,
          repository_id: positiveInteger(repositoryId, "Repository ID"),
        });
      try {
        const acceptedTask = await startScopedMigration(runner.id, submission);
        releaseScopedMigration(runner.id, submission);
        setTaskId(acceptedTask);
        setEditing(false);
        refresh();
      } catch (cause) {
        if (
          cause instanceof ControllerRequestError &&
          cause.status >= 400 &&
          cause.status < 500 &&
          cause.status !== 408 &&
          cause.status !== 429 &&
          cause.code !== "idempotency.in_progress"
        ) {
          releaseScopedMigration(runner.id, submission);
          setObservedRevision("");
          refresh();
          throw cause;
        }
        throw new Error(
          "The Controller has yet to confirm the request. Retry to check its result with the original revision.",
          { cause },
        );
      }
    } catch (cause) {
      setWriteError(
        errorMessage(cause, "Could not start scoped Runner migration."),
      );
    } finally {
      setBusy(false);
      submitInProgress.current = false;
    }
  }

  const status = observation?.status;
  const requiresVerification =
    status?.phase === "legacy" || status?.phase === "registration";
  const available = Boolean(
    status?.available &&
    requiresVerification &&
    runner.lifecycle === "ready" &&
    !taskId,
  );
  const task = status?.task_id ?? taskId;
  const revisionChanged =
    observation && observedRevision !== observation.revision;
  const selectedRepository = repositories?.find(
    (item) => item.id === repositoryId,
  );
  return (
    <ResourcePanel
      title="Runner access"
      actions={
        <>
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={busy || observing}
            onClick={refresh}
          >
            {observing ? "Checking…" : "Refresh"}
          </Button>
          {available && (
            <Button
              type="button"
              size="sm"
              variant="outline"
              aria-expanded={editing}
              disabled={busy || Boolean(pending)}
              onClick={toggleEditor}
            >
              {status?.phase === "registration"
                ? "Verify GitHub registration"
                : "Move to scoped access"}
            </Button>
          )}
        </>
      }
    >
      <div className="flex flex-wrap items-center gap-3 text-sm" role="status">
        <span>
          {status
            ? phases[status.phase]
            : observing
              ? "Checking Runner access…"
              : "Runner access status unavailable"}
        </span>
        {task && <TaskLink taskId={task}>Open migration Task</TaskLink>}
      </div>
      {taskNeedsReview && status?.phase !== "complete" && (
        <p className="text-sm text-muted-foreground">
          Review the migration Task for its result and recovery actions.
        </p>
      )}
      {status?.phase === "pending" && !taskNeedsReview && (
        <p className="text-sm text-muted-foreground">
          The migration waits for active work to finish before switching access.
        </p>
      )}
      {status?.phase === "registration" && (
        <p className="text-sm text-muted-foreground">
          Groundplane API and registry access are disabled until verification
          completes.
        </p>
      )}
      {requiresVerification && !status?.available && (
        <p className="text-sm text-muted-foreground">
          Scoped migration is currently unavailable. The Runner must be
          registered and the migration image and scoped access services must be
          ready.
        </p>
      )}
      {readError && (
        <p role="alert" className="text-sm text-destructive">
          {readError}
        </p>
      )}
      {editing && (available || pending) && (
        <InlineEditorRegion editing>
          <form
            className="space-y-3 rounded-lg border border-border bg-surface p-4"
            onSubmit={(event) => {
              event.preventDefault();
              void submit();
            }}
          >
            <p className="text-sm text-muted-foreground">
              Groundplane waits for an active job to finish before switching
              credentials and network access. The Runner keeps its GitHub
              identity and workspace.
            </p>
            {pending ? (
              <p className="text-sm text-muted-foreground">
                Retry the original request for repository{" "}
                <span className="font-mono">{pending.body.repository_id}</span>.
              </p>
            ) : (
              <>
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor={`${id}-connection`}>
                      GitHub App connection
                    </Label>
                    <Select
                      id={`${id}-connection`}
                      value={connectionId || null}
                      disabled={busy || !connections?.length}
                      placeholder={
                        connections || connectionsError
                          ? "Select connection"
                          : "Loading connections…"
                      }
                      options={(connections ?? []).map((item) => ({
                        value: item.id,
                        label: `${item.name} · ${item.scope.kind}`,
                      }))}
                      onValueChange={(next) => {
                        setConnectionId(next);
                        setRepositoryId("");
                        setRepositories(undefined);
                        setRepositoriesError("");
                      }}
                    />
                    {connections?.length === 0 && (
                      <p className="text-xs text-muted-foreground">
                        Connect a GitHub App in Settings for this Runner’s scope
                        or a parent scope.
                      </p>
                    )}
                    {connectionsError && (
                      <div role="alert" className="space-y-2">
                        <p className="text-xs text-destructive">
                          {connectionsError}
                        </p>
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          onClick={() =>
                            setConnectionsEpoch((current) => current + 1)
                          }
                        >
                          Retry connections
                        </Button>
                      </div>
                    )}
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor={`${id}-repository`}>Repository</Label>
                    <Select
                      id={`${id}-repository`}
                      value={repositoryId || null}
                      disabled={busy || !repositories?.length}
                      placeholder={
                        connection && !repositories && !repositoriesError
                          ? "Loading repositories…"
                          : "Select repository"
                      }
                      options={(repositories ?? []).map((item) => ({
                        value: item.id,
                        label: item.name,
                      }))}
                      onValueChange={setRepositoryId}
                    />
                    {repositories?.length === 0 && (
                      <p className="text-xs text-muted-foreground">
                        This connection has no repositories matching the
                        Runner’s GitHub registration.
                      </p>
                    )}
                    {repositoriesError && (
                      <div role="alert" className="space-y-2">
                        <p className="text-xs text-destructive">
                          {repositoriesError}
                        </p>
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          onClick={() =>
                            setRepositoriesEpoch((current) => current + 1)
                          }
                        >
                          Retry repositories
                        </Button>
                      </div>
                    )}
                  </div>
                </div>
                {revisionChanged && status?.available && (
                  <div
                    className="flex flex-wrap items-center gap-2"
                    role="status"
                  >
                    <p className="text-sm text-muted-foreground">
                      Review the latest Runner revision before starting
                      migration.
                    </p>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={observing}
                      onClick={() => setObservedRevision(observation.revision)}
                    >
                      Use current revision
                    </Button>
                  </div>
                )}
              </>
            )}
            {writeError && (
              <p role="alert" className="text-sm text-destructive">
                {writeError}
              </p>
            )}
            <div className="flex flex-wrap justify-end gap-2">
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={busy || Boolean(pending)}
                onClick={toggleEditor}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                size="sm"
                disabled={
                  busy ||
                  (!pending &&
                    (observing ||
                      Boolean(readError) ||
                      !available ||
                      !selectedRepository ||
                      Boolean(revisionChanged)))
                }
              >
                {busy
                  ? "Submitting…"
                  : pending
                    ? "Retry request"
                    : "Migrate when idle"}
              </Button>
            </div>
          </form>
        </InlineEditorRegion>
      )}
    </ResourcePanel>
  );
}
