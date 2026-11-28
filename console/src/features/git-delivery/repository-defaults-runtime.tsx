import {
  useEffect,
  useEffectEvent,
  useId,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { EmptyState } from "@/components/common/empty-state";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import {
  positiveInteger,
  readConnections,
  readRepositories,
  readRunnerDefault,
  removeRunnerDefault,
  saveRunnerDefault,
  type Connection,
  type RunnerDefaultState,
  type RunnerDefaultWrite,
  type Scope,
} from "./api-runtime";
import { useGitDeliveryReplay } from "./replay-runtime";
import type { GitHubRepository } from "./types";

function describe(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

export function RepositoryDefaultsRuntime({
  scope,
  scopeName,
  connections,
  connectionsLoading,
  ancestorScopes,
}: {
  scope: Scope;
  scopeName: string;
  connections?: readonly Connection[];
  connectionsLoading: boolean;
  ancestorScopes: readonly Scope[];
}) {
  const id = useId();
  const [ancestorConnections, setAncestorConnections] =
    useState<Connection[]>();
  const [ancestorError, setAncestorError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const [connectionId, setConnectionId] = useState("");
  const [locked, setLocked] = useState(false);
  const ancestorKey = JSON.stringify(ancestorScopes);
  const readAncestors = useEffectEvent((signal: AbortSignal) =>
    Promise.all(
      ancestorScopes.map((ancestor) =>
        readConnections(ancestor, signal).catch((cause) => {
          if (cause instanceof ControllerRequestError && cause.status === 403)
            return [];
          throw cause;
        }),
      ),
    ),
  );
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    void readAncestors(signal).then(
      (lists) => {
        if (signal.aborted) return;
        setAncestorConnections(lists.flat());
        setAncestorError("");
      },
      (cause) => {
        if (!signal.aborted)
          setAncestorError(
            describe(cause, "Could not load inherited GitHub connections."),
          );
      },
    );
    return () => controller.abort();
  }, [ancestorKey, attempt]);
  const available = [
    ...(connections ?? []),
    ...(ancestorConnections ?? []),
  ].filter((connection) => !connection.revoked_at);
  const connection = available.find((item) => item.id === connectionId);
  return (
    <ResourcePanel title="Repository Runner defaults">
      <p className="text-sm text-muted-foreground">
        Save a Runner pool for a repository in {scopeName}. Service bindings can
        use the nearest default from their Environment, Project, or Tenant.
      </p>
      {ancestorError && (
        <div className="flex flex-wrap items-center gap-2" role="alert">
          <p className="text-sm text-destructive">{ancestorError}</p>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => setAttempt((current) => current + 1)}
          >
            Try again
          </Button>
        </div>
      )}
      {available.length === 0 &&
      (connectionsLoading ||
        (ancestorScopes.length > 0 &&
          !ancestorConnections &&
          !ancestorError)) ? (
        <EmptyState embedded loading title="Loading GitHub connections…" />
      ) : available.length === 0 ? (
        <EmptyState embedded title="No GitHub connections available" />
      ) : (
        <div className="space-y-2">
          <Label htmlFor={`${id}-connection`}>GitHub connection</Label>
          <Select
            id={`${id}-connection`}
            value={connectionId || null}
            placeholder="Select connection"
            disabled={locked}
            options={available.map((item) => ({
              value: item.id,
              label: `${item.name} · ${item.scope.kind}`,
            }))}
            onValueChange={setConnectionId}
          />
        </div>
      )}
      {connection && (
        <RepositoryPicker
          key={connection.id}
          scope={scope}
          connection={connection}
          onLock={setLocked}
          locked={locked}
        />
      )}
    </ResourcePanel>
  );
}

function RepositoryPicker({
  connection,
  scope,
  onLock,
  locked,
}: {
  connection: Connection;
  scope: Scope;
  onLock: (locked: boolean) => void;
  locked: boolean;
}) {
  const id = useId();
  const [repositories, setRepositories] = useState<GitHubRepository[]>();
  const [error, setError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const [repositoryId, setRepositoryId] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    void readRepositories(connection, signal).then(
      (items) => {
        if (signal.aborted) return;
        setRepositories(items);
        setError("");
      },
      (cause) => {
        if (!signal.aborted)
          setError(describe(cause, "Could not load repositories."));
      },
    );
    return () => controller.abort();
  }, [connection, attempt]);
  return (
    <>
      <div className="space-y-2">
        <Label htmlFor={`${id}-repository`}>Repository</Label>
        <Select
          id={`${id}-repository`}
          value={repositoryId || null}
          placeholder={
            !repositories && !error
              ? "Loading repositories…"
              : "Select repository"
          }
          disabled={locked || !repositories?.length}
          options={(repositories ?? []).map((item) => ({
            value: item.id,
            label: item.name,
          }))}
          onValueChange={setRepositoryId}
        />
        {error && (
          <div className="flex flex-wrap items-center gap-2" role="alert">
            <p className="text-sm text-destructive">{error}</p>
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => setAttempt((current) => current + 1)}
            >
              Try again
            </Button>
          </div>
        )}
        {repositories?.length === 0 && !error && (
          <p className="text-sm text-muted-foreground">
            No repositories are available to this GitHub App installation.
          </p>
        )}
      </div>
      {repositoryId && (
        <RepositoryDefaultEditor
          key={repositoryId}
          scope={scope}
          connectionId={connection.id}
          repositoryId={repositoryId}
          onLock={onLock}
        />
      )}
    </>
  );
}

function RepositoryDefaultEditor({
  scope: { kind, id },
  connectionId,
  repositoryId,
  onLock,
}: {
  scope: Scope;
  connectionId: string;
  repositoryId: string;
  onLock: (locked: boolean) => void;
}) {
  const field = useId();
  const [state, setState] = useState<RunnerDefaultState>();
  const [attempt, setAttempt] = useState(0);
  const [mode, setMode] = useState<"edit" | "remove">();
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");
  const [readError, setReadError] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const submitted = useRef(false);
  const lastRevision = useRef<number | undefined>(undefined);
  const replay = useGitDeliveryReplay();
  const uncertain = replay.pending("edit") || replay.pending("remove");
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    void readRunnerDefault(
      { kind, id },
      connectionId,
      repositoryId,
      signal,
    ).then(
      (next) => {
        if (signal.aborted) return;
        setState(next);
        if (!next.default)
          setMode((current) => (current === "remove" ? undefined : current));
        setReadError("");
        setRefreshing(false);
      },
      (cause) => {
        if (!signal.aborted) {
          setReadError(
            describe(cause, "Could not load the repository default."),
          );
          setRefreshing(false);
        }
      },
    );
    return () => controller.abort();
  }, [attempt, connectionId, id, kind, repositoryId]);
  useEffect(() => () => onLock(false), [onLock]);

  function refresh() {
    setRefreshing(true);
    setAttempt((current) => current + 1);
  }

  function selectMode(next?: "edit" | "remove") {
    lastRevision.current = state?.revision;
    setMode(next === mode ? undefined : next);
    setNotice("");
    setError("");
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!mode || !state || refreshing || readError || submitted.current) return;
    const form = new FormData(event.currentTarget);
    const revision = lastRevision.current ?? state.revision;
    lastRevision.current = revision;
    submitted.current = true;
    onLock(true);
    setBusy(true);
    setError("");
    try {
      const scope = { kind, id };
      if (mode === "remove") {
        await replay.write(mode, String(revision), (key) =>
          removeRunnerDefault(scope, connectionId, repositoryId, revision, key),
        );
        setState({ revision: 0 });
        setNotice("Repository Runner default removed.");
      } else {
        const group = String(form.get("group") ?? "").trim();
        const groupId = String(form.get("group-id") ?? "").trim();
        if (Boolean(group) !== Boolean(groupId))
          throw new Error(
            "Enter both the Runner group name and its GitHub ID.",
          );
        const body: RunnerDefaultWrite = {
          label: String(form.get("label") ?? "").trim(),
          group: group || undefined,
          group_id: groupId
            ? positiveInteger(groupId, "Runner group ID")
            : undefined,
        };
        const saved = await replay.write(mode, JSON.stringify(body), (key) =>
          saveRunnerDefault(
            scope,
            connectionId,
            repositoryId,
            body,
            revision,
            key,
          ),
        );
        setState(saved);
        setNotice("Repository Runner default saved.");
      }
      setMode(undefined);
    } catch (cause) {
      setError(describe(cause, "Could not update the repository default."));
      if (
        cause instanceof ControllerRequestError &&
        [409, 412].includes(cause.status)
      ) {
        lastRevision.current = undefined;
        refresh();
      }
    } finally {
      submitted.current = false;
      setBusy(false);
      onLock(replay.pending(mode));
    }
  }

  if (!state)
    return readError ? (
      <div role="alert" className="flex flex-wrap items-center gap-2">
        <p className="text-sm text-destructive">{readError}</p>
        <Button type="button" size="sm" variant="outline" onClick={refresh}>
          Try again
        </Button>
      </div>
    ) : (
      <p role="status" className="text-sm text-muted-foreground">
        Loading saved Runner default…
      </p>
    );
  const choice = state.default;
  return (
    <div className="space-y-3 border-t border-border pt-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <p className="text-sm">
          {choice
            ? `${choice.label}${choice.group ? ` · ${choice.group}` : ""}`
            : "No default at this scope"}
        </p>
        <div className="flex flex-wrap gap-2">
          <Button
            type="button"
            size="sm"
            variant="outline"
            aria-expanded={mode === "edit"}
            disabled={busy || uncertain}
            onClick={() => selectMode("edit")}
          >
            {choice ? "Edit default" : "Set default"}
          </Button>
          {choice && (
            <Button
              type="button"
              size="sm"
              variant="outline"
              aria-expanded={mode === "remove"}
              disabled={busy || uncertain}
              onClick={() => selectMode("remove")}
            >
              Remove default
            </Button>
          )}
        </div>
      </div>
      {notice && (
        <p role="status" className="text-sm text-success">
          {notice}
        </p>
      )}
      {readError && (
        <div role="alert" className="flex flex-wrap items-center gap-2">
          <p className="text-sm text-destructive">{readError}</p>
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={refreshing}
            onClick={refresh}
          >
            Try again
          </Button>
        </div>
      )}
      {mode && (
        <InlineEditorRegion editing>
          <form
            key={mode}
            className="space-y-3 rounded-lg border border-border bg-surface p-4"
            onSubmit={(event) => void submit(event)}
          >
            {mode === "remove" ? (
              <p className="text-sm text-muted-foreground">
                Services that inherit this default will look for a default in a
                broader scope. If none exists, they will need a Service override
                before their next GitHub deployment.
              </p>
            ) : (
              <div className="grid gap-3 lg:grid-cols-3">
                <div className="space-y-2">
                  <Label htmlFor={`${field}-label`}>Runner label</Label>
                  <Input
                    id={`${field}-label`}
                    name="label"
                    defaultValue={choice?.label}
                    required
                    maxLength={100}
                    pattern={"[A-Za-z0-9][A-Za-z0-9._\\-]*"}
                    disabled={busy}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor={`${field}-group`}>
                    Runner group (optional)
                  </Label>
                  <Input
                    id={`${field}-group`}
                    name="group"
                    defaultValue={choice?.group}
                    maxLength={100}
                    disabled={busy}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor={`${field}-group-id`}>
                    GitHub Runner group ID
                  </Label>
                  <Input
                    id={`${field}-group-id`}
                    name="group-id"
                    defaultValue={choice?.group_id}
                    inputMode="numeric"
                    pattern="[1-9][0-9]*"
                    disabled={busy}
                  />
                </div>
              </div>
            )}
            {error && (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            )}
            <div className="flex flex-wrap justify-end gap-2">
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={busy || uncertain}
                onClick={() => selectMode()}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                size="sm"
                variant={mode === "remove" ? "destructive" : "default"}
                disabled={busy || refreshing || Boolean(readError)}
              >
                {busy
                  ? "Submitting…"
                  : uncertain
                    ? "Retry request"
                    : mode === "remove"
                      ? "Remove default"
                      : "Save default"}
              </Button>
            </div>
          </form>
        </InlineEditorRegion>
      )}
    </div>
  );
}
