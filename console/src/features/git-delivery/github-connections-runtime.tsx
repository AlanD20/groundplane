import { sha256 } from "@noble/hashes/sha2.js";
import { bytesToHex } from "@noble/hashes/utils.js";
import {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  type FormEvent,
} from "react";
import { EmptyState } from "@/components/common/empty-state";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import {
  createConnection,
  positiveInteger,
  readConnection,
  readConnections,
  revokeConnection,
  rotateConnection,
  type Connection,
  type ConnectionCredentials,
  type ConnectionWrite,
  type Scope,
} from "./api-runtime";
import { useGitDeliveryReplay } from "./replay-runtime";
import { RepositoryDefaultsRuntime } from "./repository-defaults-runtime";

type Props = {
  scope: Scope;
  scopeName: string;
  ancestorScopes?: readonly Scope[];
};

const noScopes: readonly Scope[] = [];

type Action =
  { kind: "create" } | { kind: "rotate" | "revoke"; connection: Connection };

function describe(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function value(form: FormData, name: string) {
  return String(form.get(name) ?? "");
}

async function credentials(form: FormData): Promise<ConnectionCredentials> {
  const file = form.get("key");
  if (!(file instanceof File) || !file.size || file.size > 16_384) {
    throw new Error("Choose a GitHub App private key file up to 16 KiB.");
  }
  return {
    private_key_pem: await file.text(),
    webhook_secret: value(form, "webhook") || undefined,
  };
}

export function GitHubConnectionsRuntime(props: Props) {
  return (
    <ScopedGitHubConnections
      key={`${props.scope.kind}/${props.scope.id}`}
      {...props}
    />
  );
}

function ScopedGitHubConnections({
  scope,
  scopeName,
  ancestorScopes = noScopes,
}: Props) {
  const { kind, id } = scope;
  const [items, setItems] = useState<Connection[]>();
  const [readError, setReadError] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [action, setAction] = useState<Action>();
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");
  const request = useRef<AbortController | null>(null);
  const replay = useGitDeliveryReplay();
  const refresh = useCallback(async () => {
    request.current?.abort();
    const controller = new AbortController();
    request.current = controller;
    const { signal } = controller;
    setRefreshing(true);
    try {
      const records = await readConnections({ kind, id }, signal);
      if (signal.aborted) return;
      setItems(
        records.sort(
          (a, b) => a.name.localeCompare(b.name) || a.id.localeCompare(b.id),
        ),
      );
      setReadError("");
    } catch (error) {
      if (!signal.aborted)
        setReadError(describe(error, "Could not load GitHub connections."));
    } finally {
      if (!signal.aborted) setRefreshing(false);
    }
  }, [kind, id]);

  useEffect(() => {
    void refresh();
    return () => request.current?.abort();
  }, [refresh]);

  function finished(message: string) {
    setNotice(message);
    setAction(undefined);
    void refresh();
  }

  function toggle(next: Action) {
    setNotice("");
    setAction((previous) =>
      previous?.kind === next.kind &&
      (next.kind === "create" ||
        ("connection" in previous &&
          previous.connection.id === next.connection.id))
        ? undefined
        : next,
    );
  }

  return (
    <>
      <ResourcePanel
        title="GitHub connections"
        actions={
          <>
            <Button
              type="button"
              size="sm"
              variant="outline"
              disabled={refreshing}
              onClick={() => void refresh()}
            >
              {refreshing ? "Refreshing…" : "Refresh"}
            </Button>
            <Button
              type="button"
              size="sm"
              variant={action?.kind === "create" ? "secondary" : "default"}
              aria-expanded={action?.kind === "create"}
              disabled={
                busy ||
                Boolean(action && action.kind !== "create") ||
                replay.pending("create")
              }
              onClick={() => toggle({ kind: "create" })}
            >
              Connect GitHub App
            </Button>
          </>
        }
      >
        <p className="text-sm text-muted-foreground">
          Services within {scopeName} can use the repositories available to
          these GitHub App installations.
        </p>
        {notice && (
          <p role="status" className="text-sm text-success">
            {notice}
          </p>
        )}
        {readError && (
          <p role="alert" className="text-sm text-destructive">
            {readError}
          </p>
        )}
        {action?.kind === "create" && (
          <ConnectionEditor
            action={action}
            scope={{ kind, id }}
            replay={replay}
            onBusy={setBusy}
            onCancel={() => setAction(undefined)}
            onDone={finished}
          />
        )}
        {!items && !readError && (
          <EmptyState embedded loading title="Loading GitHub connections…" />
        )}
        {items?.length === 0 && !action && (
          <EmptyState embedded title="No GitHub App connections" />
        )}
        {items && items.length > 0 && (
          <ul className="divide-y divide-border rounded-lg border border-border">
            {items.map((item) => {
              const isOpen =
                action &&
                action.kind !== "create" &&
                action.connection.id === item.id;
              return (
                <li key={item.id} className="space-y-3 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        <p className="break-words text-sm font-medium">
                          {item.name}
                        </p>
                        <Badge variant={item.revoked_at ? "muted" : "success"}>
                          {item.revoked_at ? "Revoked" : "Registered"}
                        </Badge>
                      </div>
                      <p className="mt-1 text-xs text-muted-foreground">
                        App {item.app_id} · Installation {item.installation_id}
                      </p>
                    </div>
                    {!item.revoked_at && (
                      <div className="flex flex-wrap gap-2">
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          aria-expanded={Boolean(
                            isOpen && action.kind === "rotate",
                          )}
                          disabled={
                            busy ||
                            Boolean(
                              action && (!isOpen || action.kind !== "rotate"),
                            ) ||
                            replay.pending(`rotate:${item.id}`)
                          }
                          onClick={() =>
                            toggle({ kind: "rotate", connection: item })
                          }
                        >
                          Replace credentials
                        </Button>
                        <Button
                          type="button"
                          size="sm"
                          variant="outline"
                          aria-expanded={Boolean(
                            isOpen && action.kind === "revoke",
                          )}
                          disabled={
                            busy ||
                            Boolean(
                              action && (!isOpen || action.kind !== "revoke"),
                            ) ||
                            replay.pending(`revoke:${item.id}`)
                          }
                          onClick={() =>
                            toggle({ kind: "revoke", connection: item })
                          }
                        >
                          Revoke
                        </Button>
                      </div>
                    )}
                  </div>
                  {isOpen && (
                    <ConnectionEditor
                      action={action}
                      scope={{ kind, id }}
                      replay={replay}
                      onBusy={setBusy}
                      onCancel={() => setAction(undefined)}
                      onDone={finished}
                    />
                  )}
                </li>
              );
            })}
          </ul>
        )}
      </ResourcePanel>
      <RepositoryDefaultsRuntime
        scope={scope}
        scopeName={scopeName}
        connections={items}
        connectionsLoading={refreshing && !items}
        ancestorScopes={ancestorScopes}
      />
    </>
  );
}

function ConnectionEditor({
  action,
  scope,
  replay,
  onBusy,
  onCancel,
  onDone,
}: {
  action: Action;
  scope: Scope;
  replay: ReturnType<typeof useGitDeliveryReplay>;
  onBusy: (busy: boolean) => void;
  onCancel: () => void;
  onDone: (notice: string) => void;
}) {
  const field = useId();
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  const [revision, setRevision] = useState("");
  const [checking, setChecking] = useState(action.kind !== "create");
  const [revisionAttempt, setRevisionAttempt] = useState(0);
  const submission = useRef(false);
  const connection = "connection" in action ? action.connection : undefined;
  const actionKey = connection
    ? `${action.kind}:${connection.id}`
    : action.kind;

  useEffect(() => {
    if (!connection) return;
    const controller = new AbortController();
    const { signal } = controller;
    void readConnection(connection, signal).then(
      (current) => {
        if (signal.aborted) return;
        if (current.item.revoked_at) {
          setError("This GitHub App connection has been revoked.");
        } else {
          setRevision(current.revision);
        }
        setChecking(false);
      },
      (cause) => {
        if (signal.aborted) return;
        setError(
          describe(cause, "Could not check the GitHub connection revision."),
        );
        setChecking(false);
      },
    );
    return () => controller.abort();
  }, [connection, revisionAttempt]);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (submission.current || (connection && !revision)) return;
    const form = new FormData(event.currentTarget);
    submission.current = true;
    onBusy(true);
    setSaving(true);
    setError("");
    try {
      if (action.kind === "create") {
        const body: ConnectionWrite = {
          scope,
          name: value(form, "name").trim(),
          app_id: positiveInteger(value(form, "app"), "App ID"),
          installation_id: positiveInteger(
            value(form, "installation"),
            "Installation ID",
          ),
          ...(await credentials(form)),
        };
        const fingerprint = bytesToHex(
          sha256(new TextEncoder().encode(JSON.stringify(body))),
        );
        await replay.write(actionKey, fingerprint, (key) =>
          createConnection(body, key),
        );
        onDone("GitHub App connected.");
      } else if (action.kind === "rotate") {
        const body = await credentials(form);
        const fingerprint = bytesToHex(
          sha256(new TextEncoder().encode(JSON.stringify(body))),
        );
        await replay.write(actionKey, fingerprint, (key) =>
          rotateConnection(action.connection, body, revision, key),
        );
        onDone("GitHub App credentials replaced.");
      } else {
        await replay.write(actionKey, revision, (key) =>
          revokeConnection(action.connection, revision, key),
        );
        onDone("GitHub App connection revoked.");
      }
    } catch (cause) {
      setError(describe(cause, "Could not update the GitHub App connection."));
      if (
        connection &&
        cause instanceof ControllerRequestError &&
        [409, 412].includes(cause.status)
      ) {
        setRevision("");
      }
    } finally {
      submission.current = false;
      onBusy(false);
      setSaving(false);
    }
  }

  return (
    <InlineEditorRegion editing>
      <form
        className="space-y-4 rounded-lg border border-border bg-surface p-4"
        onSubmit={(event) => void submit(event)}
      >
        {action.kind === "create" ? (
          <>
            <h3 className="text-sm font-medium">Connect a GitHub App</h3>
            <p className="text-sm text-muted-foreground">
              Install the App on the repositories you want to deploy, then enter
              the App and installation IDs from GitHub.
            </p>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2 sm:col-span-2">
                <Label htmlFor={`${field}-name`}>Connection name</Label>
                <Input
                  id={`${field}-name`}
                  name="name"
                  required
                  maxLength={100}
                  disabled={saving}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor={`${field}-app`}>App ID</Label>
                <Input
                  id={`${field}-app`}
                  name="app"
                  inputMode="numeric"
                  pattern="[1-9][0-9]*"
                  required
                  disabled={saving}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor={`${field}-installation`}>Installation ID</Label>
                <Input
                  id={`${field}-installation`}
                  name="installation"
                  inputMode="numeric"
                  pattern="[1-9][0-9]*"
                  required
                  disabled={saving}
                />
              </div>
            </div>
          </>
        ) : (
          <h3 className="text-sm font-medium">
            {action.kind === "revoke" ? "Revoke" : "Replace credentials for"}{" "}
            {action.connection.name}
          </h3>
        )}
        {action.kind === "revoke" ? (
          <p className="text-sm text-muted-foreground">
            Groundplane removes the credentials for this connection. Services
            that use it will need another connection for future GitHub
            deployments.
          </p>
        ) : (
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor={`${field}-key`}>Private key file</Label>
              <Input
                id={`${field}-key`}
                name="key"
                type="file"
                accept=".pem"
                required
                disabled={saving}
                className="py-2"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor={`${field}-hook`}>Webhook secret (optional)</Label>
              <Input
                id={`${field}-hook`}
                name="webhook"
                type="password"
                autoComplete="new-password"
                minLength={32}
                maxLength={256}
                disabled={saving}
                aria-describedby={
                  action.kind === "rotate" ? `${field}-notice` : undefined
                }
              />
              {action.kind === "rotate" && (
                <p
                  id={`${field}-notice`}
                  className="text-xs text-muted-foreground"
                >
                  Enter the secret to use with GitHub webhooks. A blank value
                  removes the existing secret.
                </p>
              )}
            </div>
          </div>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <div className="flex flex-wrap justify-end gap-2">
          {connection && !checking && !revision && (
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => {
                setChecking(true);
                setError("");
                setRevisionAttempt((attempt) => attempt + 1);
              }}
            >
              Check again
            </Button>
          )}
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={onCancel}
            disabled={saving || replay.pending(actionKey)}
          >
            Cancel
          </Button>
          <Button
            type="submit"
            variant={action.kind === "revoke" ? "destructive" : "default"}
            size="sm"
            disabled={saving || Boolean(connection && !revision)}
          >
            {saving
              ? "Submitting…"
              : checking
                ? "Checking connection…"
                : replay.pending(actionKey)
                  ? "Retry request"
                  : {
                      create: "Connect",
                      rotate: "Replace credentials",
                      revoke: "Revoke connection",
                    }[action.kind]}
          </Button>
        </div>
      </form>
    </InlineEditorRegion>
  );
}
