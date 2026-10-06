"use client";

import { useEffect, useRef, useState } from "react";
import { Database, Eye, Pencil, Plug, Plus, Trash2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CopyButton } from "@/components/common/copy-button";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import {
  newConnectorMutationIntent,
  type ConnectorMutationIntent,
} from "@/lib/connector-intent";
import { useStore } from "@/lib/store";
import { isNoResponseTransportUncertainty } from "@/lib/controller-request-errors";
import type { Connector, Environment } from "@/lib/types";
import { ConnectorEditorDrawer } from "./connector-editor-drawer";
import { ConnectorViewDialog } from "./connector-view-dialog";
import { normalizedPrefix } from "./connector-display";
import { getConnectorEditSnapshot, type ConnectorEditSnapshot } from "./api";
export function EnvironmentConnectorManager({ env }: { env: Environment }) {
  const store = useStore();
  const project = store.getProjectById(env.projectId);
  const tenantSlug =
    store.tenants.find((tenant) => tenant.id === project?.tenantId)?.slug ??
    project?.tenantId ??
    "";
  const projectSlug = project?.slug ?? env.projectId;
  const connectors = store.connectors.filter(
    (connector) => connector.scopeRef === env.id,
  );
  const secretOptions = store.reusableSecrets.filter(
    (secret) =>
      secret.kind === "env" &&
      (secret.scope === "platform" ||
        (secret.scope === "project" && secret.projectId === env.projectId)),
  );
  const backupPolicyState = store.getBackupPolicyState(env.id);
  const backupPolicy = backupPolicyState.policy;
  const policyAuthoritative =
    backupPolicyState.loaded &&
    !backupPolicyState.loading &&
    !backupPolicyState.loadError;
  const activeConnector = connectors.find(
    (connector) => connector.id === backupPolicy.connectorId,
  );
  const [createOpen, setCreateOpen] = useState(false);
  const [viewing, setViewing] = useState<Connector | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editSnapshot, setEditSnapshot] =
    useState<ConnectorEditSnapshot | null>(null);
  const [editLoading, setEditLoading] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);
  const editLoadGeneration = useRef(0);
  const editLoadAbort = useRef<AbortController | null>(null);
  const [removing, setRemoving] = useState<Connector | null>(null);
  const [removeIntent, setRemoveIntent] = useState<{
    environmentId: string;
    connectorId: string;
    intent: ConnectorMutationIntent | null;
  } | null>(null);

  useEffect(() => {
    setRemoving(null);
    setRemoveIntent(null);
    closeEdit();
  }, [env.id]);

  useEffect(
    () => () => {
      editLoadGeneration.current += 1;
      editLoadAbort.current?.abort();
    },
    [],
  );

  function closeEdit() {
    editLoadGeneration.current += 1;
    editLoadAbort.current?.abort();
    editLoadAbort.current = null;
    setEditingId(null);
    setEditSnapshot(null);
    setEditLoading(false);
    setEditError(null);
  }

  function loadEdit(connectorId: string) {
    editLoadGeneration.current += 1;
    const generation = editLoadGeneration.current;
    editLoadAbort.current?.abort();
    const controller = new AbortController();
    editLoadAbort.current = controller;
    setEditingId(connectorId);
    setEditSnapshot(null);
    setEditLoading(true);
    setEditError(null);
    void getConnectorEditSnapshot(connectorId, controller.signal).then(
      (snapshot) => {
        if (
          controller.signal.aborted ||
          editLoadGeneration.current !== generation
        )
          return;
        if (snapshot.connector.scopeRef !== env.id) {
          setEditLoading(false);
          setEditError("Connector belongs to another environment");
          return;
        }
        setEditSnapshot(snapshot);
        setEditLoading(false);
      },
      (error: unknown) => {
        if (
          controller.signal.aborted ||
          editLoadGeneration.current !== generation
        )
          return;
        setEditLoading(false);
        setEditError(
          error instanceof Error
            ? error.message
            : "Unable to load current Connector metadata",
        );
      },
    );
  }

  const beginRemove = (connector: Connector) => {
    setRemoving(connector);
    setRemoveIntent({
      environmentId: env.id,
      connectorId: connector.id,
      intent: null,
    });
  };

  const cancelRemove = () => {
    setRemoving(null);
    setRemoveIntent(null);
  };

  const clearRemoveDispatchIntent = () => {
    setRemoveIntent((current) =>
      current ? { ...current, intent: null } : null,
    );
  };

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between gap-3">
          <div className="flex flex-col gap-1">
            <CardTitle className="flex items-center gap-2">
              <Plug className="size-4 text-muted-foreground" /> Backup
              connectors
            </CardTitle>
            <p className="text-xs text-muted-foreground">
              Destinations owned only by this environment. Connectors never
              inherit from project or platform scope.
            </p>
          </div>
          <Button size="sm" onClick={() => setCreateOpen(true)}>
            <Plus className="size-3.5" /> New connector
          </Button>
        </CardHeader>
        <CardContent className="flex flex-col gap-2">
          {store.connectorsLoading ? (
            <p
              className="rounded-lg border border-border bg-surface px-3 py-4 text-sm text-muted-foreground"
              role="status"
            >
              Loading connectors...
            </p>
          ) : store.connectorError ? (
            <p
              className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-3 text-sm text-destructive"
              role="alert"
            >
              {store.connectorError}
            </p>
          ) : connectors.length === 0 ? (
            <div className="flex flex-col items-center gap-2 rounded-lg border border-dashed border-border px-4 py-8 text-center">
              <Plug className="size-5 text-muted-foreground" />
              <div>
                <p className="text-sm font-medium">
                  No connector for this environment
                </p>
                <p className="text-xs text-muted-foreground">
                  Create one before enabling backups.
                </p>
              </div>
            </div>
          ) : (
            connectors.map((connector) => {
              const active =
                policyAuthoritative &&
                backupPolicy.enabled &&
                connector.id === activeConnector?.id;
              return (
                <div
                  key={connector.id}
                  className="flex flex-col gap-3 rounded-lg border border-border bg-surface px-3 py-3 sm:flex-row sm:items-center sm:justify-between"
                >
                  <div className="flex min-w-0 items-start gap-3">
                    <span className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-primary/10 text-primary">
                      <Database className="size-4" />
                    </span>
                    <div className="flex min-w-0 flex-col gap-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <span className="font-mono text-sm font-medium">
                          {connector.name}
                        </span>
                        {active && (
                          <Badge variant="default">policy target</Badge>
                        )}
                        <Badge variant="outline">{connector.kind}</Badge>
                      </div>
                      <CopyButton
                        value={`s3://${connector.bucket}/${normalizedPrefix(connector.prefix)}`}
                        label="Copy destination"
                        className="max-w-full justify-start px-0 py-0.5 font-mono text-xs"
                      >
                        s3://{connector.bucket}/
                        {normalizedPrefix(connector.prefix)}
                      </CopyButton>
                      <CopyButton
                        value={connector.endpoint}
                        label="Copy endpoint"
                        className="max-w-full justify-start px-0 py-0.5 text-xs"
                      >
                        {connector.endpoint}
                      </CopyButton>
                    </div>
                  </div>
                  <div className="flex shrink-0 items-center justify-end gap-1">
                    <CopyButton
                      value={`s3://${connector.bucket}/${normalizedPrefix(connector.prefix)}`}
                    />
                    <Button
                      variant="ghost"
                      size="icon-sm"
                      onClick={() => setViewing(connector)}
                      aria-label={`View connector ${connector.name}`}
                      title="View connector"
                    >
                      <Eye className="size-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon-sm"
                      onClick={() => loadEdit(connector.id)}
                      aria-label={`Edit connector ${connector.name}`}
                      title="Edit connector"
                    >
                      <Pencil className="size-4" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon-sm"
                      className="text-muted-foreground hover:text-destructive"
                      onClick={() =>
                        policyAuthoritative && !active && beginRemove(connector)
                      }
                      disabled={!policyAuthoritative || active}
                      aria-label={`Remove connector ${connector.name}`}
                      title={
                        !policyAuthoritative
                          ? "Load the Backup Policy before removing this connector"
                          : active
                            ? "Disable the backup policy before removing this connector"
                            : "Remove connector"
                      }
                    >
                      <Trash2 className="size-4" />
                    </Button>
                  </div>
                </div>
              );
            })
          )}
          {policyAuthoritative &&
            !store.connectorsLoading &&
            !store.connectorError &&
            backupPolicy.connectorId &&
            !activeConnector && (
              <p className="rounded-lg border border-destructive/30 bg-destructive/10 px-3 py-2 text-xs text-destructive">
                Backup policy references missing connector{" "}
                <span className="font-mono">{backupPolicy.connectorId}</span>.
                Select an environment-owned connector before enabling or running
                backups.
              </p>
            )}
        </CardContent>
      </Card>

      <ConnectorEditorDrawer
        mode="create"
        open={createOpen}
        onOpenChange={setCreateOpen}
        env={env}
        existingNames={connectors.map((connector) => connector.name)}
        secretOptions={secretOptions}
        onSave={async (connector, intent) => {
          await store.addConnector(connector, intent);
        }}
      />

      <ConnectorEditorDrawer
        mode="edit"
        open={editingId !== null}
        onOpenChange={(open) => {
          if (!open) closeEdit();
        }}
        env={env}
        existingNames={connectors.map((connector) => connector.name)}
        secretOptions={secretOptions}
        snapshot={editSnapshot}
        loading={editLoading}
        loadError={editError}
        onReload={() => {
          if (editingId) loadEdit(editingId);
        }}
        onSave={async (input, etag, intent) => {
          if (!editSnapshot)
            throw new Error("Current Connector metadata is unavailable");
          await store.editConnector(
            editSnapshot.connector.id,
            input,
            etag,
            intent,
          );
        }}
      />

      <ConnectorViewDialog
        connector={viewing}
        onOpenChange={(open) => {
          if (!open) setViewing(null);
        }}
        tenantSlug={tenantSlug}
        projectSlug={projectSlug}
        environmentName={env.name}
      />

      <TaskRunnerDialog
        open={removing !== null}
        onOpenChange={(open) => {
          if (!open) cancelRemove();
        }}
        title={`Remove connector · ${removing?.name ?? ""}`}
        description={
          "The Connector remains visible while the Controller finalizer runs. " +
          "Failure, timeout, or abort clears the fence and retains it."
        }
        type="remove"
        target={removing?.id ?? env.id}
        workspace={tenantSlug}
        destructive
        confirmText={removing?.name ?? ""}
        startLabel="Remove connector"
        executionCopy="The Controller will verify policy references and atomically finalize this Connector:"
        steps={[
          {
            label: "Remove metadata and encrypted credentials",
            state: "pending",
          },
        ]}
        onDispatch={() => {
          if (!removing)
            return Promise.reject(new Error("No Connector selected"));
          if (removing.scopeRef !== env.id) {
            cancelRemove();
            return Promise.reject(
              new Error("Connector belongs to another environment"),
            );
          }
          if (
            !removeIntent ||
            removeIntent.environmentId !== env.id ||
            removeIntent.connectorId !== removing.id
          ) {
            cancelRemove();
            return Promise.reject(new Error("No Connector removal intent"));
          }
          if (!policyAuthoritative) {
            clearRemoveDispatchIntent();
            return Promise.reject(
              new Error("Backup Policy state is not authoritative"),
            );
          }
          if (
            backupPolicy.enabled &&
            backupPolicy.connectorId === removing.id
          ) {
            clearRemoveDispatchIntent();
            return Promise.reject(
              new Error(
                "Disable the Backup Policy before removing this Connector",
              ),
            );
          }
          const intent = removeIntent.intent ?? newConnectorMutationIntent();
          if (!removeIntent.intent) {
            setRemoveIntent((current) => {
              if (
                !current ||
                current.environmentId !== env.id ||
                current.connectorId !== removing.id ||
                current.intent !== null
              )
                return current;
              return { ...current, intent };
            });
          }
          return store
            .removeConnector(removing.id, intent)
            .then((taskID) => {
              setRemoveIntent((current) => {
                if (
                  !current ||
                  current.intent !== intent ||
                  current.environmentId !== env.id ||
                  current.connectorId !== removing.id
                )
                  return current;
                return null;
              });
              return taskID;
            })
            .catch((error) => {
              setRemoveIntent((current) => {
                if (
                  !current ||
                  current.intent !== intent ||
                  current.environmentId !== env.id ||
                  current.connectorId !== removing.id
                )
                  return current;
                return isNoResponseTransportUncertainty(error)
                  ? current
                  : { ...current, intent: null };
              });
              throw error;
            });
        }}
      />
    </>
  );
}
