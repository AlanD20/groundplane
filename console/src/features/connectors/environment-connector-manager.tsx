"use client";

import { useEffect, useRef, useState } from "react";
import { Database, Eye, Plug, Plus, Trash2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { CopyButton } from "@/components/common/copy-button";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import {
  newConnectorMutationIntent,
  type ConnectorMutationIntent,
} from "@/lib/connector-intent";
import { useStore } from "@/lib/store";
import { isNoResponseTransportUncertainty } from "@/lib/controller-request-errors";
import type {
  Connector,
  ConnectorCreateInput,
  ConnectorCredential,
  ConnectorCredentialInput,
  Environment,
  ReusableSecret,
} from "@/lib/types";
import { toYAML } from "@/lib/yaml";

type CredentialDraft = {
  kind: ConnectorCredentialInput["kind"];
  value: string;
};

const emptyCredential = (): CredentialDraft => ({ kind: "ref", value: "" });

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
  const [removing, setRemoving] = useState<Connector | null>(null);
  const [removeIntent, setRemoveIntent] = useState<{
    environmentId: string;
    connectorId: string;
    intent: ConnectorMutationIntent | null;
  } | null>(null);

  useEffect(() => {
    setRemoving(null);
    setRemoveIntent(null);
  }, [env.id]);

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
                      <span className="break-all font-mono text-xs text-muted-foreground">
                        s3://{connector.bucket}/
                        {normalizedPrefix(connector.prefix)}
                      </span>
                      <span className="truncate text-xs text-muted-foreground">
                        {connector.endpoint}
                      </span>
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

      <ConnectorCreateDrawer
        open={createOpen}
        onOpenChange={setCreateOpen}
        env={env}
        existingNames={connectors.map((connector) => connector.name)}
        secretOptions={secretOptions}
        onCreate={async (connector, intent) => {
          await store.addConnector(connector, intent);
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

function ConnectorCreateDrawer({
  open,
  onOpenChange,
  env,
  existingNames,
  secretOptions,
  onCreate,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  env: Environment;
  existingNames: string[];
  secretOptions: ReusableSecret[];
  onCreate: (
    connector: ConnectorCreateInput,
    intent: ConnectorMutationIntent,
  ) => Promise<void>;
}) {
  const [name, setName] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("backups/");
  const [region, setRegion] = useState("auto");
  const [addressing, setAddressing] = useState<"path" | "virtual" | "">("");
  const [accessKey, setAccessKey] = useState<CredentialDraft>(emptyCredential);
  const [secretKey, setSecretKey] = useState<CredentialDraft>(emptyCredential);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const createInputRevision = useRef(0);
  const createIntentRef = useRef<{
    environmentId: string;
    inputRevision: number;
    intent: ConnectorMutationIntent;
  } | null>(null);

  useEffect(() => {
    createInputRevision.current += 1;
    createIntentRef.current = null;
  }, [env.id]);

  const invalidateCreateIntent = () => {
    createInputRevision.current += 1;
    createIntentRef.current = null;
  };
  const normalizedName = name.trim();
  const duplicate = existingNames.includes(normalizedName);
  const valid =
    normalizedName !== "" &&
    !duplicate &&
    endpoint.trim() !== "" &&
    bucket.trim() !== "" &&
    addressing !== "" &&
    accessKey.value.trim() !== "" &&
    secretKey.value.trim() !== "";

  function reset() {
    invalidateCreateIntent();
    setName("");
    setEndpoint("");
    setBucket("");
    setPrefix("backups/");
    setRegion("auto");
    setAddressing("");
    setAccessKey(emptyCredential());
    setSecretKey(emptyCredential());
    setSubmitError(null);
  }

  return (
    <Drawer
      open={open}
      onOpenChange={(next) => {
        onOpenChange(next);
        if (!next) reset();
      }}
    >
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>New connector : {env.name}</DialogTitle>
          <DialogDescription>
            Create an environment-owned S3-compatible destination. Credential
            references are preferred over direct values.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 sm:grid-cols-2">
          <Field id="connector-name" label="Name" className="sm:col-span-2">
            <Input
              id="connector-name"
              value={name}
              onChange={(event) => {
                invalidateCreateIntent();
                setName(event.target.value);
              }}
              placeholder="r2-backups"
              autoFocus
            />
            {duplicate && (
              <p className="text-xs text-destructive">
                This environment already has a connector with that name.
              </p>
            )}
          </Field>
          <Field
            id="connector-endpoint"
            label="Endpoint"
            className="sm:col-span-2"
          >
            <Input
              id="connector-endpoint"
              value={endpoint}
              onChange={(event) => {
                invalidateCreateIntent();
                setEndpoint(event.target.value);
              }}
              placeholder="https://account.r2.cloudflarestorage.com"
            />
          </Field>
          <Field id="connector-bucket" label="Bucket">
            <Input
              id="connector-bucket"
              value={bucket}
              onChange={(event) => {
                invalidateCreateIntent();
                setBucket(event.target.value);
              }}
              placeholder="groundplane-backups"
            />
          </Field>
          <Field id="connector-region" label="Region">
            <Input
              id="connector-region"
              value={region}
              onChange={(event) => {
                invalidateCreateIntent();
                setRegion(event.target.value);
              }}
              placeholder="auto"
            />
          </Field>
          <Field id="connector-prefix" label="Prefix" className="sm:col-span-2">
            <Input
              id="connector-prefix"
              value={prefix}
              onChange={(event) => {
                invalidateCreateIntent();
                setPrefix(event.target.value);
              }}
              placeholder="backups/"
            />
          </Field>
          <Field
            id="connector-addressing"
            label="Addressing"
            className="sm:col-span-2"
          >
            <Select
              id="connector-addressing"
              value={addressing}
              onValueChange={(value) => {
                invalidateCreateIntent();
                setAddressing(value as "path" | "virtual" | "");
              }}
              options={[
                { value: "", label: "Select addressing behavior" },
                { value: "path", label: "Path style : endpoint/bucket/key" },
                {
                  value: "virtual",
                  label: "Virtual hosted : bucket.endpoint/key",
                },
              ]}
            />
          </Field>
          <CredentialField
            id="connector-access-key"
            label="Access key"
            draft={accessKey}
            onChange={(draft) => {
              invalidateCreateIntent();
              setAccessKey(draft);
            }}
            secretOptions={secretOptions}
          />
          <CredentialField
            id="connector-secret-key"
            label="Secret key"
            draft={secretKey}
            onChange={(draft) => {
              invalidateCreateIntent();
              setSecretKey(draft);
            }}
            secretOptions={secretOptions}
          />
        </div>
        {submitError && (
          <p className="text-sm text-destructive" role="alert">
            {submitError}
          </p>
        )}
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => {
              reset();
              onOpenChange(false);
            }}
          >
            Cancel
          </Button>
          <Button
            disabled={!valid || submitting}
            onClick={async () => {
              setSubmitting(true);
              setSubmitError(null);
              const priorCreateIntent = createIntentRef.current;
              const intent =
                priorCreateIntent &&
                priorCreateIntent.environmentId === env.id &&
                priorCreateIntent.inputRevision === createInputRevision.current
                  ? priorCreateIntent.intent
                  : newConnectorMutationIntent();
              createIntentRef.current = {
                environmentId: env.id,
                inputRevision: createInputRevision.current,
                intent,
              };
              try {
                await onCreate(
                  {
                    name: normalizedName,
                    kind: "s3-compatible",
                    scope: "environment",
                    scopeRef: env.id,
                    endpoint: endpoint.trim(),
                    bucket: bucket.trim(),
                    prefix: normalizedPrefix(prefix),
                    region: region.trim() || "auto",
                    pathStyle: addressing === "path",
                    credentials: {
                      accessKey: toCredential(accessKey),
                      secretKey: toCredential(secretKey),
                    },
                  },
                  intent,
                );
                const currentCreateIntent = createIntentRef.current;
                if (
                  !currentCreateIntent ||
                  currentCreateIntent.intent !== intent ||
                  currentCreateIntent.environmentId !== env.id ||
                  currentCreateIntent.inputRevision !==
                    createInputRevision.current
                )
                  return;
                onOpenChange(false);
                reset();
              } catch (error) {
                const currentCreateIntent = createIntentRef.current;
                if (
                  currentCreateIntent &&
                  currentCreateIntent.intent === intent &&
                  currentCreateIntent.environmentId === env.id &&
                  currentCreateIntent.inputRevision ===
                    createInputRevision.current
                ) {
                  if (!isNoResponseTransportUncertainty(error))
                    createIntentRef.current = null;
                  setSubmitError(
                    error instanceof Error
                      ? error.message
                      : "Unable to create Connector",
                  );
                }
              } finally {
                setSubmitting(false);
              }
            }}
          >
            {submitting ? "Saving..." : "Save connector"}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  );
}

function CredentialField({
  id,
  label,
  draft,
  onChange,
  secretOptions,
}: {
  id: string;
  label: string;
  draft: CredentialDraft;
  onChange: (draft: CredentialDraft) => void;
  secretOptions: ReusableSecret[];
}) {
  const listID = `${id}-secrets`;
  return (
    <fieldset className="flex min-w-0 flex-col gap-2 rounded-lg border border-border bg-surface p-3">
      <legend className="px-1 text-xs font-medium">{label}</legend>
      <Label htmlFor={`${id}-kind`}>Source</Label>
      <Select
        id={`${id}-kind`}
        value={draft.kind}
        onValueChange={(kind) =>
          onChange({
            kind: kind as ConnectorCredentialInput["kind"],
            value: "",
          })
        }
        options={[
          { value: "ref", label: "Secret-store reference" },
          { value: "value", label: "Direct encrypted value" },
        ]}
      />
      <Label htmlFor={id}>
        {draft.kind === "ref" ? "Secret key name" : "Credential value"}
      </Label>
      <Input
        id={id}
        type={draft.kind === "value" ? "password" : "text"}
        list={draft.kind === "ref" ? listID : undefined}
        value={draft.value}
        onChange={(event) => onChange({ ...draft, value: event.target.value })}
        placeholder={
          draft.kind === "ref" ? "R2_ACCESS_KEY_ID" : "Stored encrypted at rest"
        }
        autoComplete="off"
      />
      {draft.kind === "ref" && (
        <datalist id={listID}>
          {secretOptions.map((secret) => (
            <option key={secret.id} value={secret.key} />
          ))}
        </datalist>
      )}
      <p className="text-[11px] text-muted-foreground">
        {draft.kind === "ref"
          ? "The connector stores only the secret name."
          : "The Controller encrypts this value and never returns it."}
      </p>
    </fieldset>
  );
}

function ConnectorViewDialog({
  connector,
  onOpenChange,
  tenantSlug,
  projectSlug,
  environmentName,
}: {
  connector: Connector | null;
  onOpenChange: (open: boolean) => void;
  tenantSlug: string;
  projectSlug: string;
  environmentName: string;
}) {
  return (
    <Dialog open={connector !== null} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Connector : {connector?.name}</DialogTitle>
          <DialogDescription>
            Environment-owned destination and redacted desired state.
          </DialogDescription>
        </DialogHeader>
        {connector && (
          <div className="flex flex-col gap-4">
            <dl className="grid gap-3 rounded-lg border border-border bg-surface p-3 text-sm sm:grid-cols-2">
              <Detail label="Endpoint" value={connector.endpoint} />
              <Detail label="Region" value={connector.region} />
              <Detail label="Bucket" value={connector.bucket} />
              <Detail label="Prefix" value={connector.prefix} />
              <Detail
                label="Addressing"
                value={
                  connector.pathStyle ? "path style" : "virtual hosted style"
                }
              />
              <Detail
                label="Access key"
                value={credentialLabel(connector.credentials.accessKey)}
              />
              <Detail
                label="Secret key"
                value={credentialLabel(connector.credentials.secretKey)}
              />
            </dl>
            <div className="flex flex-col gap-1.5">
              <div className="flex items-center justify-between gap-2">
                <span className="font-mono text-xs text-muted-foreground">
                  connector.yaml
                </span>
                <CopyButton
                  value={toYAML(
                    connectorDocument(
                      connector,
                      tenantSlug,
                      projectSlug,
                      environmentName,
                    ),
                  )}
                />
              </div>
              <pre className="overflow-x-auto rounded-lg border border-border bg-background p-4 font-mono text-xs leading-relaxed">
                {toYAML(
                  connectorDocument(
                    connector,
                    tenantSlug,
                    projectSlug,
                    environmentName,
                  ),
                )}
              </pre>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

function Field({
  id,
  label,
  className,
  children,
}: {
  id: string;
  label: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={`flex flex-col gap-1.5 ${className ?? ""}`}>
      <Label htmlFor={id}>{label}</Label>
      {children}
    </div>
  );
}

function Detail({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="break-all font-mono text-xs">{value}</dd>
    </div>
  );
}

function normalizedPrefix(prefix: string) {
  const trimmed = prefix.trim().replace(/^\/+/, "");
  return trimmed === "" || trimmed.endsWith("/") ? trimmed : `${trimmed}/`;
}

function toCredential(draft: CredentialDraft): ConnectorCredentialInput {
  return draft.kind === "ref"
    ? { kind: "ref", name: draft.value.trim() }
    : { kind: "value", value: draft.value };
}

function credentialLabel(credential: ConnectorCredential) {
  return credential.kind === "ref"
    ? `secret ref : ${credential.name}`
    : "direct value : encrypted";
}

function connectorDocument(
  connector: Connector,
  tenantSlug: string,
  projectSlug: string,
  environmentName: string,
) {
  const credential = (value: ConnectorCredential) =>
    value.kind === "ref" ? { secret_ref: value.name } : { value: "<redacted>" };
  return {
    kind: "connector",
    schema: 1,
    metadata: {
      name: connector.name,
      tenant: tenantSlug,
      project: projectSlug,
      environment: environmentName,
    },
    connector: {
      kind: connector.kind,
      endpoint: connector.endpoint,
      bucket: connector.bucket,
      prefix: connector.prefix,
      region: connector.region,
      path_style: connector.pathStyle,
      credentials: {
        access_key: credential(connector.credentials.accessKey),
        secret_key: credential(connector.credentials.secretKey),
      },
    },
  };
}
