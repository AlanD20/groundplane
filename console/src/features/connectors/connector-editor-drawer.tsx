"use client";
import { useEffect, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import {
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import {
  newConnectorMutationIntent,
  type ConnectorMutationIntent,
} from "@/lib/connector-intent";
import type {
  Connector,
  ConnectorCreateInput,
  ConnectorCredentialInput,
  ConnectorEditInput,
  Environment,
  ReusableSecret,
} from "@/lib/types";
import type { ConnectorEditSnapshot } from "./api";
import {
  ConnectorCredentialField,
  type CredentialDraft,
} from "./connector-credential-fields";
import { normalizedPrefix } from "./connector-display";
type SharedProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  env: Environment;
  existingNames: string[];
  secretOptions: ReusableSecret[];
};
type ConnectorEditorProps = SharedProps &
  (
    | {
        mode: "create";
        onSave: (
          connector: ConnectorCreateInput,
          intent: ConnectorMutationIntent,
        ) => Promise<void>;
      }
    | {
        mode: "edit";
        snapshot: ConnectorEditSnapshot | null;
        loading: boolean;
        loadError: string | null;
        onReload: () => void;
        onSave: (
          input: ConnectorEditInput,
          etag: string,
          intent: ConnectorMutationIntent,
        ) => Promise<void>;
      }
  );
const emptyCredential = (): CredentialDraft => ({ kind: "ref", value: "" });
const keptCredential = (): CredentialDraft => ({ kind: "keep", value: "" });
export function ConnectorEditorDrawer(props: ConnectorEditorProps) {
  const { open, onOpenChange, env, existingNames, secretOptions, mode } = props;
  const snapshot = mode === "edit" ? props.snapshot : null;
  const [name, setName] = useState("");
  const [provider, setProvider] = useState("");
  const [accountId, setAccountId] = useState("");
  const [endpoint, setEndpoint] = useState("");
  const [bucket, setBucket] = useState("");
  const [prefix, setPrefix] = useState("backups/");
  const [region, setRegion] = useState("auto");
  const [addressing, setAddressing] = useState<"path" | "virtual" | "">("");
  const [accessKey, setAccessKey] = useState<CredentialDraft>(emptyCredential);
  const [secretKey, setSecretKey] = useState<CredentialDraft>(emptyCredential);
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const inputRevision = useRef(0);
  const intentRef = useRef<{
    identity: string;
    inputRevision: number;
    intent: ConnectorMutationIntent;
  } | null>(null);
  useEffect(() => {
    if (!open) return;
    inputRevision.current += 1;
    intentRef.current = null;
    setSubmitError(null);
    if (mode === "create") {
      setName("");
      setProvider("");
      setAccountId("");
      setEndpoint("");
      setBucket("");
      setPrefix("backups/");
      setRegion("auto");
      setAddressing("");
      setAccessKey(emptyCredential());
      setSecretKey(emptyCredential());
      return;
    }
    if (!snapshot) return;
    const r2AccountId = cloudflareAccountId(snapshot.connector);
    setName(snapshot.connector.name);
    setProvider(r2AccountId ? "r2" : "s3");
    setAccountId(r2AccountId ?? "");
    setEndpoint(snapshot.connector.endpoint);
    setBucket(snapshot.connector.bucket);
    setPrefix(snapshot.connector.prefix);
    setRegion(snapshot.connector.region);
    setAddressing(snapshot.connector.pathStyle ? "path" : "virtual");
    setAccessKey(keptCredential());
    setSecretKey(keptCredential());
  }, [env.id, mode, open, snapshot]);
  const invalidateIntent = () => {
    inputRevision.current += 1;
    intentRef.current = null;
    setSubmitError(null);
  };
  const change = <Value,>(setter: (value: Value) => void, value: Value) => {
    invalidateIntent();
    setter(value);
  };
  const close = () => {
    invalidateIntent();
    onOpenChange(false);
  };
  const normalizedName = name.trim();
  const isR2 = provider === "r2";
  const normalizedAccountId = accountId.trim().toLowerCase();
  const resolvedEndpoint = isR2
    ? `https://${normalizedAccountId}.r2.cloudflarestorage.com`
    : endpoint.trim();
  const duplicate = existingNames.some(
    (candidate) =>
      candidate === normalizedName &&
      (mode === "create" || candidate !== snapshot?.connector.name),
  );
  const credentialsValid = [accessKey, secretKey].every(
    (credential) =>
      credential.kind === "keep" || credential.value.trim() !== "",
  );
  const fieldsValid =
    normalizedName !== "" &&
    !duplicate &&
    provider !== "" &&
    (isR2
      ? /^[a-f0-9]{32}$/.test(normalizedAccountId)
      : endpoint.trim() !== "" && region.trim() !== "" && addressing !== "") &&
    bucket.trim() !== "" &&
    credentialsValid;
  const editInput = snapshot
    ? buildEditInput(snapshot.connector, {
        name: normalizedName,
        endpoint: resolvedEndpoint,
        bucket: bucket.trim(),
        prefix: normalizedPrefix(prefix),
        region: isR2 ? "auto" : region.trim(),
        pathStyle: isR2 || addressing === "path",
        accessKey,
        secretKey,
      })
    : null;
  const valid =
    fieldsValid &&
    (mode === "create" ||
      (snapshot !== null && editInput !== null && hasEdit(editInput)));

  const submit = async () => {
    if (!valid) return;
    setSubmitting(true);
    setSubmitError(null);
    const identity =
      mode === "create"
        ? env.id
        : `${snapshot?.connector.id ?? ""}:${snapshot?.etag ?? ""}`;
    const prior = intentRef.current;
    const intent =
      prior &&
      prior.identity === identity &&
      prior.inputRevision === inputRevision.current
        ? prior.intent
        : newConnectorMutationIntent();
    intentRef.current = {
      identity,
      inputRevision: inputRevision.current,
      intent,
    };
    try {
      if (mode === "create") {
        await props.onSave(
          {
            name: normalizedName,
            kind: "s3-compatible",
            scope: "environment",
            scopeRef: env.id,
            endpoint: resolvedEndpoint,
            bucket: bucket.trim(),
            prefix: normalizedPrefix(prefix),
            region: isR2 ? "auto" : region.trim(),
            pathStyle: isR2 || addressing === "path",
            credentials: {
              accessKey: requiredCredential(accessKey),
              secretKey: requiredCredential(secretKey),
            },
          },
          intent,
        );
      } else if (snapshot && editInput) {
        await props.onSave(editInput, snapshot.etag, intent);
      }
      const current = intentRef.current;
      if (
        !current ||
        current.intent !== intent ||
        current.identity !== identity ||
        current.inputRevision !== inputRevision.current
      )
        return;
      close();
    } catch (error) {
      const current = intentRef.current;
      if (
        current?.intent === intent &&
        current.identity === identity &&
        current.inputRevision === inputRevision.current
      ) {
        setSubmitError(
          error instanceof Error
            ? error.message
            : `Unable to ${mode} Connector`,
        );
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Drawer
      open={open}
      onOpenChange={(next) => {
        if (!next) close();
      }}
    >
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>
            {mode === "create" ? "Add destination" : "Edit destination"} ·{" "}
            {env.name}
          </DialogTitle>
          <DialogDescription>
            {mode === "create"
              ? "Choose where this Environment stores its backups."
              : "Update the destination or replace either credential. Existing credential values are never read."}
          </DialogDescription>
        </DialogHeader>
        {mode === "edit" && !snapshot ? (
          <div className="flex flex-col items-start gap-3 rounded-lg border border-border bg-surface p-4">
            <p
              className={
                props.loadError
                  ? "text-sm text-destructive"
                  : "text-sm text-muted-foreground"
              }
              role={props.loadError ? "alert" : "status"}
            >
              {props.loadError ?? "Loading current Connector metadata..."}
            </p>
            {props.loadError && (
              <Button
                variant="outline"
                size="sm"
                onClick={props.onReload}
                disabled={props.loading}
              >
                {props.loading ? "Retrying..." : "Retry"}
              </Button>
            )}
          </div>
        ) : (
          <fieldset
            disabled={submitting}
            aria-label="Connector settings"
            className="grid gap-4 sm:grid-cols-2"
          >
            <Field
              id="connector-provider"
              label="Provider"
              className="sm:col-span-2"
            >
              <Select
                id="connector-provider"
                value={provider}
                onValueChange={(value) => {
                  invalidateIntent();
                  setProvider(value);
                  setAccountId("");
                  setEndpoint("");
                  setRegion("");
                  setAddressing("");
                }}
                options={[
                  { value: "", label: "Select a backup provider" },
                  { value: "r2", label: "Cloudflare R2" },
                  { value: "s3", label: "Other S3-compatible storage" },
                ]}
              />
            </Field>
            <Field id="connector-name" label="Name" className="sm:col-span-2">
              <Input
                id="connector-name"
                value={name}
                onChange={(event) => change(setName, event.target.value)}
                placeholder="r2-backups"
                autoFocus
              />
              {duplicate && (
                <p className="text-xs text-destructive">
                  This environment already has a destination with that name.
                </p>
              )}
            </Field>
            {isR2 ? (
              <Field
                id="connector-account-id"
                label="Cloudflare account ID"
                className="sm:col-span-2"
              >
                <Input
                  id="connector-account-id"
                  value={accountId}
                  onChange={(event) => change(setAccountId, event.target.value)}
                  placeholder="32-character account ID"
                  spellCheck={false}
                />
                <p className="text-xs text-muted-foreground">
                  Find it on your R2 Overview page. Region and addressing are
                  configured automatically.
                </p>
              </Field>
            ) : provider === "s3" ? (
              <Field
                id="connector-endpoint"
                label="Endpoint"
                className="sm:col-span-2"
              >
                <Input
                  id="connector-endpoint"
                  value={endpoint}
                  onChange={(event) => change(setEndpoint, event.target.value)}
                  placeholder="https://s3.example.com"
                />
              </Field>
            ) : null}
            <Field id="connector-bucket" label="Bucket">
              <Input
                id="connector-bucket"
                value={bucket}
                onChange={(event) => change(setBucket, event.target.value)}
                placeholder="groundplane-backups"
              />
            </Field>
            {provider === "s3" && (
              <Field id="connector-region" label="Region">
                <Input
                  id="connector-region"
                  value={region}
                  onChange={(event) => change(setRegion, event.target.value)}
                  placeholder="auto"
                />
              </Field>
            )}
            <Field
              id="connector-prefix"
              label="Prefix"
              className="sm:col-span-2"
            >
              <Input
                id="connector-prefix"
                value={prefix}
                onChange={(event) => change(setPrefix, event.target.value)}
                placeholder="backups/"
              />
            </Field>
            {provider === "s3" && (
              <Field
                id="connector-addressing"
                label="Addressing"
                className="sm:col-span-2"
              >
                <Select
                  id="connector-addressing"
                  value={addressing}
                  onValueChange={(value) =>
                    change(setAddressing, value as "path" | "virtual" | "")
                  }
                  options={[
                    { value: "", label: "Select addressing behavior" },
                    {
                      value: "path",
                      label: "Path style : endpoint/bucket/key",
                    },
                    {
                      value: "virtual",
                      label: "Virtual hosted : bucket.endpoint/key",
                    },
                  ]}
                />
              </Field>
            )}
            <ConnectorCredentialField
              id="connector-access-key"
              label="Access key"
              draft={accessKey}
              existing={snapshot?.connector.credentials.accessKey}
              onChange={(draft) => change(setAccessKey, draft)}
              secretOptions={secretOptions}
            />
            <ConnectorCredentialField
              id="connector-secret-key"
              label="Secret key"
              draft={secretKey}
              existing={snapshot?.connector.credentials.secretKey}
              onChange={(draft) => change(setSecretKey, draft)}
              secretOptions={secretOptions}
            />
          </fieldset>
        )}
        {submitError && (
          <div className="flex flex-col items-start gap-2">
            <p className="text-sm text-destructive" role="alert">
              {submitError}
            </p>
            {mode === "edit" && (
              <Button
                variant="outline"
                size="sm"
                disabled={submitting}
                onClick={props.onReload}
              >
                Reload current settings
              </Button>
            )}
          </div>
        )}
        <DialogFooter>
          <Button variant="outline" onClick={close}>
            Cancel
          </Button>
          <Button
            disabled={
              !valid || submitting || (mode === "edit" && props.loading)
            }
            onClick={submit}
          >
            {submitting
              ? "Saving..."
              : mode === "create"
                ? "Create destination"
                : "Save changes"}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
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

function requiredCredential(draft: CredentialDraft): ConnectorCredentialInput {
  const credential = changedCredential(draft);
  if (!credential) throw new Error("A new Connector requires both credentials");
  return credential;
}

function changedCredential(
  draft: CredentialDraft,
): ConnectorCredentialInput | undefined {
  if (draft.kind === "keep") return undefined;
  return draft.kind === "ref"
    ? { kind: "ref", name: draft.value.trim() }
    : { kind: "value", value: draft.value };
}

function buildEditInput(
  connector: Connector,
  current: Pick<
    Connector,
    "name" | "endpoint" | "bucket" | "prefix" | "region" | "pathStyle"
  > & {
    accessKey: CredentialDraft;
    secretKey: CredentialDraft;
  },
): ConnectorEditInput {
  const input: ConnectorEditInput = {};
  for (const key of [
    "name",
    "endpoint",
    "bucket",
    "prefix",
    "region",
  ] as const) {
    if (current[key] !== connector[key]) input[key] = current[key];
  }
  if (current.pathStyle !== connector.pathStyle)
    input.pathStyle = current.pathStyle;
  const accessKey = changedCredential(current.accessKey);
  const secretKey = changedCredential(current.secretKey);
  if (accessKey || secretKey) input.credentials = { accessKey, secretKey };
  return input;
}

function hasEdit(input: ConnectorEditInput) {
  return Object.keys(input).length > 0;
}

function cloudflareAccountId(connector: Connector) {
  if (connector.region !== "auto" || !connector.pathStyle) return null;
  return (
    /^https:\/\/([a-f0-9]{32})\.r2\.cloudflarestorage\.com\/?$/i
      .exec(connector.endpoint)?.[1]
      ?.toLowerCase() ?? null
  );
}
