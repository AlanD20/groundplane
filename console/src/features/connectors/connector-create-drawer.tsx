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
import { isNoResponseTransportUncertainty } from "@/lib/controller-request-errors";
import type {
  ConnectorCreateInput,
  ConnectorCredentialInput,
  Environment,
  ReusableSecret,
} from "@/lib/types";
import { normalizedPrefix } from "./connector-display";
type CredentialDraft = {
  kind: ConnectorCredentialInput["kind"];
  value: string;
};

const emptyCredential = (): CredentialDraft => ({ kind: "ref", value: "" });

export function ConnectorCreateDrawer({
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
  const isR2 = provider === "r2";
  const normalizedAccountId = accountId.trim().toLowerCase();
  const resolvedEndpoint = isR2
    ? `https://${normalizedAccountId}.r2.cloudflarestorage.com`
    : endpoint.trim();
  const duplicate = existingNames.includes(normalizedName);
  const valid =
    normalizedName !== "" &&
    !duplicate &&
    provider !== "" &&
    (isR2
      ? /^[a-f0-9]{32}$/.test(normalizedAccountId)
      : endpoint.trim() !== "" && region.trim() !== "" && addressing !== "") &&
    bucket.trim() !== "" &&
    accessKey.value.trim() !== "" &&
    secretKey.value.trim() !== "";

  function reset() {
    invalidateCreateIntent();
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
            Choose where this Environment stores its backups.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-4 sm:grid-cols-2">
          <Field
            id="connector-provider"
            label="Provider"
            className="sm:col-span-2"
          >
            <Select
              id="connector-provider"
              value={provider}
              onValueChange={(value) => {
                invalidateCreateIntent();
                setProvider(value);
                setAccountId("");
                setEndpoint("");
                setRegion("");
                setAddressing("");
                setSubmitError(null);
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
          {isR2 ? (
            <Field
              id="connector-account-id"
              label="Cloudflare account ID"
              className="sm:col-span-2"
            >
              <Input
                id="connector-account-id"
                value={accountId}
                onChange={(event) => {
                  invalidateCreateIntent();
                  setAccountId(event.target.value);
                }}
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
                onChange={(event) => {
                  invalidateCreateIntent();
                  setEndpoint(event.target.value);
                }}
                placeholder="https://s3.example.com"
              />
            </Field>
          ) : null}
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
          {provider === "s3" && (
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
          )}
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
          {provider === "s3" && (
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
          )}
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
                    endpoint: resolvedEndpoint,
                    bucket: bucket.trim(),
                    prefix: normalizedPrefix(prefix),
                    region: isR2 ? "auto" : region.trim(),
                    pathStyle: isR2 || addressing === "path",
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

function toCredential(draft: CredentialDraft): ConnectorCredentialInput {
  return draft.kind === "ref"
    ? { kind: "ref", name: draft.value.trim() }
    : { kind: "value", value: draft.value };
}
