import { ConfigurationTypeOptions } from "@/components/common/configuration-type-options";
import { ConfigurationTypeFilter } from "@/components/common/configuration-type-filter";
import { BulkConfigurationMenu, type BulkConfigurationKind } from "@/components/common/bulk-configuration-menu";
import { EntryRemoveDialog } from "./entry-remove-dialog";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { EmptyState } from "@/components/common/empty-state";
import { CreationButton } from "@/components/common/creation-button";
import {
  ResourceForm,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceFormFooter as DialogFooter,
} from "@/components/common/resource-form";
("use client");

import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { Button } from "@/components/ui/button";
import { ResourcePanel } from "@/components/common/resource-panel";
import { EntryReferenceFields } from "./entry-reference-fields";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

import { ConfigurationEncryption } from "@/components/common/configuration-encryption";
import { EntryLiteralValue } from "./entry-literal-value";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, EnvironmentEntry, Service } from "@/lib/types";
import { Plus, ShieldCheck } from "lucide-react";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { BulkEntryDrawer } from "./bulk-entry-drawer";
import { EntryExposure } from "./entry-exposure";
import { EntryProtection } from "./entry-protection";
import { EntryRow } from "./entry-row";
import { entrySourceLabel } from "./entry-source-label";
// ---- Variables (env vars + env files) ----

export function EnvVarsCard({
  env,
  service,
}: {
  env: Environment;
  service?: Service;
}) {
  const store = useStore();
  const params = useRequiredParams("tenant", "project");
  const entries = service
    ? env.entries.filter(
        (entry) =>
          entry.exposure.includes("all") ||
          entry.exposure.includes(service.name),
      )
    : env.entries;
  const [open, setOpen] = useState(false);
  const [kind, setKind] = useState<"env" | "file">("env");
  const [key, setKey] = useState("");
  const [value, setValue] = useState("");
  const [path, setPath] = useState("");
  const [uid, setUID] = useState("0");
  const [gid, setGID] = useState("0");
  const [sourceKind, setSourceKind] = useState<
    "literal" | "secret_ref" | "fact"
  >("literal");
  const [secretRef, setSecretRef] = useState("");
  const [factAttach, setFactAttach] = useState("");
  const [factGrantAttach, setFactGrantAttach] = useState("");
  const [factKey, setFactKey] = useState("");
  const [exposure, setExposure] = useState<string[]>([service?.name ?? "all"]);
  const [secret, setSecret] = useState(false);
  const [encryption, setEncryption] = useState<"age" | "none">("none");
  const [valueReady, setValueReady] = useState(true);
  const [editing, setEditing] = useState<EnvironmentEntry | null>(null);
  const [removing, setRemoving] = useState<EnvironmentEntry | null>(null);
  const [after, setAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [bulkOpen, setBulkOpen] = useState(false);
  const [bulkKind, setBulkKind] = useState<BulkConfigurationKind>("variables");
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState("all");
  const project = store.getProject(params.tenant, params.project);
  const [search, setSearch] = useSearchParams();
  const fromConnection = search.get("fromConnection");
  const fromValue = search.get("connectionValue");
  const fromGrant = search.get("connectionGrant") ?? "";
  useEffect(() => {
    if (!fromConnection || !fromValue) return;
    const attach = env.attaches.find((item) => item.id === fromConnection);
    const fact = attach?.factSets
      .find((set) => (set.grantAttachId ?? "") === fromGrant)
      ?.facts.find((item) => item.key === fromValue);
    if (!fact) return;
    setSourceKind("fact");
    setFactAttach(fromConnection);
    setFactGrantAttach(fromGrant);
    setFactKey(fromValue);
    setSecret(fact.secret);
    setEncryption(fact.secret ? "age" : "none");
    setKey(fromValue);
    setOpen(true);
    setSearch(
      (current) => {
        const next = new URLSearchParams(current);
        [
          "fromConnection",
          "connectionValue",
          "connectionGrant",
          "connectionSecret",
        ].forEach((key) => next.delete(key));
        return next;
      },
      { replace: true },
    );
  }, [fromConnection, fromValue, fromGrant, env.attaches, setSearch]);

  const table = useTableView(
    entries.filter((entry) =>
      (typeFilter === "all" ||
        (entry.type === "file" ? "file" : entry.secret ? "secret" : "variable") === typeFilter) &&
      `${entry.key ?? entry.path ?? ""} ${entry.exposure.join(" ")} ${entry.source.kind}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
    ),
    {
      name: (entry) => entry.key ?? entry.path ?? "",
      type: (entry) => entry.type,
    },
    "name",
    "asc",
    `${env.id}/${query}/${typeFilter}`,
  );

  function startEdit(entry: EnvironmentEntry) {
    setKind(entry.type);
    setSecret(entry.secret);
    setEncryption(entry.encryption ?? (entry.secret ? "age" : "none"));
    setValueReady(!entry.secret && entry.encryption !== "age");
    setKey(entry.key ?? "");
    setPath(entry.path ?? "");
    setUID(String(entry.uid ?? 0));
    setGID(String(entry.gid ?? 0));
    setSourceKind(entry.source.kind);
    setValue(
      entry.source.kind === "literal" && !entry.secret && entry.encryption !== "age"
        ? (entry.source.literal ?? "")
        : "",
    );
    setSecretRef(
      entry.source.kind === "secret_ref" ? entry.source.secretRef : "",
    );
    setFactAttach(entry.source.kind === "fact" ? entry.source.attachId : "");
    setFactGrantAttach(
      entry.source.kind === "fact" ? (entry.source.grantAttachId ?? "") : "",
    );
    setFactKey(entry.source.kind === "fact" ? entry.source.fact : "");
    setExposure([...entry.exposure]);
    setAfter([]);
    setEditing(entry);
    setSaveError(null);
    setOpen(true);
  }

  function closeDrawer() {
    setOpen(false);
    setAfter([]);
    setEditing(null);
    setKind("env");
    setKey("");
    setValue("");
    setPath("");
    setUID("0");
    setGID("0");
    setSourceKind("literal");
    setSecretRef("");
    setFactAttach("");
    setFactGrantAttach("");
    setFactKey("");
    setExposure([service?.name ?? "all"]);
    setSecret(false);
    setEncryption("none");
    setValueReady(true);
    setSaveError(null);
  }


  async function saveEntry() {
    if (sourceKind === "literal" && !valueReady) {
      setSaveError("Load the current value, enter a replacement, or explicitly use an empty value.");
      return;
    }
    if (exposure.length === 0) {
      setSaveError("Select all services or at least one service.");
      return;
    }
    let source;
    switch (sourceKind) {
      case "literal":
        source = { kind: "literal", literal: value };
        break;
      case "secret_ref":
        if (!secretRef.trim()) {
          setSaveError("Choose or enter a Configuration reference.");
          return;
        }
        source = { kind: "secret_ref", secret_ref: secretRef.trim() };
        break;
      case "fact":
        if (!factAttach.trim() || !factKey.trim()) {
          setSaveError("Select a backing connection and connection value.");
          return;
        }
        source = {
          kind: "fact",
          attach_id: factAttach.trim(),
          grant_attach_id: factGrantAttach.trim() || undefined,
          fact: factKey.trim(),
        };
        break;
    }
    setSaving(true);
    setSaveError(null);
    try {
      if (editing) {
        await store.updateEntry(env.id, editing.id, {
          source,
          encryption,
          exposure,
          after,
        });
      } else {
        await store.addEntry(env.id, {
          type: kind,
          key: kind === "env" ? key.trim() : undefined,
          path: kind === "file" ? path.trim() : undefined,
          uid: kind === "file" ? Number(uid) : undefined,
          gid: kind === "file" ? Number(gid) : undefined,
          source,
          encryption,
          exposure,
          secret,
          after,
        });
      }
      closeDrawer();
    } catch (error) {
      setSaveError(
        error instanceof Error ? error.message : "Unable to save Entry",
      );
    } finally {
      setSaving(false);
    }
  }

  const editor = (
    <ResourceForm
      inline
      open={open}
      onOpenChange={(next) => (next ? undefined : closeDrawer())}
    >
      <DialogHeader>
        <DialogTitle>
          {editing ? "Edit configuration" : "Add configuration"} ·{" "}
          {env.name}
        </DialogTitle>
      </DialogHeader>
      <div className="grid min-w-0 gap-4 md:grid-cols-2">
        <div className="flex flex-col gap-1.5">
          <ConfigurationTypeOptions
            value={kind === "file" ? "file" : secret ? "secret" : "variable"}
            disabled={!!editing}
            onChange={(type) => {
              if (type !== "secret" && exposure.includes("component:edge-tunnel")) {
                setExposure([]);
              }
              setKind(type === "file" ? "file" : "env");
              setSecret(type === "secret");
              setEncryption(type === "secret" ? "age" : "none");
            }}
          />
        </div>

        {kind === "env" ? (
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="entry-key">Key</Label>
            <Input
              id="entry-key"
              value={key}
              disabled={!!editing}
              onChange={(event) => setKey(event.target.value)}
              placeholder="APP_ENV"
            />
          </div>
        ) : (
          <>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="entry-path">Destination path</Label>
              <Input
                id="entry-path"
                value={path}
                disabled={!!editing}
                onChange={(event) => setPath(event.target.value)}
                placeholder="config/app.ini"
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-uid">UID</Label>
                <Input
                  id="entry-uid"
                  type="number"
                  min={0}
                  max={4294967294}
                  value={uid}
                  disabled={!!editing}
                  onChange={(event) => setUID(event.target.value)}
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-gid">GID</Label>
                <Input
                  id="entry-gid"
                  type="number"
                  min={0}
                  max={4294967294}
                  value={gid}
                  disabled={!!editing}
                  onChange={(event) => setGID(event.target.value)}
                />
              </div>
            </div>
          </>
        )}

        <div className="flex flex-col gap-1.5">
          <Label>Value source</Label>
          <div className="flex flex-wrap gap-2">
            {(["literal", "secret_ref", "fact"] as const).map((candidate) => (
              <Button
                key={candidate}
                variant={sourceKind === candidate ? "default" : "outline"}
                size="sm"
                aria-pressed={sourceKind === candidate}
                disabled={exposure.includes("component:edge-tunnel") && candidate !== "literal"}
                onClick={() => setSourceKind(candidate)}
              >
                {
                  {
                    literal: "Enter value",
                    secret_ref: "Project / Platform configuration",
                    fact: "Connection value",
                  }[candidate]
                }
              </Button>
            ))}
          </div>
        </div>
        {sourceKind === "literal" && <EntryLiteralValue
          key={editing?.id ?? "new"} entry={editing} file={kind === "file"} secret={secret}
          value={value} onChange={setValue} ready={valueReady} onReadyChange={setValueReady} />}
        {sourceKind !== "literal" && (
          <EntryReferenceFields
            env={env}
            projectId={project?.id}
            sourceKind={sourceKind}
            kind={kind}
            secretRef={secretRef}
            connectionSource={{
              attachId: factAttach,
              grantAttachId: factGrantAttach,
              key: factKey,
            }}
            onSecretChange={(reference) => {
              setSecretRef(reference);
              const source = store.reusableSecrets.find(
                (item) => item.id === reference,
              );
              if (source && !editing) {
                setSecret(source.sensitive);
                setEncryption(source.sensitive ? "age" : "none");
              }
            }}
            onConnectionChange={(source) => {
              setFactAttach(source.attachId);
              setFactGrantAttach(source.grantAttachId);
              setFactKey(source.key);
            }}
          />
        )}

        <EntryExposure
          services={env.services}
          value={exposure}
          onChange={setExposure}
          allowTunnel={!service && kind === "env" && secret &&
            env.components.some((component) => component.kind === "cloudflare-tunnel")}
          onTunnelSelect={() => setSourceKind("literal")}
        />

        {kind === "file" && (
          <EntryProtection
            secret={secret}
            editing={!!editing}
            onChange={(secret) => { setSecret(secret); setEncryption(secret ? "age" : "none"); }}
          />
        )}
        <div className="md:col-span-2">
          <ConfigurationEncryption value={encryption} sensitive={secret} onChange={setEncryption} />
        </div>
        {saveError && <p className="text-sm text-destructive">{saveError}</p>}
      </div>
      <TaskRunAfterSelector
        value={after}
        onChange={setAfter}
        onBusyChange={setAfterLoading}
      />
      <DialogFooter>
        <Button variant="outline" onClick={closeDrawer} disabled={saving}>
          Cancel
        </Button>
        <Button
          disabled={
            saving ||
            afterLoading ||
            (kind === "env" ? !key.trim() : !path.trim()) ||
            exposure.length === 0 ||
            (sourceKind === "literal" && !valueReady) ||
            (sourceKind === "secret_ref" && !secretRef.trim()) ||
            (sourceKind === "fact" && (!factAttach.trim() || !factKey.trim()))
          }
          onClick={() => void saveEntry()}
        >
          {saving
            ? "Saving…"
            : editing
              ? "Save changes"
              : kind === "env"
                ? secret ? "Add secret" : "Add variable"
                : "Add file"}
        </Button>
      </DialogFooter>
    </ResourceForm>
  );

  function renderEntry(entry: EnvironmentEntry) {
    const label =
      entry.type === "env" ? (entry.key ?? entry.id) : (entry.path ?? entry.id);
    const literal =
      entry.source.kind === "literal" ? entry.source.literal : undefined;
    return (
      <EntryRow
        key={entry.id}
        label={label}
        editing={open && editing?.id === entry.id}
        editor={open && editing?.id === entry.id ? editor : undefined}
        file={entry.type === "file"}
        environmentName={env.name}
        source={entrySourceLabel(
          entry,
          env,
          store.reusableSecrets,
          project?.id,
        )}
        exposure={entry.exposure}
        secret={entry.secret}
        encryption={entry.encryption}
        emptySecretValue={entry.emptySecretValue}
        value={entry.secret ? undefined : (literal ?? "Resolved on apply")}
        copyValue={!entry.secret ? literal : undefined}
        loadValue={entry.secret || entry.encryption === "age" ? () => store.revealEntry(entry.id) : undefined}
        onEdit={() => startEdit(entry)}
        onRemove={() => setRemoving(entry)}
      />
    );
  }

  return (
    <>
      <ResourcePanel
        title={
          <span className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-muted-foreground" />{" "}
            Configuration
          </span>
        }
        actions={
          <div className="flex items-center gap-2">
            <BulkConfigurationMenu size="sm" disabled={open || bulkOpen} onSelect={(kind) => {
              setBulkKind(kind);
              setBulkOpen(true);
            }} />
            <CreationButton
              active={open || bulkOpen}
              variant="outline"
              size="sm"
              onClick={() => setOpen(true)}
            >
              <Plus className="size-3.5" /> Add configuration
            </CreationButton>
          </div>
        }
      >
        {!editing && editor}
        <BulkEntryDrawer
          key={bulkKind}
          kind={bulkKind}
          inline
          serviceName={service?.name}
          env={env}
          bulkOpen={bulkOpen}
          setBulkOpen={setBulkOpen}
        />

        {service && (
          <p className="text-xs text-muted-foreground">
            Environment Entries available to {service.name}. Shared Entries are
            the same definitions shown in Environment Configuration; editing one
            affects all listed Services.
          </p>
        )}
        <ListToolbar
          label="configuration"
          query={query}
          onQueryChange={setQuery}
          sort={table}
          fields={[
            { value: "name", label: "Key / path" },
            { value: "type", label: "Type" },
          ]}
        >
          <ConfigurationTypeFilter value={typeFilter} onChange={setTypeFilter} />
        </ListToolbar>
        <ul aria-label="Configuration values" className="min-w-0 space-y-2">
          {editing &&
            open &&
            !table.rows.some((entry) => entry.id === editing.id) &&
            renderEntry(editing)}
          {table.rows.map(renderEntry)}
        </ul>
        {!table.total && (
          <EmptyState
            embedded
            title={
              entries.length
                ? "No matching configuration"
                : "No configuration yet"
            }
            description={
              entries.length
                ? "Try another search or type filter."
                : "Add configuration values or files and select the Services that receive them."
            }
          />
        )}
        <TablePagination table={table} label="Configuration values" />
        <p className="mt-2 text-xs text-muted-foreground">
          Saving updates desired configuration only. Deploy or apply the
          Blueprint to update managed files and running processes.
        </p>
      </ResourcePanel>

      <EntryRemoveDialog
        entry={removing}
        environmentId={env.id}
        workspace={params.tenant}
        onClose={() => setRemoving(null)}
      />
    </>
  );
}
