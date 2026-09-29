"use client";

import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, EnvironmentEntry } from "@/lib/types";
import { Plus, ShieldCheck } from "lucide-react";
import { useState } from "react";
import { BulkEntryDrawer } from "./bulk-entry-drawer";
import { EntryRow } from "./entry-row";
// ---- Variables (env vars + env files) ----

export function EnvVarsCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const entries = env.entries;
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
  const [exposure, setExposure] = useState<string[]>(["all"]);
  const [secret, setSecret] = useState(false);
  const [editing, setEditing] = useState<EnvironmentEntry | null>(null);
  const [removing, setRemoving] = useState<EnvironmentEntry | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [bulkOpen, setBulkOpen] = useState(false);
  const [query, setQuery] = useState("");
  const table = useTableView(
    entries.filter((entry) =>
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
    `${env.id}/${query}`,
  );

  function startEdit(entry: EnvironmentEntry) {
    setKind(entry.type);
    setSecret(entry.secret);
    setKey(entry.key ?? "");
    setPath(entry.path ?? "");
    setUID(String(entry.uid ?? 0));
    setGID(String(entry.gid ?? 0));
    setSourceKind(entry.source.kind);
    setValue(
      entry.source.kind === "literal" && !entry.secret
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
    setEditing(entry);
    setSaveError(null);
    setOpen(true);
  }

  function closeDrawer() {
    setOpen(false);
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
    setExposure(["all"]);
    setSecret(false);
    setSaveError(null);
  }

  function toggleServiceExposure(serviceName: string) {
    setExposure((current) => {
      const services = current.filter((candidate) => candidate !== "all");
      if (services.includes(serviceName)) {
        return services.filter((candidate) => candidate !== serviceName);
      }
      return [...services, serviceName];
    });
  }

  async function saveEntry() {
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
          setSaveError("Secret reference is required.");
          return;
        }
        source = { kind: "secret_ref", secret_ref: secretRef.trim() };
        break;
      case "fact":
        if (!factAttach.trim() || !factKey.trim()) {
          setSaveError("Fact Attach id and fact key are required.");
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
        await store.updateEntry(env.id, editing.id, { source, exposure });
      } else {
        await store.addEntry(env.id, {
          type: kind,
          key: kind === "env" ? key.trim() : undefined,
          path: kind === "file" ? path.trim() : undefined,
          uid: kind === "file" ? Number(uid) : undefined,
          gid: kind === "file" ? Number(gid) : undefined,
          source,
          exposure,
          secret,
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

  function renderEntry(entry: EnvironmentEntry) {
    const label =
      entry.type === "env" ? (entry.key ?? entry.id) : (entry.path ?? entry.id);
    const literal =
      entry.source.kind === "literal" ? entry.source.literal : undefined;
    return (
      <EntryRow
        key={entry.id}
        label={label}
        file={entry.type === "file"}
        path={entry.path}
        secret={entry.secret}
        emptySecretValue={entry.emptySecretValue}
        value={entry.secret ? undefined : (literal ?? entry.source.kind)}
        loadValue={entry.secret ? () => store.revealEntry(entry.id) : undefined}
        onEdit={() => startEdit(entry)}
        onRemove={() => setRemoving(entry)}
      />
    );
  }

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <ShieldCheck className="size-4 text-muted-foreground" /> Environment
            Entries
          </CardTitle>
          <div className="flex items-center gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setBulkOpen(true)}
            >
              Bulk edit
            </Button>
            <Button variant="outline" size="sm" onClick={() => setOpen(true)}>
              <Plus className="size-3.5" /> Add entry
            </Button>
          </div>
        </CardHeader>
        <CardContent className="flex flex-col gap-1.5">
          <ListToolbar
            label="Entries"
            query={query}
            onQueryChange={setQuery}
            sort={table}
            fields={[
              { value: "name", label: "Key / path" },
              { value: "type", label: "Type" },
            ]}
          />
          {table.rows.map((entry) => (
            <div key={entry.id} className="min-w-0">
              {renderEntry(entry)}
              <p className="px-3 pb-3 pt-1 text-[11px] text-muted-foreground">
                Exposed to{" "}
                {entry.exposure.includes("all")
                  ? "all Services"
                  : entry.exposure.join(", ") || "no Services"}{" "}
                ·{" "}
                {entry.source.kind === "literal"
                  ? "Literal value"
                  : entry.source.kind === "fact"
                    ? "Attach fact"
                    : "Secret reference"}
              </p>
            </div>
          ))}
          {!table.total && (
            <p className="p-6 text-center text-xs text-muted-foreground">
              {entries.length
                ? "No Entries match your search."
                : "No Entries yet."}
            </p>
          )}
          <TablePagination table={table} label="Entries" />
          <p className="mt-2 text-xs text-muted-foreground">
            Entries are one resource model for environment variables and files.
            Secret values remain encrypted and load only through the explicit
            reveal action.
          </p>
        </CardContent>
      </Card>

      <Drawer
        open={open}
        onOpenChange={(next) => (next ? undefined : closeDrawer())}
      >
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>
              {editing ? "Edit Entry" : "Add Entry"} · {env.name}
            </DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label>Type</Label>
              <div className="flex gap-2">
                {(["env", "file"] as const).map((candidate) => (
                  <Button
                    key={candidate}
                    variant={kind === candidate ? "default" : "outline"}
                    size="sm"
                    disabled={!!editing}
                    onClick={() => setKind(candidate)}
                  >
                    {candidate === "env" ? "env variable" : "file"}
                  </Button>
                ))}
              </div>
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
              <Label>Source</Label>
              <div className="flex flex-wrap gap-2">
                {(["literal", "secret_ref", "fact"] as const).map(
                  (candidate) => (
                    <Button
                      key={candidate}
                      variant={sourceKind === candidate ? "default" : "outline"}
                      size="sm"
                      onClick={() => setSourceKind(candidate)}
                    >
                      {candidate.replace("_", " ")}
                    </Button>
                  ),
                )}
              </div>
            </div>
            {sourceKind === "literal" && (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-value">Literal value</Label>
                {kind === "file" ? (
                  <Textarea
                    id="entry-value"
                    value={value}
                    onChange={(event) => setValue(event.target.value)}
                    rows={6}
                    spellCheck={false}
                    className="font-mono text-xs"
                  />
                ) : (
                  <Input
                    id="entry-value"
                    value={value}
                    onChange={(event) => setValue(event.target.value)}
                  />
                )}
                {editing?.secret && (
                  <p className="text-xs text-muted-foreground">
                    The existing secret is never loaded into this form. Enter a
                    replacement value.
                  </p>
                )}
              </div>
            )}
            {sourceKind === "secret_ref" && (
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="entry-secret-ref">
                  Reusable Secret reference
                </Label>
                <Input
                  id="entry-secret-ref"
                  value={secretRef}
                  onChange={(event) => setSecretRef(event.target.value)}
                  placeholder="DATABASE_PASSWORD"
                />
              </div>
            )}
            {sourceKind === "fact" && (
              <div className="grid gap-3">
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-fact-attach">Attach id</Label>
                  <Input
                    id="entry-fact-attach"
                    value={factAttach}
                    onChange={(event) => setFactAttach(event.target.value)}
                    placeholder="att_..."
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-fact-grant">
                    Grant Attach id · optional
                  </Label>
                  <Input
                    id="entry-fact-grant"
                    value={factGrantAttach}
                    onChange={(event) => setFactGrantAttach(event.target.value)}
                    placeholder="att_..."
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="entry-fact-key">Fact key</Label>
                  <Input
                    id="entry-fact-key"
                    value={factKey}
                    onChange={(event) => setFactKey(event.target.value)}
                    placeholder="pg16_URL"
                  />
                </div>
              </div>
            )}

            <div className="flex flex-col gap-1.5">
              <Label>Exposure</Label>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={exposure.includes("all") ? "default" : "outline"}
                  size="sm"
                  onClick={() => setExposure(["all"])}
                >
                  All services
                </Button>
                {env.services.map((service) => (
                  <Button
                    key={service.id}
                    variant={
                      exposure.includes(service.name) ? "default" : "outline"
                    }
                    size="sm"
                    onClick={() => toggleServiceExposure(service.name)}
                  >
                    {service.name}
                  </Button>
                ))}
              </div>
            </div>

            <label className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5">
              <div className="flex flex-col">
                <span className="text-sm font-medium">
                  Secret storage class
                </span>
                <span className="text-xs text-muted-foreground">
                  Secret Entries are encrypted and materialize at mode 0600.
                </span>
              </div>
              <Switch
                checked={secret}
                disabled={!!editing}
                onCheckedChange={setSecret}
              />
            </label>
            {saveError && (
              <p className="text-sm text-destructive">{saveError}</p>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={closeDrawer} disabled={saving}>
              Cancel
            </Button>
            <Button
              disabled={
                saving ||
                (kind === "env" ? !key.trim() : !path.trim()) ||
                exposure.length === 0 ||
                (sourceKind === "secret_ref" && !secretRef.trim()) ||
                (sourceKind === "fact" &&
                  (!factAttach.trim() || !factKey.trim()))
              }
              onClick={() => void saveEntry()}
            >
              {saving ? "Saving…" : editing ? "Save Entry" : "Add Entry"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>
      <BulkEntryDrawer
        env={env}
        bulkOpen={bulkOpen}
        setBulkOpen={setBulkOpen}
      />
      <TaskRunnerDialog
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove Entry · ${removing ? (removing.type === "env" ? removing.key : removing.path) : ""}`}
        description="Removes the pinned materialization, rewrites generated environment files when needed, and deletes the Entry only after the Agent reports success."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        confirmText={
          removing
            ? removing.type === "env"
              ? (removing.key ?? removing.id)
              : (removing.path ?? removing.id)
            : ""
        }
        startLabel="Remove Entry"
        steps={[
          { label: "Validate the pinned Entry generation", state: "pending" },
          { label: "Remove or rewrite materialized files", state: "pending" },
          { label: "Finalize the Entry record", state: "pending" },
        ]}
        onDispatch={async () => {
          if (!removing) throw new Error("No Entry selected for removal");
          return store.removeEntry(env.id, removing.id);
        }}
      />
    </>
  );
}
