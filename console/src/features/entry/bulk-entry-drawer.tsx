import type { BulkConfigurationKind } from "@/components/common/bulk-configuration-menu";
import { BulkRevealConfirmation } from "@/components/common/bulk-reveal-confirmation";
import { Checkbox } from "@/components/ui/checkbox";
("use client");

import { useEffect, useRef, useState } from "react";

import { useStore } from "@/lib/store";

import { Button } from "@/components/ui/button";

import { Label } from "@/components/ui/label";

import { CodeEditor } from "@/components/ui/code-editor";
import {
  ResourceFormFooter as DialogFooter,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
} from "@/components/common/resource-form";
import { ResourceForm } from "@/components/common/resource-form";

import type { Environment } from "@/lib/types";

import {
  formatBulkEnvironmentEntries,
  parseBulkEnvironmentEntries,
} from "./bulk-entry-input";
import {
  loadBulkEntryValues,
  planBulkEntryChanges,
  type BulkEntryBaseline,
} from "./bulk-entry-draft";
import { listAllEntries } from "./api";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
export function BulkEntryDrawer({
  env,
  bulkOpen,
  setBulkOpen,
  inline = false,
  serviceName,
  kind,
}: {
  env: Environment;
  bulkOpen: boolean;
  inline?: boolean;
  serviceName?: string;
  kind: BulkConfigurationKind;
  setBulkOpen: (open: boolean) => void;
}) {
  const store = useStore();
  const [after, setAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const acceptedBatches = useRef(new Set<string>());
  const [bulkText, setBulkText] = useState("");
  const [bulkExposure, setBulkExposure] = useState<string[]>(["all"]);
  const secretMode = kind === "secrets";
  const [encryption, setEncryption] = useState<"age" | "none">("age");
  const [confirmReveal, setConfirmReveal] = useState(false);
  const [bulkSaving, setBulkSaving] = useState(false);
  const [bulkError, setBulkError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [loadAttempt, setLoadAttempt] = useState(0);
  const [baseline, setBaseline] = useState<BulkEntryBaseline[]>([]);
  const [outsideKeys, setOutsideKeys] = useState<string[]>([]);
  const reveal = useRef(store.revealEntry);
  reveal.current = store.revealEntry;
  useEffect(() => {
    let cancelled = false;
    setBulkText("");
    setAfter([]);
    acceptedBatches.current.clear();
    setBaseline([]);
    setLoaded(false);
    setBulkError(null);
    setEncryption("age");
    setConfirmReveal(false);
    setBulkExposure([serviceName ?? "all"]);
    if (!bulkOpen) return;
    setLoading(true);
    void listAllEntries(env.id)
      .then(async (entries) => {
        if (cancelled) return;
        const visible = entries.filter(
          (entry) =>
            entry.type === "env" && entry.secret === secretMode &&
            (!serviceName ||
            (entry.exposure.length === 1 && entry.exposure[0] === serviceName)),
        );
        const values = secretMode ? visible.map((entry) => ({ entry, key: entry.key! }))
          : await loadBulkEntryValues(visible, (id) => reveal.current(id));
        if (cancelled) return;
        setOutsideKeys(
          entries
            .filter((entry) => entry.type === "env" && !visible.includes(entry))
            .map((entry) => entry.key!),
        );
        setBaseline(values);
        if (!secretMode) setBulkText(formatBulkEnvironmentEntries(
          values.filter((item): item is BulkEntryBaseline & { value: string } => "value" in item && typeof item.value === "string")));
        setLoaded(true);
      })
      .catch(() => {
        if (!cancelled)
          setBulkError(
            "Could not load configuration. Retry to edit safely.",
          );
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [bulkOpen, env.id, serviceName, loadAttempt, secretMode]);

  async function loadExistingSecrets() {
    setLoading(true);
    setBulkError(null);
    try {
      const values = await loadBulkEntryValues(baseline.map((item) => item.entry), (id) => reveal.current(id));
      setBaseline(values);
      setBulkText(formatBulkEnvironmentEntries(values));
    } catch (error) {
      setBulkError(error instanceof Error ? error.message : "Could not reveal secrets.");
    } finally {
      setLoading(false);
    }
  }

  let plan: ReturnType<typeof planBulkEntryChanges> | undefined;
  let inputError: string | undefined;
  if (loaded && bulkText.trim()) {
    try {
      plan = planBulkEntryChanges(
        parseBulkEnvironmentEntries(bulkText),
        baseline,
        {
          secret: secretMode,
          ...(secretMode ? { encryption } : {}),
          exposure: serviceName ? [serviceName] : bulkExposure,
        },
        outsideKeys,
      );
    } catch (error) {
      inputError = error instanceof Error ? error.message : "Invalid variables";
    }
  }
  function toggleBulkServiceExposure(serviceName: string) {
    setBulkExposure((current) => {
      const services = current.filter((candidate) => candidate !== "all");
      if (services.includes(serviceName)) {
        return services.filter((candidate) => candidate !== serviceName);
      }
      return [...services, serviceName];
    });
  }

  async function saveBulkEntries() {
    if (bulkExposure.length === 0) {
      setBulkError("Select all services or at least one service.");
      return;
    }
    if (!loaded || !plan?.batches.length || inputError) return;
    setBulkSaving(true);
    setBulkError(null);
    try {
      for (const batch of plan.batches) {
        const signature = JSON.stringify({ ...batch, after });
        if (acceptedBatches.current.has(signature)) continue;
        await store.bulkUpsertEntries(env.id, { ...batch, after });
        // Accepted Tasks own these exact inputs, even if a later submission fails.
        acceptedBatches.current.add(signature);
      }
      setBulkOpen(false);
      setBulkText("");
      setBulkExposure(["all"]);
      setEncryption("age");
    } catch (error) {
      setBulkError(
        error instanceof Error ? error.message : "Unable to bulk edit Entries",
      );
    } finally {
      setBulkSaving(false);
    }
  }

  return (
    <ResourceForm
      inline={inline}
      open={bulkOpen}
      onOpenChange={(next) => {
        if (bulkSaving) return;
        setBulkOpen(next);
        if (!next) setBulkError(null);
      }}
    >
      <DialogHeader>
        <DialogTitle>
          Bulk edit {kind} · {serviceName ?? env.name}
        </DialogTitle>
      </DialogHeader>
      <div className="flex flex-col gap-4">
        {secretMode && (
          <>
            <p className="text-sm text-muted-foreground">
              Paste secrets to add or replace. Existing secret values stay hidden
              until you load them. Files and ordinary variables are excluded.
            </p>
            <Button variant="outline" disabled={loading || !loaded || bulkSaving || baseline.length === 0}
              onClick={() => {
                if (store.requireRevealConfirm) setConfirmReveal(true);
                else void loadExistingSecrets();
              }}>
              Load existing secrets
            </Button>
            <BulkRevealConfirmation open={confirmReveal} onOpenChange={setConfirmReveal}
              subject={`${serviceName ?? env.name} secrets`} onConfirm={() => void loadExistingSecrets()} />
            <label className="flex items-center gap-2 text-sm">
              <Checkbox checked={encryption === "age"} disabled={loading || bulkSaving}
                onChange={(event) => setEncryption(event.target.checked ? "age" : "none")} />
              Encrypt every secret in this batch
            </label>
            {encryption === "none" && <p className="text-sm text-muted-foreground">
              Values stay hidden in the Console but will be stored unencrypted.
            </p>}
          </>
        )}
        {!secretMode && <p className="text-sm text-muted-foreground">
          New variables are unencrypted. Existing encryption settings are preserved;
          change them individually when needed. Secrets and files are excluded.
        </p>}
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="entry-bulk-values">{secretMode ? "Secrets" : "Variables"}</Label>
          <CodeEditor
            id="entry-bulk-values"
            label={`Environment ${kind}`}
            language="text"
            value={bulkText}
            disabled={loading || !loaded || bulkSaving}
            onValueChange={setBulkText}
          />
          <p className="text-xs text-muted-foreground">
            KEY=value; multiline values use quoted \n escapes. Omitted keys stay
            unchanged.
          </p>
          {serviceName && (
            <p className="text-xs text-muted-foreground">
              Only {kind} exposed exclusively to {serviceName} are editable
              here. Shared entries stay unchanged. New keys are exposed
              only to this Service.
            </p>
          )}
          {loading && (
            <p role="status" className="text-sm text-muted-foreground">
              Loading {kind}…
            </p>
          )}
          {!loading && !loaded && (
            <Button
              variant="outline"
              onClick={() => setLoadAttempt((attempt) => attempt + 1)}
            >
              Retry loading
            </Button>
          )}
        </div>
        <fieldset
          disabled={bulkSaving || !loaded}
          className="space-y-4 rounded-lg border border-border p-4"
        >
          <legend className="px-1 text-sm font-medium">New keys</legend>
          {!serviceName && (
            <div className="flex flex-col gap-1.5">
              <Label>Exposure</Label>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant={bulkExposure.includes("all") ? "default" : "outline"}
                  size="sm"
                  onClick={() => setBulkExposure(["all"])}
                >
                  All services
                </Button>
                {env.services.map((service) => (
                  <Button
                    key={service.id}
                    variant={
                      bulkExposure.includes(service.name)
                        ? "default"
                        : "outline"
                    }
                    size="sm"
                    onClick={() => toggleBulkServiceExposure(service.name)}
                  >
                    {service.name}
                  </Button>
                ))}
              </div>
            </div>
          )}
        </fieldset>
        {!!plan?.replacedReferences.length && (
          <p role="status" className="break-words text-sm text-warning">
            Saving replaces linked values with literals:{" "}
            {plan.replacedReferences.join(", ")}.
          </p>
        )}
        {(bulkError || inputError) && (
          <p role="alert" className="text-sm text-destructive">
            {bulkError || inputError}
          </p>
        )}
      </div>
      <TaskRunAfterSelector
        value={after}
        onChange={setAfter}
        onBusyChange={setAfterLoading}
      />
      <DialogFooter>
        <Button
          variant="outline"
          onClick={() => setBulkOpen(false)}
          disabled={bulkSaving}
        >
          Cancel
        </Button>
        <Button
          disabled={
            bulkSaving ||
            afterLoading ||
            !loaded ||
            !plan?.batches.length ||
            !!inputError ||
            bulkExposure.length === 0
          }
          onClick={() => void saveBulkEntries()}
        >
          {bulkSaving ? "Saving…" : "Save changes"}
        </Button>
      </DialogFooter>
    </ResourceForm>
  );
}
