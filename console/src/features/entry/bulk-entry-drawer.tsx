"use client";

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
  type BulkEntryValue,
} from "./bulk-entry-draft";
import { listAllEntries } from "./api";
import { entryFromAPI } from "./projection";
export function BulkEntryDrawer({
  env,
  bulkOpen,
  setBulkOpen,
  inline = false,
  serviceName,
}: {
  env: Environment;
  bulkOpen: boolean;
  inline?: boolean;
  serviceName?: string;
  setBulkOpen: (open: boolean) => void;
}) {
  const store = useStore();
  const [bulkText, setBulkText] = useState("");
  const [bulkExposure, setBulkExposure] = useState<string[]>(["all"]);
  const [bulkStorage, setBulkStorage] = useState<"plain" | "secret">("plain");
  const [bulkSaving, setBulkSaving] = useState(false);
  const [bulkError, setBulkError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [loadAttempt, setLoadAttempt] = useState(0);
  const [baseline, setBaseline] = useState<BulkEntryValue[]>([]);
  const [outsideKeys, setOutsideKeys] = useState<string[]>([]);
  const reveal = useRef(store.revealEntry);
  reveal.current = store.revealEntry;
  useEffect(() => {
    let cancelled = false;
    setBulkText("");
    setBaseline([]);
    setLoaded(false);
    setBulkError(null);
    setBulkStorage("plain");
    setBulkExposure([serviceName ?? "all"]);
    if (!bulkOpen) return;
    setLoading(true);
    void listAllEntries(env.id)
      .then(async (entries) => {
        if (cancelled) return;
        const visible = entries.filter(
          (entry) =>
            !serviceName ||
            entry.exposure.includes("all") ||
            entry.exposure.includes(serviceName),
        );
        const values = await loadBulkEntryValues(visible, (id) =>
          reveal.current(id),
        );
        if (cancelled) return;
        setOutsideKeys(
          entries
            .filter((entry) => entry.type === "env" && !visible.includes(entry))
            .map((entry) => entry.key!),
        );
        setBaseline(values);
        setBulkText(formatBulkEnvironmentEntries(values));
        setLoaded(true);
      })
      .catch(() => {
        if (!cancelled)
          setBulkError(
            "Could not load all variable values. Retry to edit safely.",
          );
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [bulkOpen, env.id, serviceName, loadAttempt]);

  let plan: ReturnType<typeof planBulkEntryChanges> | undefined;
  let inputError: string | undefined;
  if (loaded && bulkText.trim()) {
    try {
      plan = planBulkEntryChanges(
        parseBulkEnvironmentEntries(bulkText),
        baseline,
        { secret: bulkStorage === "secret", exposure: bulkExposure },
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
        const response = await store.bulkUpsertEntries(env.id, batch);
        // Retain accepted batches as the baseline if a later batch fails.
        setBaseline((current) => {
          const next = new Map(current.map((item) => [item.key, item]));
          for (const entry of response.entries ?? []) {
            const value = batch.entries.find((item) => item.key === entry.key);
            if (value)
              next.set(value.key, { ...value, entry: entryFromAPI(entry) });
          }
          return [...next.values()];
        });
      }
      setBulkOpen(false);
      setBulkText("");
      setBulkExposure(["all"]);
      setBulkStorage("plain");
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
        <DialogTitle>Bulk edit environment variables · {env.name}</DialogTitle>
      </DialogHeader>
      <div className="flex flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="entry-bulk-values">Variables</Label>
          <CodeEditor
            id="entry-bulk-values"
            label="Environment variables"
            language="text"
            value={bulkText}
            disabled={loading || !loaded || bulkSaving}
            onValueChange={setBulkText}
          />
          <p className="text-xs text-muted-foreground">
            KEY=value; multiline values use quoted \n escapes. Omitted keys stay
            unchanged.
          </p>
          {loading && (
            <p role="status" className="text-sm text-muted-foreground">
              Loading variables…
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
          <div className="flex flex-col gap-1.5">
            <Label>Encryption</Label>
            <div className="flex gap-2">
              {(["plain", "secret"] as const).map((candidate) => (
                <Button
                  key={candidate}
                  variant={bulkStorage === candidate ? "default" : "outline"}
                  size="sm"
                  onClick={() => setBulkStorage(candidate)}
                >
                  {candidate === "plain" ? "Plain" : "Encrypted"}
                </Button>
              ))}
            </div>
          </div>
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
                    bulkExposure.includes(service.name) ? "default" : "outline"
                  }
                  size="sm"
                  onClick={() => toggleBulkServiceExposure(service.name)}
                >
                  {service.name}
                </Button>
              ))}
            </div>
          </div>
        </fieldset>
        {!!plan?.replacedReferences.length && (
          <p role="status" className="break-words text-sm text-warning">
            Saving replaces linked values with literals:{" "}
            {plan.replacedReferences.join(", ")}. Encryption is preserved.
          </p>
        )}
        {(bulkError || inputError) && (
          <p role="alert" className="text-sm text-destructive">
            {bulkError || inputError}
          </p>
        )}
      </div>
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
