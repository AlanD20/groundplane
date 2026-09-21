"use client";

import { useState } from "react";

import { useStore } from "@/lib/store";

import { Button } from "@/components/ui/button";

import { Label } from "@/components/ui/label";

import { Textarea } from "@/components/ui/textarea";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";

import type { Environment } from "@/lib/types";

import { parseBulkEnvironmentEntries } from "./bulk-entry-input";
export function BulkEntryDrawer({
  env,
  bulkOpen,
  setBulkOpen,
}: {
  env: Environment;
  bulkOpen: boolean;
  setBulkOpen: (open: boolean) => void;
}) {
  const store = useStore();
  const [bulkText, setBulkText] = useState("");
  const [bulkExposure, setBulkExposure] = useState<string[]>(["all"]);
  const [bulkStorage, setBulkStorage] = useState<"plain" | "secret">("plain");
  const [bulkSaving, setBulkSaving] = useState(false);
  const [bulkError, setBulkError] = useState<string | null>(null);
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
    let entries: { key: string; value: string }[];
    try {
      entries = parseBulkEnvironmentEntries(bulkText);
    } catch (error) {
      setBulkError(
        error instanceof Error ? error.message : "Invalid bulk Entry input",
      );
      return;
    }
    setBulkSaving(true);
    setBulkError(null);
    try {
      await store.bulkUpsertEntries(env.id, {
        entries,
        exposure: bulkExposure,
        secret: bulkStorage === "secret",
      });
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
    <Drawer
      open={bulkOpen}
      onOpenChange={(next) => {
        setBulkOpen(next);
        if (!next) setBulkError(null);
      }}
    >
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>
            Bulk edit environment variables · {env.name}
          </DialogTitle>
        </DialogHeader>
        <div className="flex flex-col gap-4">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="entry-bulk-values">Variables</Label>
            <Textarea
              id="entry-bulk-values"
              value={bulkText}
              onChange={(event) => setBulkText(event.target.value)}
              rows={12}
              spellCheck={false}
              className="font-mono text-xs"
              placeholder={
                "APP_ENV=production\nDATABASE_URL=postgres://app:pass@db/app\nEMPTY_VALUE="
              }
            />
            <p className="text-xs text-muted-foreground">
              One KEY=value per line. Values may contain =. Blank lines and
              lines beginning with # are ignored. Matching keys are updated and
              omitted keys remain unchanged.
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label>Storage</Label>
            <div className="flex gap-2">
              {(["plain", "secret"] as const).map((candidate) => (
                <Button
                  key={candidate}
                  variant={bulkStorage === candidate ? "default" : "outline"}
                  size="sm"
                  onClick={() => setBulkStorage(candidate)}
                >
                  {candidate === "plain" ? "Plain values" : "Secret values"}
                </Button>
              ))}
            </div>
            <p className="text-xs text-muted-foreground">
              Existing keys must already use the selected storage class.
            </p>
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
          {bulkError && <p className="text-sm text-destructive">{bulkError}</p>}
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
              bulkSaving || !bulkText.trim() || bulkExposure.length === 0
            }
            onClick={() => void saveBulkEntries()}
          >
            {bulkSaving ? "Applying…" : "Apply bulk edit"}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  );
}
