"use client";

import { useEffect, useRef, useState } from "react";
import { ChevronDown, ChevronUp, Database, HardDrive } from "lucide-react";
import { useStore } from "@/lib/store";
import {
  MAXIMUM_BACKUP_POLICY_KEEP,
  isValidBackupPolicyKeep,
} from "@/lib/backup-policy-contract";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { CopyButton } from "@/components/common/copy-button";
import { cn } from "@/lib/utils";
import type { Environment } from "@/lib/types";
import type {
  BackupPolicyReplacement,
  BackupPolicySourceInput,
} from "@/features/backup/types";
import { Checkbox } from "@/components/ui/checkbox";
import {
  backupPolicyConfigured,
  deriveStrategy,
  backupSourceLabel,
} from "@/features/backup/environment-backup-projection";
import {
  MAX_BACKUP_POLICY_SOURCES,
  validateBackupPolicy,
} from "./backup-policy-form-model";
import {
  backupPolicySourceOptions,
  backupSourceInputLabel,
} from "./backup-policy-source-options";
export function BackupPolicyDialog({
  env,
  open,
  onOpenChange,
}: {
  env: Environment;
  open: boolean;
  onOpenChange: (v: boolean) => void;
}) {
  const store = useStore();
  const policyState = store.getBackupPolicyState(env.id);
  const backup = policyState.policy;
  const [enabled, setEnabled] = useState(false);
  const [frequency, setFrequency] = useState("");
  const [keep, setKeep] = useState("");
  const [encryption, setEncryption] = useState<"age" | "none" | "">("");
  const [connector, setConnector] = useState("");
  const [sources, setSources] = useState<BackupPolicySourceInput[]>([]);
  const openedEmpty = useRef(false);
  const autoFilledFrequency = useRef(false);
  const autoFilledKeep = useRef(false);
  const connectorOptions = store.connectors.filter(
    (candidate) => candidate.scopeRef === env.id,
  );
  const selectedConnector = connectorOptions.find(
    (candidate) => candidate.id === connector,
  );
  const {
    attachOptions,
    unsupportedAttachSources,
    missingAttachSources,
    missingVolumeSources,
  } = backupPolicySourceOptions(store, policyState, sources);
  useEffect(() => {
    if (!open) return;
    setEnabled(backup.enabled);
    setFrequency(backup.frequency ?? "");
    setKeep(backup.keep === undefined ? "" : String(backup.keep));
    setEncryption(backup.encryption ?? "");
    setConnector(backup.connectorId ?? "");
    setSources(
      backup.sources.map(({ kind, targetId }) => ({ kind, targetId })),
    );
    openedEmpty.current = !backupPolicyConfigured(backup);
    autoFilledFrequency.current = false;
    autoFilledKeep.current = false;
  }, [open]);
  const includesSource = (
    kind: BackupPolicySourceInput["kind"],
    targetId: string,
  ) =>
    sources.some(
      (source) => source.kind === kind && source.targetId === targetId,
    );
  const toggleSource = (
    kind: BackupPolicySourceInput["kind"],
    targetId: string,
    checked: boolean,
  ) => {
    setSources((current) =>
      checked
        ? current.some(
            (source) => source.kind === kind && source.targetId === targetId,
          )
          ? current
          : [...current, { kind, targetId }]
        : current.filter(
            (source) => source.kind !== kind || source.targetId !== targetId,
          ),
    );
  };
  const moveSource = (index: number, offset: -1 | 1) => {
    setSources((current) => {
      const destination = index + offset;
      if (destination < 0 || destination >= current.length) return current;
      const next = [...current];
      [next[index], next[destination]] = [next[destination], next[index]];
      return next;
    });
  };
  const keepNumber =
    keep === "" ? undefined : /^\d+$/.test(keep) ? Number(keep) : Number.NaN;
  const replacement: BackupPolicyReplacement = {
    enabled,
    frequency: frequency || undefined,
    keep: keepNumber,
    encryption: encryption || undefined,
    connectorId: connector || undefined,
    sources,
  };
  const sourcesAvailable = sources.every((source) => {
    if (source.kind === "config") return source.targetId === env.id;
    const catalog =
      source.kind === "attach" ? attachOptions : policyState.volumes;
    return catalog.some((candidate) => candidate.id === source.targetId);
  });
  const validationError = validateBackupPolicy(
    replacement,
    Boolean(selectedConnector),
    sourcesAvailable,
  );
  const policyAuthoritative =
    policyState.loaded && !policyState.loading && !policyState.loadError;
  const policyLocked = policyState.saving || !policyAuthoritative;
  return (
    <Drawer
      open={open}
      onOpenChange={(next) => !policyState.saving && onOpenChange(next)}
    >
      <DrawerContent>
        <DialogHeader>
          <DialogTitle>
            Backup policy · {deriveStrategy(store, env)}
          </DialogTitle>
        </DialogHeader>
        <fieldset disabled={policyLocked} className="contents">
          <div className="flex flex-col gap-4">
            <p className="text-xs text-muted-foreground">
              Strategy comes from the adapter. These are the operator&apos;s
              choices for this backup resource.
            </p>
            <label className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2.5">
              <div className="flex flex-col">
                <span className="text-sm font-medium">Backups</span>
                <span className="text-xs text-muted-foreground">
                  {enabled
                    ? "enabled — scheduled runs per the policy below"
                    : "off — nothing is ever backed up (staging/dev friendly)"}
                </span>
              </div>
              <Switch
                checked={enabled}
                disabled={policyState.saving}
                onCheckedChange={(next) => {
                  setEnabled(next);
                  if (next) {
                    if (openedEmpty.current && !frequency) {
                      setFrequency("*-*-* 03:15:00");
                      autoFilledFrequency.current = true;
                    }
                    if (
                      openedEmpty.current &&
                      (keepNumber === undefined ||
                        !isValidBackupPolicyKeep(keepNumber))
                    ) {
                      setKeep("7");
                      autoFilledKeep.current = true;
                    }
                  } else {
                    if (autoFilledFrequency.current) setFrequency("");
                    if (autoFilledKeep.current) setKeep("");
                    autoFilledFrequency.current = false;
                    autoFilledKeep.current = false;
                  }
                }}
              />
            </label>
            {enabled && (
              <>
                <div className="flex flex-col gap-1.5">
                  <Label>Sources · what to back up</Label>
                  <p className="text-xs text-muted-foreground">
                    One source per attach, not per service — a shared attach
                    (api + worker + scheduler) is backed up once. One run backs
                    up every selected source in the order shown below. Select
                    only what you need — at most 12 sources.
                  </p>
                  {attachOptions.length === 0 && (
                    <p className="text-xs text-muted-foreground">
                      no supported PostgreSQL attaches yet
                    </p>
                  )}
                  {unsupportedAttachSources.map((source) => (
                    <div
                      key={source.id}
                      className="flex items-center justify-between gap-3 rounded-lg border border-warning/30 bg-warning/5 px-3 py-2 text-xs text-warning"
                    >
                      <span>
                        {backupSourceLabel(store, env, source)} survives, but
                        its Attach kind is unsupported for MVP backup. Remove it
                        before enabling.
                      </span>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          toggleSource("attach", source.targetId, false)
                        }
                      >
                        Remove
                      </Button>
                    </div>
                  ))}
                  {missingAttachSources.map((source) => (
                    <div
                      key={source.id}
                      className="flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
                    >
                      <span>
                        Missing Attach target {source.targetId}. Remove this
                        stale source before enabling.
                      </span>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          toggleSource("attach", source.targetId, false)
                        }
                      >
                        Remove
                      </Button>
                    </div>
                  ))}
                  {missingVolumeSources.map((source) => (
                    <div
                      key={source.id}
                      className="flex items-center justify-between gap-3 rounded-lg border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
                    >
                      <span>
                        Missing Volume target {source.targetId}. Remove this
                        stale source before enabling.
                      </span>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() =>
                          toggleSource("volume", source.targetId, false)
                        }
                      >
                        Remove
                      </Button>
                    </div>
                  ))}
                  <div className="flex flex-col gap-1.5">
                    {attachOptions.map((a) => {
                      const g = store.getBackingProject(a.backingProjectId);
                      const checked = includesSource("attach", a.id);
                      return (
                        <label
                          key={a.id}
                          className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs"
                        >
                          <span className="flex items-center gap-2">
                            <Checkbox
                              checked={checked}
                              disabled={
                                policyState.saving ||
                                (!checked &&
                                  sources.length >= MAX_BACKUP_POLICY_SOURCES)
                              }
                              onChange={(e) =>
                                toggleSource("attach", a.id, e.target.checked)
                              }
                              className="accent-primary"
                            />
                            <Database className="size-3.5 text-muted-foreground" />
                            <span className="font-mono">
                              {g?.name ?? a.name}
                            </span>
                            <span className="font-mono text-muted-foreground">
                              {a.name}
                            </span>
                          </span>
                          <span className="font-mono text-[10px] text-muted-foreground">
                            {a.id}
                          </span>
                        </label>
                      );
                    })}
                  </div>
                  {policyState.volumes.length > 0 && (
                    <div className="flex flex-col gap-1.5">
                      <span className="text-xs font-medium text-muted-foreground">
                        Volumes · pick a few, not all
                      </span>
                      {policyState.volumes.map((v) => {
                        const checked = includesSource("volume", v.id);
                        return (
                          <label
                            key={v.id}
                            className="flex items-center justify-between gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs"
                          >
                            <span className="flex items-center gap-2">
                              <Checkbox
                                checked={checked}
                                disabled={
                                  policyState.saving ||
                                  (!checked &&
                                    sources.length >= MAX_BACKUP_POLICY_SOURCES)
                                }
                                onChange={(e) =>
                                  toggleSource("volume", v.id, e.target.checked)
                                }
                                className="accent-primary"
                              />
                              <HardDrive className="size-3.5 text-muted-foreground" />
                              <span className="font-mono">{v.slug}</span>
                              <span className="font-mono text-muted-foreground">
                                key: {v.key}
                              </span>
                            </span>
                            <span className="font-mono text-[10px] text-muted-foreground">
                              {v.id}
                            </span>
                          </label>
                        );
                      })}
                    </div>
                  )}
                  <label className="flex items-start gap-2 rounded-lg border border-border bg-surface px-3 py-2 text-xs">
                    <Checkbox
                      checked={includesSource("config", env.id)}
                      disabled={
                        policyState.saving ||
                        (!includesSource("config", env.id) &&
                          sources.length >= MAX_BACKUP_POLICY_SOURCES)
                      }
                      onChange={(e) =>
                        toggleSource("config", env.id, e.target.checked)
                      }
                      className="mt-0.5 accent-primary"
                    />
                    <span className="flex flex-col gap-1">
                      <span className="font-medium text-foreground">
                        Environment config — env vars, files &amp; secrets
                        (values included)
                      </span>
                      <span className="text-muted-foreground">
                        Exported and age-encrypted like any source; restore
                        replaces this environment&apos;s entries. Does{" "}
                        <span className="font-medium text-warning">NOT</span>{" "}
                        back up backing environments — postgres, valkey, and
                        their configs are never included — and never platform
                        state.
                      </span>
                    </span>
                  </label>
                  <p className="text-xs text-muted-foreground">
                    {sources.length} / {MAX_BACKUP_POLICY_SOURCES} sources
                    selected.
                  </p>
                  {sources.length > 0 && (
                    <div className="flex flex-col gap-1.5 rounded-lg border border-border bg-surface p-2">
                      <span className="text-xs font-medium text-muted-foreground">
                        Backup order
                      </span>
                      {sources.map((source, index) => (
                        <div
                          key={`${source.kind}:${source.targetId}`}
                          className="flex items-center gap-2 rounded-md bg-background px-2 py-1.5 text-xs"
                        >
                          <span className="w-5 font-mono text-muted-foreground">
                            {index + 1}
                          </span>
                          <span className="min-w-0 flex-1 truncate font-mono">
                            {backupSourceInputLabel(store, env, source)}
                          </span>
                          <Button
                            variant="ghost"
                            size="icon"
                            disabled={policyState.saving || index === 0}
                            aria-label={`Move ${backupSourceInputLabel(store, env, source)} up`}
                            onClick={() => moveSource(index, -1)}
                          >
                            <ChevronUp className="size-3.5" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="icon"
                            disabled={
                              policyState.saving || index === sources.length - 1
                            }
                            aria-label={`Move ${backupSourceInputLabel(store, env, source)} down`}
                            onClick={() => moveSource(index, 1)}
                          >
                            <ChevronDown className="size-3.5" />
                          </Button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="bp-freq">Frequency · UTC</Label>
                  <Input
                    id="bp-freq"
                    disabled={policyState.saving}
                    value={frequency}
                    onChange={(event) => {
                      autoFilledFrequency.current = false;
                      setFrequency(event.target.value);
                    }}
                    placeholder="*-*-* 03:15:00"
                  />
                  <p className="text-xs text-muted-foreground">
                    Daily: <span className="font-mono">*-*-* HH:MM:SS</span>.
                    Weekly:{" "}
                    <span className="font-mono">Mon *-*-* HH:MM:SS</span>. Exact
                    spacing, UTC only.
                  </p>
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="bp-keep">Retention (backups kept)</Label>
                  <Input
                    id="bp-keep"
                    type="number"
                    min={1}
                    max={MAXIMUM_BACKUP_POLICY_KEEP}
                    step={1}
                    disabled={policyState.saving}
                    value={keep}
                    onChange={(event) => {
                      autoFilledKeep.current = false;
                      setKeep(event.target.value);
                    }}
                    placeholder="7"
                  />
                  <p className="text-xs text-muted-foreground">
                    Enter an integer from 1 to {MAXIMUM_BACKUP_POLICY_KEEP}{" "}
                    while enabled.
                  </p>
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label>Encryption</Label>
                  <div
                    aria-disabled={policyState.saving}
                    className={cn(
                      policyState.saving && "pointer-events-none opacity-60",
                    )}
                  >
                    <Select
                      value={encryption}
                      onValueChange={(v) =>
                        !policyState.saving &&
                        setEncryption(v as "age" | "none")
                      }
                      placeholder="Select encryption"
                      options={[
                        { value: "age", label: "age — encrypted" },
                        { value: "none", label: "unencrypted" },
                      ]}
                    />
                  </div>
                </div>
                {encryption === "age" && (
                  <div className="flex flex-col gap-1 rounded-lg border border-border bg-surface px-3 py-2">
                    <span className="text-xs font-medium text-muted-foreground">
                      Recipient (generated per environment)
                    </span>
                    <div className="flex items-center justify-between gap-2">
                      <span className="truncate font-mono text-xs text-foreground">
                        {backup.ageRecipient ?? "— generated on first enable —"}
                      </span>
                      {backup.ageRecipient && (
                        <CopyButton value={backup.ageRecipient} />
                      )}
                    </div>
                    <p className="text-xs text-muted-foreground">
                      Managed on the Backups tab: export the current identity
                      repeatably with no-store for DR; rotate per environment.
                    </p>
                  </div>
                )}
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="bp-connector">Connector</Label>
                  <div
                    aria-disabled={policyState.saving}
                    className={cn(
                      policyState.saving && "pointer-events-none opacity-60",
                    )}
                  >
                    <Select
                      id="bp-connector"
                      value={connector}
                      onValueChange={(value) =>
                        !policyState.saving && setConnector(value)
                      }
                      placeholder="Select an environment connector"
                      options={connectorOptions.map((candidate) => ({
                        value: candidate.id,
                        label: candidate.name,
                      }))}
                    />
                  </div>
                  {selectedConnector ? (
                    <p className="font-mono text-xs text-muted-foreground">
                      s3://{selectedConnector.bucket}/{selectedConnector.prefix}
                    </p>
                  ) : (
                    <p className="text-xs text-destructive">
                      Create and select a connector owned by this environment.
                    </p>
                  )}
                </div>
              </>
            )}
          </div>
        </fieldset>
        <DialogFooter>
          {validationError && (
            <p className="mr-auto text-xs text-destructive">
              {validationError}
            </p>
          )}
          {policyState.saveError && (
            <p className="mr-auto text-xs text-destructive">
              {policyState.saveError}
            </p>
          )}
          <Button
            variant="outline"
            disabled={policyState.saving}
            onClick={() => onOpenChange(false)}
          >
            Cancel
          </Button>
          <Button
            disabled={policyLocked || Boolean(validationError)}
            onClick={() => {
              if (validationError || !policyAuthoritative) return;
              void store
                .replaceBackupPolicy(env.id, replacement)
                .then(() => onOpenChange(false))
                .catch(() => undefined);
            }}
          >
            {policyState.saving ? "Saving…" : "Save policy"}
          </Button>
        </DialogFooter>
      </DrawerContent>
    </Drawer>
  );
}
