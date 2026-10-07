"use client";

import { Input } from "@/components/ui/input";
import { TaskLink } from '@/components/common/task-link';

import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type ChangeEvent,
} from "react";
import {
  Download,
  FileCode2,
  FileUp,
  Pencil,
  RefreshCw,
  Upload,
} from "lucide-react";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { CopyButton } from "@/components/common/copy-button";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CodeEditor } from "@/components/ui/code-editor";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { BlueprintApplyAction } from "@/features/blueprint/blueprint-apply-action";
import { BlueprintReview } from "@/features/blueprint/blueprint-review";
import { BlueprintApplyScope } from './blueprint-apply-scope';
import {
  createBlueprintTextApplyRequest,
  type BlueprintApplyRequest,
} from "@/lib/blueprint-bundle";
import { useStore } from "@/lib/store";
import {
  type BlueprintDocumentResponse,
  type BlueprintValidationResponse,
  readBlueprintApplyRecovery,
  resolvePendingBlueprintApply,
  settleBlueprintApply,
} from "./api";
import { newULID } from "@/lib/utils";
import type { Environment } from "@/lib/types";

type PreparedBlueprint = {
  request: BlueprintApplyRequest;
  validation: BlueprintValidationResponse;
  key: string;
};

export function BlueprintWorkspace({
  environment,
  workspace,
}: {
  environment: Environment;
  workspace: string;
}) {
  const store = useStore();
  const getBlueprint = store.getBlueprint;
  const getTask = store.getTask;
  const fileInput = useRef<HTMLInputElement>(null);
  const [snapshot, setSnapshot] = useState<BlueprintDocumentResponse | null>(
    null,
  );
  const [draft, setDraft] = useState("");
  const [editing, setEditing] = useState(false);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState(false);
  const [selectedService, setSelectedService] = useState('');
  const [error, setError] = useState("");
  const [prepared, setPrepared] = useState<PreparedBlueprint | null>(null);
  const [dispatched, setDispatched] = useState(false);
  const [confirmRefresh, setConfirmRefresh] = useState(false);
  const [recovery, setRecovery] = useState(readBlueprintApplyRecovery);
  const pendingApply = recovery.pending;
  const applyBlocked = pendingApply !== null || recovery.error !== "";
  const refreshRecovery = useCallback(() => setRecovery(readBlueprintApplyRecovery()), []);
  const [resolveError, setResolveError] = useState("");
  const [resolving, setResolving] = useState(false);

  useEffect(() => {
    if (!pendingApply?.taskId) return;
    let active = true;
    let polling = false;
    const timer = window.setInterval(() => {
      if (polling) return;
      polling = true;
      void getTask(pendingApply.taskId!).then((task) => {
        if (!active) return;
        if (task.id !== pendingApply.taskId) throw new Error("Controller returned a different Task.");
        setResolveError("");
        if (["completed", "failed", "timed_out", "aborted"].includes(task.status)) {
          settleBlueprintApply(task.id);
          refreshRecovery();
        }
      }).catch((cause: unknown) => {
        if (active) setResolveError(cause instanceof Error ? cause.message : "Task status is unavailable.");
      }).finally(() => { polling = false; });
    }, 2000);
    return () => { active = false; window.clearInterval(timer); };
  }, [getTask, pendingApply, refreshRecovery]);

  async function resolveApply() {
    setResolving(true);
    setResolveError("");
    try {
      await resolvePendingBlueprintApply();
    } catch (cause) {
      setResolveError(cause instanceof Error ? cause.message : "Unable to resolve Blueprint Apply.");
    } finally {
      refreshRecovery();
      setResolving(false);
    }
  }

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const current = await getBlueprint(environment.id);
      setSnapshot(current);
      setDraft(current.document);
      setEditing(false);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Blueprint could not be loaded.",
      );
    } finally {
      setLoading(false);
    }
  }, [environment.id, getBlueprint]);

  useEffect(() => {
    void load();
  }, [load]);

  async function importDocument(event: ChangeEvent<HTMLInputElement>) {
    const file = event.currentTarget.files?.[0];
    event.currentTarget.value = "";
    if (!file) return;
    setError("");
    try {
      setDraft(await file.text());
      setEditing(true);
    } catch {
      setError("The Blueprint file could not be read.");
    }
  }

  function exportDocument() {
    if (!snapshot) return;
    const url = URL.createObjectURL(
      new Blob([snapshot.document], { type: "application/yaml;charset=utf-8" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `${environment.name}.blueprint.yaml`;
    link.click();
    URL.revokeObjectURL(url);
  }

  function refresh() {
    if (editing && snapshot && draft !== snapshot.document) {
      setConfirmRefresh(true);
      return;
    }
    void load();
  }

  async function reviewDraft() {
    if (!snapshot) return;
    setWorking(true);
    setError("");
    try {
      const request = await createBlueprintTextApplyRequest(draft);
      if (selectedService) request.manifest.service = selectedService;
      const validation = await store.validateBlueprint(
        environment.id,
        request,
        snapshot.revision,
      );
      setPrepared({ request, validation, key: newULID() });
      setDispatched(false);
    } catch (cause) {
      setError(
        cause instanceof Error ? cause.message : "Blueprint validation failed.",
      );
    } finally {
      setWorking(false);
    }
  }

  return (
    <>
      <Card>
        <CardHeader className="gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <CardTitle className="flex items-center gap-2">
              <FileCode2 className="size-4 text-primary" /> Blueprint
            </CardTitle>
            <p className="mt-1 text-xs text-muted-foreground">
              Canonical authored input only. Runtime ids, generated paths,
              observations, and secret values are excluded.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <BlueprintApplyAction
              environment={environment}
              workspace={workspace}
              disabled={applyBlocked}
              onIntentChange={refreshRecovery}
            />
            <Button
              variant="outline"
              size="sm"
              disabled={!snapshot}
              onClick={() => fileInput.current?.click()}
            >
              <FileUp /> Import file
            </Button>
            <Input
              ref={fileInput}
              className="hidden"
              type="file"
              accept=".yaml,.yml,application/yaml,text/yaml,text/plain"
              onChange={(event) => void importDocument(event)}
            />
            <Button
              variant="outline"
              size="sm"
              disabled={!snapshot}
              onClick={exportDocument}
            >
              <Download /> Export saved
            </Button>
            {snapshot && <CopyButton value={draft} label="copy displayed Blueprint" />}
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {recovery.error && <p role="alert" className="text-sm text-destructive">Apply is unavailable: {recovery.error}</p>}
          {pendingApply && (
            <div role="status" className="space-y-2 rounded-lg border border-warning/40 p-3 text-sm">
              <p>A Blueprint Apply is unresolved for Environment <code>{pendingApply.environmentId}</code>. Do not submit another Apply until the original Task settles.</p>
              {pendingApply.taskId ? <TaskLink taskId={pendingApply.taskId}>{pendingApply.taskId}</TaskLink> : (
                <Button size="sm" variant="outline" disabled={resolving} onClick={() => void resolveApply()}>
                  {resolving ? "Resolving…" : "Resolve original Apply"}
                </Button>
              )}
              {resolveError && <p role="alert" className="text-xs text-destructive">{resolveError}</p>}
            </div>
          )}
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div className="flex items-center gap-2">
              <Badge variant="outline">
                revision {snapshot?.revision ?? "loading"}
              </Badge>
              {editing && <Badge>local draft</Badge>}
            </div>
            <div className="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                disabled={loading || working}
                onClick={refresh}
              >
                <RefreshCw /> Refresh
              </Button>
              {!editing && (
                <Button
                  size="sm"
                  disabled={!snapshot}
                  onClick={() => setEditing(true)}
                >
                  <Pencil /> Edit
                </Button>
              )}
              {editing && (
                <>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => {
                      setDraft(snapshot?.document ?? "");
                      setEditing(false);
                      setError("");
                    }}
                  >
                    Cancel
                  </Button>
                  <Button
                    size="sm"
                    disabled={working || !draft.trim() || applyBlocked}
                    onClick={() => void reviewDraft()}
                  >
                    <Upload /> {working ? "Validating…" : "Review apply"}
                  </Button>
                </>
              )}
            </div>
          </div>
          {editing && <BlueprintApplyScope environment={environment} value={selectedService} onChange={setSelectedService} disabled={working} />}
          <CodeEditor
            id="environment-blueprint"
            label="Environment Blueprint YAML"
            language="yaml"
            value={loading ? "Loading Blueprint…" : draft}
            readOnly={!editing || loading}
            onValueChange={setDraft}
          />
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <p className="text-xs text-muted-foreground">
            Review the validation result before applying. Omitted Blueprint
            Entries are removed; other resource omissions may be retained or
            rejected until their removal workflows are supported.
          </p>
        </CardContent>
      </Card>

      {prepared && snapshot && (
        <TaskRunnerDialog
          open
          onOpenChange={(open) => {
            if (open) return;
            setPrepared(null);
            if (dispatched) void load();
          }}
          title={`Apply Blueprint · ${environment.name}`}
          description="Confirm the validated changes. Apply requires the reviewed base revision to remain current."
          type="update"
          target={environment.id}
          workspace={workspace}
          executionCopy="The Controller revision-fences the desired-state publication before scheduling reconciliation:"
          steps={[
            { label: "Recheck Blueprint revision", state: "pending" },
            { label: "Publish canonical desired revision", state: "pending" },
            { label: "Schedule Environment reconciliation", state: "pending" },
          ]}
          review={<BlueprintReview validation={prepared.validation} />}
          startLabel="Confirm and apply"
          onDispatch={async () => {
            try {
              const accepted = await store.applyBlueprint(
                environment.id,
                prepared.request,
                snapshot.revision,
                prepared.key,
              );
              setDispatched(true);
              return accepted.task_id;
            } finally {
              refreshRecovery();
            }
          }}
          variant="dialog"
        />
      )}
      <Dialog open={confirmRefresh} onOpenChange={setConfirmRefresh}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Discard local Blueprint changes?</DialogTitle>
            <DialogDescription>
              Refresh loads the saved Blueprint and replaces your unsaved draft.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmRefresh(false)}>
              Keep editing
            </Button>
            <Button onClick={() => { setConfirmRefresh(false); void load(); }}>
              Discard and refresh
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
