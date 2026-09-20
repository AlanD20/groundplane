"use client";

import { Input } from "@/components/ui/input";

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
import { BlueprintApplyAction } from "@/features/blueprint/blueprint-apply-action";
import {
  createBlueprintTextApplyRequest,
  type BlueprintApplyRequest,
} from "@/lib/blueprint-bundle";
import { useStore } from "@/lib/store";
import {
  type BlueprintDocumentResponse,
  type BlueprintValidationResponse,
} from "./api";
import type { Environment } from "@/lib/types";

type PreparedBlueprint = {
  request: BlueprintApplyRequest;
  validation: BlueprintValidationResponse;
};

export function BlueprintWorkspace({
  environment,
  workspace,
}: {
  environment: Environment;
  workspace: string;
}) {
  const store = useStore();
  const fileInput = useRef<HTMLInputElement>(null);
  const [snapshot, setSnapshot] = useState<BlueprintDocumentResponse | null>(
    null,
  );
  const [draft, setDraft] = useState("");
  const [editing, setEditing] = useState(false);
  const [loading, setLoading] = useState(true);
  const [working, setWorking] = useState(false);
  const [error, setError] = useState("");
  const [prepared, setPrepared] = useState<PreparedBlueprint | null>(null);
  const [dispatched, setDispatched] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const current = await store.getBlueprint(environment.id);
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
  }, [environment.id, store]);

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
      new Blob([draft], { type: "application/yaml;charset=utf-8" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `${environment.name}.blueprint.yaml`;
    link.click();
    URL.revokeObjectURL(url);
  }

  async function reviewDraft() {
    if (!snapshot) return;
    setWorking(true);
    setError("");
    try {
      const request = await createBlueprintTextApplyRequest(draft);
      const validation = await store.validateBlueprint(
        environment.id,
        request,
        snapshot.revision,
      );
      setPrepared({ request, validation });
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
              <Download /> Export
            </Button>
            {snapshot && <CopyButton value={draft} label="copy" />}
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
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
                onClick={() => void load()}
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
                    disabled={working || !draft.trim()}
                    onClick={() => void reviewDraft()}
                  >
                    <Upload /> {working ? "Validating…" : "Review apply"}
                  </Button>
                </>
              )}
            </div>
          </div>
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
            Omitted existing resources are retained. Destruction remains
            available only through each resource's explicit Remove action.
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
          description="Apply the validated document only if its base revision is still current."
          type="update"
          target={environment.id}
          workspace={workspace}
          executionCopy="The Controller revision-fences the desired-state publication before scheduling reconciliation:"
          steps={[
            { label: "Recheck Blueprint revision", state: "pending" },
            { label: "Publish canonical desired revision", state: "pending" },
            { label: "Schedule Environment reconciliation", state: "pending" },
          ]}
          review={
            <div className="max-h-64 space-y-1 overflow-y-auto rounded-lg border border-border bg-surface p-3 text-xs">
              {(prepared.validation.changes ?? []).map((change) => (
                <div
                  key={`${change.resource}:${change.key}:${change.action}`}
                  className="flex gap-2"
                >
                  <Badge variant="outline">{change.action}</Badge>
                  <code>
                    {change.resource}/{change.key}
                  </code>
                </div>
              ))}
            </div>
          }
          startLabel="Apply Blueprint"
          onDispatch={async () => {
            const accepted = await store.applyBlueprint(
              environment.id,
              prepared.request,
              snapshot.revision,
            );
            setDispatched(true);
            return accepted.task_id;
          }}
          variant="drawer"
        />
      )}
    </>
  );
}
