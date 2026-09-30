import { useCallback, useEffect, useRef, useState } from "react";
import { Database, RefreshCw, Save } from "lucide-react";
import type { operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel } from "@/components/common/resource-panel";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { CodeEditor } from "@/components/ui/code-editor";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { requestTask } from "@/features/task/api";
import { newULID } from "@/lib/utils";

type Document =
  operations["etcd.config.show"]["responses"][200]["content"]["application/json"];
type Accepted =
  operations["etcd.config.apply"]["responses"][202]["content"]["application/json"];

export default function PlatformEtcdPage() {
  const [document, setDocument] = useState<Document>();
  const [content, setContent] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [confirm, setConfirm] = useState(false);
  const [taskID, setTaskID] = useState<string>();
  const applyIntent = useRef<{ revision: string; key: string } | undefined>(
    undefined,
  );
  const load = useCallback(async (signal?: AbortSignal) => {
    const next = await controllerRequest<Document>("/etcd/config", 200, {
      signal,
    });
    if (!signal?.aborted) {
      setDocument(next);
      setContent(next.content);
    }
  }, []);
  useEffect(() => {
    const controller = new AbortController();
    void load(controller.signal).catch((cause) => {
      if (!controller.signal.aborted) setError(message(cause));
    });
    return () => controller.abort();
  }, [load]);
  useEffect(() => {
    if (!taskID) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        const task = await requestTask(taskID, controller.signal);
        if (controller.signal.aborted) return;
        if (
          ["completed", "failed", "aborted", "timed_out"].includes(task.status)
        ) {
          await load(controller.signal);
          if (!controller.signal.aborted) {
            setTaskID(undefined);
            setError(
              task.status === "completed"
                ? undefined
                : `Apply ${task.status}. Open the Task for its recorded result.`,
            );
          }
          return;
        }
      } catch {
        /* etcd activation briefly interrupts control-plane reads. */
      }
      if (!controller.signal.aborted)
        timer = setTimeout(() => void poll(), 2000);
    };
    void poll();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [taskID, load]);
  const dirty = document !== undefined && content !== document.content;
  async function save() {
    if (!document) return;
    setBusy(true);
    setError(undefined);
    try {
      const next = await controllerRequest<Document>("/etcd/config", 200, {
        method: "PUT",
        body: { content, expected_revision: document.revision },
      });
      setDocument(next);
      setContent(next.content);
      applyIntent.current = undefined;
    } catch (cause) {
      setError(message(cause));
    } finally {
      setBusy(false);
    }
  }
  async function apply() {
    if (!document) return;
    setBusy(true);
    setError(undefined);
    if (applyIntent.current?.revision !== document.revision)
      applyIntent.current = { revision: document.revision, key: newULID() };
    try {
      const accepted = await controllerRequest<Accepted>(
        "/etcd/config/apply",
        202,
        {
          method: "POST",
          body: { expected_revision: document.revision },
          idempotencyKey: applyIntent.current.key,
        },
      );
      setTaskID(accepted.task_id);
      setConfirm(false);
      applyIntent.current = undefined;
    } catch (cause) {
      setError(message(cause));
    } finally {
      setBusy(false);
    }
  }
  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="etcd"
        icon={<Database />}
        description="Control-plane store configuration"
      />
      <ResourcePanel
        title="Configuration"
        actions={
          <Button
            variant="outline"
            disabled={busy || !!taskID}
            onClick={() =>
              void load().catch((cause) => setError(message(cause)))
            }
          >
            <RefreshCw />
            Reload
          </Button>
        }
      >
        <div className="space-y-4 p-5">
          {document ? (
            <>
              <p className="text-xs text-muted-foreground">{document.path}</p>
              <CodeEditor
                id="etcd-config-document"
                label="etcd YAML"
                language="yaml"
                value={content}
                onValueChange={setContent}
                disabled={busy || !!taskID}
              />
              <p className="text-sm text-muted-foreground">
                Save validates the YAML without restarting. Apply activates the
                saved configuration by restarting only etcd.
              </p>
              {document.apply_required && (
                <p
                  role="status"
                  className="rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm"
                >
                  Saved changes are not active yet.
                </p>
              )}
              <div className="flex gap-2">
                <Button
                  disabled={!dirty || busy || !!taskID}
                  onClick={() => void save()}
                >
                  <Save />
                  Save
                </Button>
                <Button
                  variant="outline"
                  disabled={
                    dirty || busy || !!taskID || !document.apply_required
                  }
                  onClick={() => setConfirm(true)}
                >
                  Apply saved configuration
                </Button>
              </div>
            </>
          ) : (
            <p className="text-sm text-muted-foreground">
              {error ? "Configuration unavailable." : "Loading configuration…"}
            </p>
          )}
          {taskID && (
            <p
              role="status"
              className="flex flex-wrap items-center gap-2 text-sm"
            >
              Applying. Control-plane requests may briefly reconnect.
              <TaskLink taskId={taskID}>Open Task</TaskLink>
            </p>
          )}
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
        </div>
      </ResourcePanel>
      <Dialog open={confirm} onOpenChange={setConfirm}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Apply etcd configuration?</DialogTitle>
            <DialogDescription>
              etcd will restart briefly. Controller API requests may be
              unavailable during activation. Application containers and their
              connections are not restarted. If activation fails, GP restores
              the previous etcd configuration.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirm(false)}>
              Cancel
            </Button>
            <Button disabled={busy} onClick={() => void apply()}>
              {busy ? "Submitting…" : "Apply"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
function message(cause: unknown) {
  return cause instanceof Error
    ? cause.message
    : "Unable to update etcd configuration";
}
