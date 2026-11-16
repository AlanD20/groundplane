import { useEffect, useRef, useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { TaskLink } from "@/components/common/task-link";
import { useStore } from "@/lib/store";
import { newULID } from "@/lib/utils";
import type { Environment } from "@/lib/types";
import type { RecoveryPointGroup } from "./recovery-point-groups";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { backupSourceLabel } from "./environment-backup-projection";

export function RecoveryGroupDeleteDialog({
  env,
  group,
  onClose,
}: {
  env: Environment;
  group: RecoveryPointGroup;
  onClose: () => void;
}) {
  const store = useStore();
  const [typed, setTyped] = useState("");
  const [running, setRunning] = useState(false);
  const [after, setAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const [error, setError] = useState("");
  const [tasks, setTasks] = useState<
    { point: string; id: string; status: string }[]
  >([]);
  const lifetime = useRef(new AbortController());
  const started = useRef(false);
  const keys = useRef(
    new Map(group.points.map((point) => [point.id, newULID()])),
  );
  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    return () => controller.abort();
  }, []);

  const taskIds = tasks.map((task) => task.id).join(",");
  const completed = tasks.filter((task) => task.status === "succeeded").length;
  const { getTask, loadRecoveryPoints } = store;
  useEffect(() => {
    if (!taskIds) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    async function refresh() {
      try {
        const details = await Promise.all(
          taskIds.split(",").map((id) => getTask(id, controller.signal)),
        );
        if (controller.signal.aborted) return;
        setTasks((prior) =>
          prior.map((item) => ({
            ...item,
            status:
              details.find((detail) => detail.task.id === item.id)?.task
                .status ?? item.status,
          })),
        );
        if (details.some((detail) => !detail.task.finished_at))
          timer = setTimeout(() => void refresh(), 1500);
        else await loadRecoveryPoints(env.id);
      } catch (cause) {
        if (!controller.signal.aborted)
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to refresh deletion Tasks",
          );
      }
    }
    void refresh();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [taskIds, getTask, loadRecoveryPoints, env.id]);

  async function remove() {
    if (started.current || afterLoading) return;
    started.current = true;
    setRunning(true);
    const signal = lifetime.current.signal;
    let prerequisites = [...after];
    try {
      for (const point of group.points) {
        if (signal.aborted) return;
        const id = await store.deleteRecoveryPoint(
          env.id,
          point.id,
          keys.current.get(point.id)!,
          prerequisites,
        );
        if (signal.aborted) return;
        setTasks((prior) => [
          ...prior,
          { point: point.id, id, status: "queued" },
        ]);
        prerequisites = [id];
      }
    } catch (failure) {
      if (!signal.aborted)
        setError(
          failure instanceof Error
            ? failure.message
            : "Unable to submit archive deletion",
        );
    } finally {
      if (!signal.aborted) setRunning(false);
    }
  }

  return (
    <Dialog
      open
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete this backup?</DialogTitle>
          <DialogDescription>
            Backup captured {new Date(group.createdAt).toLocaleString()}:
            permanently delete these {group.points.length} remaining archives.
            Source data is not changed. Each archive uses GP’s protected
            deletion.
          </DialogDescription>
        </DialogHeader>
        <ul className="space-y-2 text-sm">
          {group.points.map((point) => (
            <li key={point.id}>
              {backupSourceLabel(store, env, {
                id: point.sourceId,
                kind: point.sourceKind,
                targetId: point.targetId,
              })}
            </li>
          ))}
        </ul>
        {!started.current && (
          <div className="space-y-2">
            <Label htmlFor="confirm-backup-delete">
              Type {env.name} to confirm
            </Label>
            <Input
              id="confirm-backup-delete"
              value={typed}
              onChange={(event) => setTyped(event.target.value)}
              autoComplete="off"
            />
          </div>
        )}
        {!started.current && (
          <TaskRunAfterSelector
            value={after}
            onChange={setAfter}
            onBusyChange={setAfterLoading}
          />
        )}
        {tasks.length > 0 && (
          <div className="space-y-2 rounded-lg border border-border p-3 text-xs">
            <p>
              {completed} of {group.points.length} archives deleted
            </p>
            {tasks.map((task, index) => (
              <div
                key={task.id}
                className="flex items-center justify-between gap-3"
              >
                <TaskLink taskId={task.id}>Deletion {index + 1}</TaskLink>
                <span>{task.status}</span>
              </div>
            ))}
            {running && (
              <p className="text-muted-foreground">
                Accepted deletion Tasks continue on the server after this dialog
                closes. Each archive waits for the previous deletion to succeed.
              </p>
            )}
          </div>
        )}
        {error && (
          <p role="alert" className="text-sm text-destructive">
            {error}
          </p>
        )}
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            {running ? "Close and stop submissions" : "Close"}
          </Button>
          {!started.current && (
            <Button
              variant="destructive"
              disabled={typed !== env.name || afterLoading}
              onClick={() => void remove()}
            >
              Permanently delete backup
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
