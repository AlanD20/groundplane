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
  const [completed, setCompleted] = useState(0);
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

  async function remove() {
    if (started.current) return;
    started.current = true;
    setRunning(true);
    const signal = lifetime.current.signal;
    try {
      for (const point of group.points) {
        if (signal.aborted) return;
        const id = await store.deleteRecoveryPoint(
          env.id,
          point.id,
          keys.current.get(point.id)!,
        );
        if (signal.aborted) return;
        setTasks((prior) => [
          ...prior,
          { point: point.id, id, status: "queued" },
        ]);
        for (;;) {
          const task = await store.getTask(id, signal);
          if (signal.aborted) return;
          setTasks((prior) =>
            prior.map((item) =>
              item.id === id ? { ...item, status: task.status } : item,
            ),
          );
          if (task.status === "completed") break;
          if (["failed", "aborted", "timed_out"].includes(task.status)) {
            throw new Error(
              "Deletion stopped. Inspect the linked Task; no further archives were selected.",
            );
          }
          await new Promise<void>((resolve) => {
            const finish = () => {
              clearTimeout(timer);
              signal.removeEventListener("abort", finish);
              resolve();
            };
            const timer = setTimeout(finish, 1500);
            signal.addEventListener("abort", finish, { once: true });
          });
          if (signal.aborted) return;
        }
        setCompleted((value) => value + 1);
        await store.loadRecoveryPoints(env.id);
      }
    } catch (failure) {
      if (!signal.aborted)
        setError(
          failure instanceof Error
            ? failure.message
            : "Unable to delete backup archives",
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
                Closing stops the queue. An already accepted Task continues.
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
            {running ? "Close and stop queue" : "Close"}
          </Button>
          {!started.current && (
            <Button
              variant="destructive"
              disabled={typed !== env.name}
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
