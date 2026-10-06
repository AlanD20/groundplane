import { TaskDetailDrawer } from "@/components/common/task-detail-drawer";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { StatusBadge } from "@/components/common/status-badge";
import { useStore } from "@/lib/store";
import type { ActivityEntry } from "@/lib/types";
import { Bell, X } from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { useSearchParams } from "react-router-dom";
import {
  dismissAcceptedTask,
  markTaskNotificationsRead,
  useAcceptedTasks,
} from "./accepted-tasks";
import { requestTask } from "./api";
import { taskFromAPI } from "./journal-model";
import { taskPresentation } from "./task-overview";

export function TaskNotifications() {
  const notifications = useAcceptedTasks();
  const [open, setOpen] = useState(false);
  const [tasks, setTasks] = useState<Record<string, ActivityEntry>>({});
  const taskCache = useRef<Record<string, ActivityEntry>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const unread = notifications.filter((item) => item.unread).length;
  const toasts = notifications.filter((item) => item.toast).slice(0, 3);
  // One polling loop owns all accepted Tasks; opening the center adds no requests.
  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      const pending = notifications.filter(
        ({ id }) =>
          !taskCache.current[id] || !isTerminal(taskCache.current[id]),
      );
      await Promise.all(
        pending.map(async ({ id }) => {
          try {
            const task = taskFromAPI(await requestTask(id, controller.signal));
            if (controller.signal.aborted) return;
            taskCache.current[id] = task;
            setTasks((current) => ({ ...current, [id]: task }));
            setErrors((current) => {
              const next = { ...current };
              delete next[id];
              return next;
            });
          } catch (error) {
            if (!controller.signal.aborted)
              setErrors((current) => ({
                ...current,
                [id]:
                  error instanceof Error
                    ? error.message
                    : "Unable to load Task",
              }));
          }
        }),
      );
      if (pending.length && !controller.signal.aborted)
        timer = setTimeout(() => void poll(), 2000);
    };
    void poll();
    return () => {
      controller.abort();
      if (timer) clearTimeout(timer);
    };
  }, [notifications]);

  return (
    <>
      <Button
        variant="ghost"
        size="icon"
        className="relative"
        aria-label={`Task notifications${unread ? `, ${unread} unread` : ""}`}
        onClick={() => {
          setOpen(true);
          markTaskNotificationsRead();
        }}
      >
        <Bell className="size-4" />
        {!!unread && (
          <span className="absolute right-1 top-1 size-2 rounded-full bg-primary" />
        )}
      </Button>
      {createPortal(
        <section
          aria-label="Task updates"
          aria-live="polite"
          className="pointer-events-none fixed right-4 top-20 z-40 flex w-[min(380px,calc(100vw-2rem))] flex-col gap-2"
        >
          {toasts.map(({ id }) => (
            <TaskToast key={id} id={id}>
              <TaskNotificationRow
                id={id}
                task={tasks[id]}
                error={errors[id]}
              />
              <div className="mt-3 flex items-center justify-between">
                <TaskLink taskId={id} onClick={() => dismissAcceptedTask(id)}>
                  Open Task
                </TaskLink>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  aria-label="Dismiss notification"
                  onClick={() => dismissAcceptedTask(id)}
                >
                  <X className="size-3.5" />
                </Button>
              </div>
            </TaskToast>
          ))}
        </section>,
        document.body,
      )}
      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>Task notifications</DialogTitle>
          </DialogHeader>
          <div className="space-y-3">
            {!notifications.length && (
              <p className="text-sm text-muted-foreground">
                Operations you start in this session appear here.
              </p>
            )}
            {notifications.map(({ id }) => (
              <div
                key={id}
                className="space-y-3 rounded-lg border border-border p-4"
              >
                <TaskNotificationRow
                  id={id}
                  task={tasks[id]}
                  error={errors[id]}
                />
                <TaskLink taskId={id} onClick={() => setOpen(false)}>
                  Open Task
                </TaskLink>
              </div>
            ))}
          </div>
        </DrawerContent>
      </Drawer>
    </>
  );
}

function TaskToast({ id, children }: { id: string; children: ReactNode }) {
  // Lifetime belongs to this visible toast, not to polling or Task completion.
  // Dismissal retains the Task and its unread state in the notification center.
  useEffect(() => {
    const timer = setTimeout(() => dismissAcceptedTask(id), 8000);
    return () => clearTimeout(timer);
  }, [id]);
  return (
    <div className="pointer-events-auto rounded-xl border border-border bg-card p-4 shadow-lg animate-in fade-in slide-in-from-top-2">
      {children}
    </div>
  );
}

function isTerminal(task: ActivityEntry) {
  return ["completed", "failed", "timed_out", "aborted"].includes(task.status);
}

function TaskNotificationRow({
  task,
  error,
}: {
  id: string;
  task?: ActivityEntry;
  error?: string;
}) {
  const store = useStore();
  return (
    <div className="space-y-2">
      <p className="break-words text-sm font-medium">
        {task ? taskPresentation(task, store).title : "Operation accepted"}
      </p>
      {task && <StatusBadge status={task.status} />}
      {error && (
        <p className="text-xs text-destructive">Status unavailable: {error}</p>
      )}
    </div>
  );
}

export function LinkedTask() {
  const [search, setSearch] = useSearchParams();
  const id = search.get("task");
  if (!id) return null;
  return (
    <TaskDetailDrawer
      key={id}
      taskId={id}
      scope={{ kind: "all" }}
      surface="tasks"
      onOpenChange={(open) => {
        if (!open)
          setSearch((current) => {
            const next = new URLSearchParams(current);
            next.delete("task");
            return next;
          });
      }}
    />
  );
}
