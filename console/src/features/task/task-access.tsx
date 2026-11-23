import { TaskDetailDrawer } from "@/components/common/task-detail-drawer";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { StatusBadge } from "@/components/common/status-badge";
import { useStore } from "@/lib/store";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import type { ActivityEntry } from "@/lib/types";
import { Bell, Check, CheckCheck, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useSearchParams } from "react-router-dom";
import {
  dismissAcceptedTask,
  expireTaskToasts,
  markTaskNotificationsRead,
  readTaskNotification,
  useAcceptedTasks,
} from "./accepted-tasks";
import { requestTask } from "./api";
import { taskFromAPI } from "./journal-model";
import { taskPresentation } from "./task-overview";

type TaskUpdate = { task?: ActivityEntry; error?: string };

export function TaskNotifications() {
  const store = useStore();
  const notifications = useAcceptedTasks();
  const [open, setOpen] = useState(false);
  const [focusedToast, setFocusedToast] = useState<string>();
  const [updates, setUpdates] = useState<Record<string, TaskUpdate>>({});
  const trigger = useRef<HTMLButtonElement>(null);
  const drawerTitle = useRef<HTMLHeadingElement>(null);
  const unread = notifications.filter((item) => item.unread).length;
  const now = Date.now();
  const activeToasts = notifications.filter(
    ({ id, toastUntil }) =>
      toastUntil !== undefined && (toastUntil > now || id === focusedToast),
  );
  const toasts = activeToasts.slice(0, 3);
  const focused = activeToasts.find(({ id }) => id === focusedToast);
  if (focused && !toasts.includes(focused)) toasts[2] = focused;
  const nextExpiry = Math.min(
    ...notifications.flatMap(({ id, toastUntil }) =>
      id === focusedToast ? [] : (toastUntil ?? []),
    ),
  );

  useEffect(() => {
    if (!Number.isFinite(nextExpiry)) return;
    const expire = () => expireTaskToasts(Date.now(), focusedToast);
    const timer = window.setTimeout(
      expire,
      Math.max(0, nextExpiry - Date.now()),
    );
    return () => window.clearTimeout(timer);
  }, [nextExpiry, focusedToast]);

  // New acceptances queue refreshes without canceling reads already in flight.
  useVisibleRefresh(
    async (signal) => {
      const pending = notifications.filter(
        ({ id }) => !updates[id]?.task?.finishedAt,
      );
      if (!pending.length) return;
      const results = await Promise.all(
        pending.map(async ({ id }): Promise<[string, TaskUpdate]> => {
          try {
            const { task } = await requestTask(id, signal, { limit: 1 });
            return [id, { task: taskFromAPI(task) }];
          } catch {
            return [
              id,
              {
                task: updates[id]?.task,
                error: "Status temporarily unavailable",
              },
            ];
          }
        }),
      );
      if (signal.aborted) return;
      setUpdates((current) => {
        const next: Record<string, TaskUpdate> = {};
        for (const { id } of notifications) {
          if (current[id]) next[id] = current[id];
        }
        let changed = Object.keys(next).length !== Object.keys(current).length;
        for (const [id, update] of results) {
          const previous = current[id];
          if (
            previous &&
            previous.error === update.error &&
            previous.task?.updatedAt === update.task?.updatedAt &&
            previous.task?.status === update.task?.status &&
            previous.task?.finishedAt === update.task?.finishedAt
          )
            continue;
          next[id] = update;
          changed = true;
        }
        return changed ? next : current;
      });
    },
    { enabled: notifications.length > 0, interval: 2_000 },
  );

  const notificationLink = (id: string, onClick: () => void) => {
    const update = updates[id];
    const task = update?.task;
    const title = task?.targetName
      ? task.title
      : task
        ? taskPresentation(task, store).title
        : "Operation accepted";
    return (
      <TaskLink
        taskId={id}
        label={`Open Task: ${title}`}
        onClick={onClick}
        className="flex min-w-0 flex-1 justify-between gap-3 rounded-lg px-3 py-2.5 text-foreground no-underline hover:bg-muted [&>span]:break-words"
      >
        <span className="block text-sm font-medium">{title}</span>
        <span className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
          {task && <StatusBadge status={task.status} />}
          {update?.error}
        </span>
      </TaskLink>
    );
  };

  return (
    <>
      <Button
        ref={trigger}
        variant="ghost"
        size="icon"
        className="relative"
        aria-label={`Task notifications${unread ? `, ${unread} unread` : ""}`}
        aria-expanded={open}
        aria-haspopup="dialog"
        onClick={() => setOpen(true)}
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
          className="pointer-events-none fixed inset-x-4 top-20 z-40 ml-auto flex max-w-sm flex-col gap-2"
        >
          {!open &&
            toasts.map(({ id }) => (
              <div
                key={id}
                onFocus={() => setFocusedToast(id)}
                onBlur={(event) => {
                  if (!event.currentTarget.contains(event.relatedTarget))
                    setFocusedToast((current) =>
                      current === id ? undefined : current,
                    );
                }}
                className="pointer-events-auto flex items-start gap-1 rounded-xl border border-border bg-card p-1.5 shadow-lg motion-safe:animate-in motion-safe:fade-in motion-safe:slide-in-from-top-2"
              >
                {notificationLink(id, () => {
                  trigger.current?.focus({ preventScroll: true });
                  readTaskNotification(id);
                })}
                <Button
                  variant="ghost"
                  size="icon-xs"
                  className="mt-2 shrink-0"
                  aria-label="Dismiss Task popup"
                  onClick={() => {
                    trigger.current?.focus({ preventScroll: true });
                    dismissAcceptedTask(id);
                  }}
                >
                  <X className="size-3.5" />
                </Button>
              </div>
            ))}
        </section>,
        document.body,
      )}
      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent
          className="max-w-md gap-4"
          initialFocus={drawerTitle}
          finalFocus={trigger}
        >
          <DialogHeader className="flex-row flex-wrap items-center justify-between gap-2 pr-8">
            <DialogTitle ref={drawerTitle} tabIndex={-1}>
              Task notifications
            </DialogTitle>
            {!!unread && (
              <Button
                type="button"
                variant="ghost"
                size="sm"
                onClick={() => {
                  drawerTitle.current?.focus({ preventScroll: true });
                  markTaskNotificationsRead();
                }}
              >
                <CheckCheck className="size-4" /> Mark all as read
              </Button>
            )}
          </DialogHeader>
          {notifications.length ? (
            <ul className="divide-y divide-border rounded-xl border border-border">
              {notifications.map(({ id, unread }) => (
                <li key={id} className="flex items-start gap-1 px-1">
                  {unread && (
                    <span
                      aria-label="Unread"
                      role="img"
                      className="mt-4 size-2 shrink-0 rounded-full bg-primary"
                    />
                  )}
                  {notificationLink(id, () => {
                    readTaskNotification(id);
                    setOpen(false);
                  })}
                  {unread && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon-sm"
                      className="mt-1.5 shrink-0"
                      aria-label="Mark notification as read"
                      title="Mark as read"
                      onClick={(event) => {
                        event.currentTarget
                          .closest("li")
                          ?.querySelector("a")
                          ?.focus({ preventScroll: true });
                        readTaskNotification(id);
                      }}
                    >
                      <Check className="size-4" />
                    </Button>
                  )}
                </li>
              ))}
            </ul>
          ) : (
            <p className="rounded-xl border border-dashed border-border p-4 text-sm text-muted-foreground">
              Tasks you start in this session appear here.
            </p>
          )}
        </DrawerContent>
      </Drawer>
    </>
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
