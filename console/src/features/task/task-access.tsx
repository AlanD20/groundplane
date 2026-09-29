import { TaskDetailDrawer } from "@/components/common/task-detail-drawer";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { useStore } from "@/lib/store";
import type { ActivityEntry } from "@/lib/types";
import { CheckCircle2, X } from "lucide-react";
import { useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { dismissAcceptedTask, useAcceptedTasks } from "./accepted-tasks";
import { requestTask } from "./api";
import { taskFromAPI } from "./journal-model";
import { taskPresentation } from "./task-overview";

export function AcceptedTasks() {
  const tasks = useAcceptedTasks();
  if (!tasks.length) return null;
  return (
    <section
      aria-label="Recently triggered Tasks"
      aria-live="polite"
      className="mb-5 divide-y divide-border rounded-lg border border-primary/20 bg-card"
    >
      {tasks.map((id) => (
        <AcceptedTask key={id} id={id} />
      ))}
    </section>
  );
}

function AcceptedTask({ id }: { id: string }) {
  const store = useStore();
  const [task, setTask] = useState<ActivityEntry>();
  useEffect(() => {
    const controller = new AbortController();
    void requestTask(id, controller.signal)
      .then((response) => {
        if (!controller.signal.aborted) setTask(taskFromAPI(response));
      })
      .catch(() => undefined);
    return () => controller.abort();
  }, [id]);
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-3 px-3 py-2 text-xs">
      <CheckCircle2 className="size-3.5 shrink-0 text-primary" />
      <span className="min-w-0 flex-1 truncate">
        {task ? taskPresentation(task, store).title : "Operation accepted"}
      </span>
      <TaskLink taskId={id}>Inspect Task</TaskLink>
      <Button
        variant="ghost"
        size="icon-xs"
        aria-label={`Dismiss Task ${id}`}
        onClick={() => dismissAcceptedTask(id)}
      >
        <X className="size-3.5" />
      </Button>
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
