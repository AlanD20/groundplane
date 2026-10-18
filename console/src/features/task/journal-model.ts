import type { operations } from "@/lib/api.generated";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalState,
  TaskPageSize,
  TaskStatus,
  TaskType,
} from "@/lib/types";
import { taskTypeFromAPI } from "./task-type";

export type TaskPageResponse =
  operations["task.list"]["responses"][200]["content"]["application/json"];
export type TaskPageItem = NonNullable<TaskPageResponse["items"]>[number];
export type TaskEventResponse =
  operations["task.events"]["responses"][200]["content"]["text/event-stream"][number]["data"];

const taskStatuses = new Set<TaskStatus>([
  "queued",
  "preparing",
  "waiting",
  "running",
  "attention_required",
  "stopping",
  "finishing",
  "succeeded",
  "failed",
  "cancelled",
  "superseded",
]);
export function emptyTaskJournal(pageSize: TaskPageSize): TaskJournalState {
  return {
    entries: [],
    pageSize,
    pageIndex: 0,
    pageCursors: [undefined],
    nextCursor: null,
    loaded: false,
    loading: false,
    loadingMore: false,
    loadError: null,
    failedCursor: null,
  };
}
export function taskJournalKey(scope: TaskJournalScope): string {
  const filters = `:${scope.status ?? ""}:${scope.taskType ?? ""}:${scope.resourceKind ?? ""}`;
  switch (scope.kind) {
    case "all":
      return "all" + filters;
    case "workspace":
      return `workspace:${scope.workspace}` + filters;
    case "project":
      return `project:${scope.projectId}` + filters;
    case "environment":
      return `environment:${scope.environmentId}` + filters;
  }
}
export function taskJournalQuery(
  scope: TaskJournalScope,
  cursor?: string,
  pageSize: TaskPageSize = 5,
): string {
  const query = new URLSearchParams({ limit: String(pageSize) });
  query.set(
    "scope",
    scope.kind === "all"
      ? "all"
      : scope.kind === "workspace"
        ? scope.workspace
        : scope.kind === "project"
          ? scope.projectId
          : scope.environmentId,
  );
  if (scope.status) query.set("status", scope.status);
  if (scope.taskType) query.set("action", scope.taskType);
  if (scope.resourceKind) query.set("resource_kind", scope.resourceKind);
  if (cursor) {
    const separator = cursor.indexOf(":");
    if (separator < 1) throw new Error("Task page cursor is invalid");
    query.set("revision", cursor.slice(0, separator));
    query.set("after", cursor.slice(separator + 1));
  }
  return query.toString();
}
export function taskJournalCursor(page: TaskPageResponse): string | null {
  if (!page.more) return null;
  if (!page.after || !Number.isSafeInteger(page.revision) || page.revision < 1)
    throw new Error("Controller returned an incomplete Task page cursor");
  return `${page.revision}:${page.after}`;
}
function requiredTaskTimestamp(value: string, field: string): string {
  if (Number.isNaN(Date.parse(value)))
    throw new Error(`Controller returned invalid Task ${field}`);
  return value;
}
function taskTitle(
  type: TaskType,
  target: string,
  resourceKind?: string,
): string {
  const labels: Record<TaskType, string> = {
    deploy: "Deploy",
    rollback: "Rollback",
    backup: "Backup",
    backup_prune: "Prune backups",
    restore: "Restore",
    attach: "Attach",
    detach: "Detach",
    run: "Run",
    script: "Run script",
    provision: "Provision",
    create: "Create",
    start: "Start",
    stop: "Stop",
    destroy: "Remove containers",
    remove: resourceKind === "service" ? "Delete Service" : "Remove",
    update: "Update",
    rotate: "Rotate",
    fetch: "Fetch image",
    prepare: "Prepare software",
    apply: "Apply software",
    check: "Test connection",
  };
  return `${labels[type]} · ${target}`;
}
export function taskFromAPI(task: TaskPageItem): ActivityEntry {
  if (!taskStatuses.has(task.status as TaskStatus))
    throw new Error(`Controller returned unknown Task status ${task.status}`);
  const createdAt = requiredTaskTimestamp(task.created_at, "created_at");
  const updatedAt = requiredTaskTimestamp(task.updated_at, "updated_at");
  const type = taskTypeFromAPI(task.action, task.actor);
  return {
    id: task.id,
    type,
    title: taskTitle(type, task.target_name || task.target_id),
    target: task.target_id,
    targetName: task.target_name,
    resourceKind: task.resource_kind,
    workspace: task.scope.tenant_id || "platform",
    workspaceType: task.scope.tenant_id ? "tenant" : "platform",
    tenantId: task.scope.tenant_id,
    projectId: task.scope.project_id,
    environmentId: task.scope.environment_id,
    status: task.status as TaskStatus,
    actor: task.actor,
    ts: updatedAt,
    createdAt,
    updatedAt,
    finishedAt: task.finished_at ?? null,
    counts: task.counts,
    cancel: task.cancel,
  };
}

export function requireTaskId(
  response: { task_id?: string | null },
  operation: string,
): string {
  if (typeof response.task_id !== "string" || response.task_id.length === 0) {
    throw new Error(`Controller response is missing ${operation} task_id`);
  }
  return response.task_id;
}
