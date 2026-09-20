import type { operations } from "@/lib/api.generated";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalState,
  TaskStatus,
  TaskStep,
  TaskType,
} from "@/lib/types";
import { taskTypeFromAPI } from "./task-type";

export type TaskPageResponse =
  operations["task.list"]["responses"][200]["content"]["application/json"];
export type TaskPageItem = NonNullable<TaskPageResponse["items"]>[number];
export type TaskEventResponse =
  operations["task.events"]["responses"][200]["content"]["text/event-stream"][number]["data"];

const taskStatuses = new Set<TaskStatus>([
  "pending",
  "running",
  "completed",
  "failed",
  "timed_out",
  "aborted",
]);
export function emptyTaskJournal(): TaskJournalState {
  return {
    entries: [],
    nextCursor: null,
    loaded: false,
    loading: false,
    loadingMore: false,
    loadError: null,
    failedCursor: null,
  };
}
export function taskJournalKey(scope: TaskJournalScope): string {
  switch (scope.kind) {
    case "all":
      return "all";
    case "workspace":
      return `workspace:${scope.workspace}`;
    case "project":
      return `project:${scope.projectId}`;
    case "environment":
      return `environment:${scope.environmentId}`;
  }
}
export function taskJournalQuery(
  scope: TaskJournalScope,
  cursor?: string,
): string {
  const query = new URLSearchParams({ limit: "50" });
  if (cursor) query.set("cursor", cursor);
  if (scope.kind === "workspace") query.set("workspace", scope.workspace);
  if (scope.kind === "project") query.set("project", scope.projectId);
  if (scope.kind === "environment")
    query.set("environment", scope.environmentId);
  return query.toString();
}
function requiredTaskTimestamp(value: string, field: string): string {
  if (Number.isNaN(Date.parse(value)))
    throw new Error(`Controller returned invalid Task ${field}`);
  return value;
}
function taskStepState(status: string): TaskStep["state"] {
  switch (status) {
    case "pending":
      return "pending";
    case "completed":
      return "done";
    case "running":
      return "running";
    case "failed":
    case "timed_out":
    case "aborted":
      return "failed";
    default:
      throw new Error(`Controller returned unknown Task step status ${status}`);
  }
}
function taskTitle(type: TaskType, target: string): string {
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
    destroy: "Destroy",
    remove: "Remove",
    update: "Update",
    rotate: "Rotate",
  };
  return `${labels[type]} · ${target}`;
}
export function taskFromAPI(value: TaskPageItem): ActivityEntry {
  const task = value;
  if (!taskStatuses.has(task.status as TaskStatus))
    throw new Error(`Controller returned unknown Task status ${task.status}`);
  if (task.workspace_type !== "platform" && task.workspace_type !== "tenant") {
    throw new Error(
      `Controller returned unknown Task workspace ${task.workspace_type}`,
    );
  }
  if (task.actor !== "operator" && task.actor !== "system") {
    throw new Error(`Controller returned unknown Task actor ${task.actor}`);
  }
  if (task.workspace_type === "tenant" && !task.tenant_id) {
    throw new Error(
      "Controller returned a Tenant-owned Task without tenant_id",
    );
  }
  if (task.workspace_type === "platform" && task.tenant_id) {
    throw new Error("Controller returned a Platform-owned Task with tenant_id");
  }
  if (task.environment_id && !task.project_id) {
    throw new Error(
      "Controller returned an Environment-owned Task without project_id",
    );
  }

  const createdAt = requiredTaskTimestamp(task.created_at, "created_at");
  const updatedAt = requiredTaskTimestamp(task.updated_at, "updated_at");
  const startedAt = task.started_at
    ? requiredTaskTimestamp(task.started_at, "started_at")
    : null;
  const finishedAt = task.finished_at
    ? requiredTaskTimestamp(task.finished_at, "finished_at")
    : null;
  const type = taskTypeFromAPI(task.type, task.actor);

  return {
    id: task.id,
    operationId: task.operation_id,
    retryOf: task.retry_of,
    planHash: task.plan_hash,
    type,
    title: taskTitle(type, task.target),
    target: task.target,
    workspace:
      task.workspace_type === "platform" ? "platform" : task.tenant_id!,
    workspaceType: task.workspace_type,
    tenantId: task.tenant_id ?? undefined,
    projectId: task.project_id ?? undefined,
    environmentId: task.environment_id ?? undefined,
    status: task.status as TaskStatus,
    actor: task.actor,
    ts: updatedAt,
    createdAt,
    updatedAt,
    startedAt,
    finishedAt,
    steps: task.steps?.map((step) => ({
      label: step.name,
      state: taskStepState(step.status),
    })),
  };
}

const taskEventKeys = [
  "attempt",
  "ordinal",
  "received_at",
  "sequence",
  "state",
  "step_id",
];

function isTaskEventState(value: unknown): value is TaskEventResponse["state"] {
  return (
    value === "pending" ||
    value === "running" ||
    value === "completed" ||
    value === "failed" ||
    value === "aborted" ||
    value === "timed_out"
  );
}

export function parseTaskEvent(data: string): TaskEventResponse {
  const value: unknown = JSON.parse(data);
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("task event must be an object");
  }

  const event = value as Record<string, unknown>;
  const keys = Object.keys(event).sort();
  if (
    keys.length !== taskEventKeys.length ||
    keys.some((key, index) => key !== taskEventKeys[index])
  ) {
    throw new Error("task event has unexpected fields");
  }
  if (
    typeof event.sequence !== "number" ||
    !Number.isSafeInteger(event.sequence) ||
    event.sequence < 1
  ) {
    throw new Error("task event sequence is invalid");
  }
  if (typeof event.step_id !== "string" || event.step_id.length === 0) {
    throw new Error("task event step id is invalid");
  }
  if (!isTaskEventState(event.state)) {
    throw new Error("task event state is invalid");
  }
  if (
    typeof event.attempt !== "number" ||
    !Number.isSafeInteger(event.attempt) ||
    event.attempt < 1
  ) {
    throw new Error("task event attempt is invalid");
  }
  if (
    typeof event.ordinal !== "number" ||
    !Number.isSafeInteger(event.ordinal) ||
    event.ordinal < 1
  ) {
    throw new Error("task event ordinal is invalid");
  }
  if (
    typeof event.received_at !== "string" ||
    Number.isNaN(Date.parse(event.received_at))
  ) {
    throw new Error("task event received_at is invalid");
  }

  return {
    attempt: event.attempt,
    ordinal: event.ordinal,
    received_at: event.received_at,
    sequence: event.sequence,
    state: event.state,
    step_id: event.step_id,
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
