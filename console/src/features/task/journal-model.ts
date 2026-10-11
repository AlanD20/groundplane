import type { operations } from "@/lib/api.generated";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalState,
  TaskPageSize,
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
  if (scope.status) query.set("status", scope.status);
  if (scope.taskType) query.set("type", scope.taskType);
  if (scope.resourceKind) query.set("resource_kind", scope.resourceKind);
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
  if (
    task.executor !== undefined &&
    task.executor !== "agent" &&
    task.executor !== "controller" &&
    task.executor !== "blueprint" &&
    task.executor !== "software"
  ) {
    throw new Error(
      `Controller returned unknown Task executor ${task.executor}`,
    );
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
    reconciliationRequired: task.reconciliation_required,
    operationId: task.operation_id,
    retryOf: task.retry_of,
    planHash: task.plan_hash,
    type,
    title: taskTitle(
      type,
      task.image_fetch?.requested ?? task.target_name ?? task.target,
      task.resource_kind,
    ),
    imageFetch: task.image_fetch,
    target: task.target,
    targetName: task.target_name,
    resourceKind: task.resource_kind,
    executor: task.executor,
    timeoutSeconds: task.timeout_seconds,
    resultSummary: task.result_summary,
    failure: task.failure,
    recovery: task.recovery,
    preparation: task.preparation,
    attemptHistory: task.attempt_history,
    interruptedDetachRetry: task.interrupted_detach_retry,
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
      action: step.action,
      description: step.description,
      target: step.target,
      timeoutSeconds: step.timeout_seconds,
      detail: step.script_slug,
      state: taskStepState(step.status),
      healthWait: step.health_wait ?? undefined,
      healthWaitReceivedAt: step.health_wait_received_at ?? undefined,
      healthchecks: step.healthchecks ?? undefined,
      failure: step.failure ?? undefined,
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
    taskEventKeys.some((key) => !keys.includes(key)) ||
    keys.some(
      (key) =>
        !taskEventKeys.includes(key) &&
        key !== "health_wait" &&
        key !== "controller_operation" &&
        key !== "failure",
    )
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
  const operation =
    event.controller_operation === undefined
      ? undefined
      : parseControllerOperation(event.controller_operation);
  if (
    typeof event.step_id !== "string" ||
    (event.step_id.length === 0) !== Boolean(operation)
  ) {
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

  const health =
    event.health_wait === undefined
      ? undefined
      : parseHealthWait(event.health_wait);
  if (health && (event.state !== "running" || operation))
    throw new Error("health observation requires a running step");
  const failure =
    event.failure === undefined ? undefined : parseFailure(event.failure);
  if (
    failure &&
    (failure.status !== event.state ||
      (failure.step_id ?? "") !== event.step_id ||
      health)
  )
    throw new Error("Task failure does not match its event");
  return {
    attempt: event.attempt,
    ordinal: event.ordinal,
    received_at: event.received_at,
    sequence: event.sequence,
    state: event.state,
    step_id: event.step_id,
    health_wait: health,
    failure,
    controller_operation: operation,
  };
}

function parseControllerOperation(
  value: unknown,
): NonNullable<TaskEventResponse["controller_operation"]> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Controller operation must be an object");
  const operation = value as Record<string, unknown>;
  if (
    Object.keys(operation).some((key) => key !== "stage" && key !== "name") ||
    typeof operation.stage !== "string" ||
    !operation.stage ||
    operation.stage.length > 128 ||
    typeof operation.name !== "string" ||
    !operation.name ||
    operation.name.length > 256
  )
    throw new Error("Controller operation is invalid");
  return { stage: operation.stage, name: operation.name };
}

function parseFailure(
  value: unknown,
): NonNullable<TaskEventResponse["failure"]> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Task failure must be an object");
  const failure = value as Record<string, unknown>;
  if (
    Object.keys(failure).some(
      (key) =>
        !["status", "step_id", "code", "summary", "exit_code"].includes(key),
    ) ||
    (failure.status !== "failed" &&
      failure.status !== "timed_out" &&
      failure.status !== "aborted") ||
    typeof failure.summary !== "string" ||
    failure.summary.length === 0 ||
    failure.summary.length > 1024 ||
    (failure.step_id !== undefined &&
      (typeof failure.step_id !== "string" || failure.step_id.length > 128)) ||
    (failure.code !== undefined &&
      (typeof failure.code !== "string" || failure.code.length > 128)) ||
    (failure.exit_code !== undefined &&
      (typeof failure.exit_code !== "number" ||
        !Number.isInteger(failure.exit_code) ||
        failure.exit_code < -2147483648 ||
        failure.exit_code > 2147483647))
  )
    throw new Error("Task failure is invalid");
  return {
    status: failure.status,
    summary: failure.summary,
    step_id: failure.step_id,
    code: failure.code,
    exit_code: failure.exit_code,
  };
}

function parseHealthWait(
  value: unknown,
): NonNullable<TaskEventResponse["health_wait"]> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("health observation must be an object");
  const observation = value as Record<string, unknown>;
  const counts = [
    "expected",
    "observed",
    "running",
    "ready",
    "starting",
    "unhealthy",
    "exited",
  ];
  const keys = [
    ...counts,
    "elapsed_milliseconds",
    "timeout_milliseconds",
    "diagnostic",
  ];
  if (
    Object.keys(observation).length !== keys.length ||
    Object.keys(observation).some((key) => !keys.includes(key))
  ) {
    throw new Error("health observation has unexpected fields");
  }
  for (const key of [
    ...counts,
    "elapsed_milliseconds",
    "timeout_milliseconds",
  ]) {
    const number = observation[key];
    if (
      typeof number !== "number" ||
      !Number.isSafeInteger(number) ||
      number < 0 ||
      number > (counts.includes(key) ? 65536 : 86400000)
    ) {
      throw new Error("health observation counts or timing are invalid");
    }
  }
  const p = observation as NonNullable<TaskEventResponse["health_wait"]>;
  if (
    p.expected < 1 ||
    p.timeout_milliseconds < 1 ||
    p.ready > p.running ||
    p.running + p.exited > p.observed ||
    p.starting + p.unhealthy > p.observed ||
    ![
      "none",
      "pgrep_missing",
      "curl_missing",
      "nc_missing",
      "command_missing",
      "command_not_executable",
    ].includes(p.diagnostic)
  ) {
    throw new Error("health observation is inconsistent");
  }
  return p;
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
