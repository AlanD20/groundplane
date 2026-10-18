import type { TaskEventResponse } from "./journal-model";

function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value))
    throw new Error("Expected event object");
  return value as Record<string, unknown>;
}

function sequence(value: unknown): boolean {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function timestamp(value: unknown): boolean {
  return (
    typeof value === "string" &&
    value.endsWith("Z") &&
    Number.isFinite(Date.parse(value))
  );
}

function text(value: unknown, maximum: number, required = false): boolean {
  return value === undefined
    ? !required
    : typeof value === "string" &&
        value.length <= maximum &&
        (!required || value.length > 0);
}

export function parseTaskEvent(data: string): TaskEventResponse {
  const event = object(JSON.parse(data));
  const expected = [
    "sequence",
    "work_item_id",
    "attempt_id",
    "attempt_sequence",
    "recorded_at",
    "stage",
  ];
  if (
    Object.keys(event).length !== expected.length ||
    expected.some((key) => !(key in event)) ||
    !sequence(event.sequence) ||
    !sequence(event.attempt_sequence) ||
    !timestamp(event.recorded_at) ||
    typeof event.work_item_id !== "string" ||
    !/^work_[0-9A-HJKMNP-TV-Z]{26}$/.test(event.work_item_id) ||
    typeof event.attempt_id !== "string" ||
    !/^try_[0-9A-HJKMNP-TV-Z]{26}$/.test(event.attempt_id)
  )
    throw new Error("Invalid execution event identity");
  const stage = object(event.stage);
  const keys = [
    "key",
    "name",
    "state",
    "expected",
    "observed",
    "failure_code",
    "started_at",
    "finished_at",
  ];
  if (
    Object.keys(stage).some((key) => !keys.includes(key)) ||
    typeof stage.key !== "string" ||
    !/^[a-z0-9_-]{1,64}$/.test(stage.key) ||
    !text(stage.name, 160, true) ||
    !text(stage.expected, 1024, true) ||
    !text(stage.observed, 2048) ||
    !text(stage.failure_code, 128) ||
    typeof stage.state !== "string" ||
    ![
      "waiting",
      "running",
      "succeeded",
      "failed",
      "skipped",
      "unknown",
    ].includes(stage.state) ||
    (stage.started_at !== undefined && !timestamp(stage.started_at)) ||
    (stage.finished_at !== undefined && !timestamp(stage.finished_at))
  )
    throw new Error("Invalid execution stage");
  return event as TaskEventResponse;
}
