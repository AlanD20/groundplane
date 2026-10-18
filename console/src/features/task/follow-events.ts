import {
  ControllerRequestError,
  ControllerTransportError,
  controllerResponseError,
} from "@/lib/controller-request-errors";
import { requestTask } from "./api";
import { parseTaskEvent } from "./event-model";
import type { TaskEventResponse } from "./journal-model";

// Fetch exposes HTTP failures and permits an explicit resume header. Diagnostic
// gaps never imply execution failure, and reconnect never submits new work.
export async function followTaskEvents(
  taskId: string,
  signal: AbortSignal,
  onEvent: (event: TaskEventResponse) => void,
  onNotice: (message: string) => void,
) {
  let sequence = 0,
    delay = 1000;
  const path = `/api/v1/tasks/${encodeURIComponent(taskId)}/events`;
  while (!signal.aborted) {
    try {
      const headers = new Headers({ Accept: "text/event-stream" });
      if (sequence) headers.set("Last-Event-ID", String(sequence));
      let response: Response;
      try {
        response = await fetch(path, { headers, signal });
      } catch (cause) {
        throw new ControllerTransportError(
          "Task stream is disconnected",
          cause,
        );
      }
      if (response.status !== 200)
        throw await controllerResponseError(response, "GET", path);
      if (
        response.headers.get("content-type")?.split(";")[0].trim() !==
          "text/event-stream" ||
        !response.body
      ) {
        await response.body?.cancel();
        throw new Error("Task stream returned an invalid response format");
      }
      await consumeEvents(response.body, signal, (event) => {
        if (event.sequence <= sequence) return;
        if (event.sequence > sequence + 1)
          onNotice(
            "Earlier Task events are no longer retained. Recorded stages and the Task outcome remain available.",
          );
        onEvent(event);
        sequence = event.sequence;
        delay = 1000;
      });
      if ((await requestTask(taskId, signal)).task.finished_at) return;
    } catch (error) {
      if (signal.aborted) return;
      if (
        error instanceof ControllerRequestError &&
        error.code === "cursor.expired"
      ) {
        onNotice(
          "The event cursor expired. Refreshing Task details and resuming retained events; earlier events are missing.",
        );
        sequence = 0;
      } else if (
        error instanceof ControllerTransportError ||
        (error instanceof ControllerRequestError &&
          (error.status >= 500 || error.status === 408 || error.status === 429))
      ) {
        onNotice(
          "Task events are disconnected; reconnecting. This does not change the Task outcome.",
        );
      } else {
        onNotice(
          error instanceof Error
            ? error.message
            : "Unable to follow Task events",
        );
        return;
      }
      try {
        if ((await requestTask(taskId, signal)).task.finished_at) return;
      } catch (readError) {
        if (signal.aborted) return;
        if (
          readError instanceof ControllerRequestError &&
          readError.status === 404
        ) {
          onNotice(
            "Task history is no longer available. Refresh the resource for its current state.",
          );
          return;
        }
      }
    }
    await waitForReconnect(delay, signal);
    delay = Math.min(delay * 2, 30_000);
  }
}

async function consumeEvents(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal,
  emit: (event: TaskEventResponse) => void,
) {
  const reader = body.getReader();
  const decoder = new TextDecoder("utf-8", { fatal: true });
  let pending = "",
    id = "",
    data: string[] = [],
    size = 0;
  const line = (text: string) => {
    if (text.endsWith("\r")) text = text.slice(0, -1);
    if (!text) {
      if (!id && !data.length) return;
      if (!id || !data.length)
        throw new Error("Task event frame is incomplete");
      const event = parseTaskEvent(data.join("\n"));
      if (id !== String(event.sequence))
        throw new Error("Task event identity is inconsistent");
      emit(event);
      id = "";
      data = [];
      size = 0;
      return;
    }
    if (text.startsWith(":")) return;
    size += text.length;
    if (size > 65536) throw new Error("Task event exceeds its size limit");
    const separator = text.indexOf(":");
    const key = separator < 0 ? text : text.slice(0, separator);
    const value =
      separator < 0 ? "" : text.slice(separator + 1).replace(/^ /, "");
    if (key === "id") {
      if (id) throw new Error("Task event repeats its identity");
      id = value;
    } else if (key === "data") data.push(value);
    else if (key === "event" && value !== "message" && value !== "")
      throw new Error("Unknown Task event type");
  };
  try {
    while (!signal.aborted) {
      let part: ReadableStreamReadResult<Uint8Array>;
      try {
        part = await reader.read();
      } catch (cause) {
        throw new ControllerTransportError(
          "Task event read was interrupted",
          cause,
        );
      }
      pending += decoder.decode(part.value, { stream: !part.done });
      let end: number;
      while ((end = pending.indexOf("\n")) >= 0) {
        line(pending.slice(0, end));
        pending = pending.slice(end + 1);
      }
      if (pending.length > 65536)
        throw new Error("Task event line exceeds its size limit");
      if (part.done) {
        if (pending || id || data.length)
          throw new ControllerTransportError(
            "Task stream ended during an event",
            undefined,
          );
        return;
      }
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

function waitForReconnect(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", done);
      resolve();
    };
    const timer = setTimeout(done, ms);
    signal.addEventListener("abort", done, { once: true });
  });
}
