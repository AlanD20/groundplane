import {
  ControllerTransportError,
  controllerResponseError,
} from "./controller-request-errors";
import { newULID } from "./utils";

export async function controllerRequest<Response>(
  path: string,
  expectedStatus: number,
  init: {
    method?: "GET" | "POST" | "PATCH" | "PUT" | "DELETE";
    body?: unknown;
    signal?: AbortSignal;
    idempotencyKey?: string;
  } = {},
): Promise<Response> {
  const method = init.method ?? "GET";
  const headers = new Headers({ Accept: "application/json" });
  if (init.body !== undefined) headers.set("Content-Type", "application/json");
  if (method !== "GET")
    headers.set("Idempotency-Key", init.idempotencyKey ?? newULID());
  const requestBody =
    init.body === undefined ? undefined : JSON.stringify(init.body);
  let response: globalThis.Response;
  try {
    response = await fetch(`/api/v1${path}`, {
      method,
      headers,
      body: requestBody,
      signal: init.signal,
    });
  } catch (error) {
    const detail =
      error instanceof Error
        ? error.message
        : "request failed before an HTTP response";
    throw new ControllerTransportError(`${method} ${path}: ${detail}`, error);
  }
  if (response.status !== expectedStatus) {
    throw await controllerResponseError(response, method, path);
  }
  return (await response.json()) as Response;
}

export function waitForRequest<T>(
  request: Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  if (!signal) return request;
  if (signal.aborted)
    return Promise.reject(
      signal.reason ?? new DOMException("Aborted", "AbortError"),
    );
  return new Promise<T>((resolve, reject) => {
    const abort = () =>
      reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
    signal.addEventListener("abort", abort, { once: true });
    void request
      .then(resolve, reject)
      .finally(() => signal.removeEventListener("abort", abort));
  });
}
