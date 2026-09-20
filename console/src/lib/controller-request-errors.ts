export function isNoResponseTransportUncertainty(error: unknown): boolean {
  return error instanceof ControllerTransportError && !error.responseReceived;
}

export function controllerUpdateRejected(error: unknown): boolean {
  return (
    error instanceof ControllerRequestError &&
    [400, 404, 405, 409, 422].includes(error.status)
  );
}

export function isTaskNotFoundError(error: unknown): boolean {
  return (
    error instanceof ControllerRequestError &&
    (error.status === 404 || error.code === "not_found")
  );
}
export class ControllerRequestError extends Error {
  readonly responseReceived = true;

  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message);
    this.name = "ControllerRequestError";
  }
}

export class ControllerTransportError extends Error {
  readonly responseReceived = false;

  constructor(
    message: string,
    readonly cause: unknown,
  ) {
    super(message);
    this.name = "ControllerTransportError";
  }
}

export async function controllerResponseError(
  response: Response,
  method: string,
  path: string,
) {
  let detail = `${method} ${path} returned ${response.status}`;
  let code: string | undefined;
  try {
    const problem = (await response.json()) as {
      code?: string;
      detail?: string;
    };
    if (problem.detail) detail = problem.detail;
    code = problem.code;
  } catch {
    // The status remains actionable even when a proxy returned a non-JSON body.
  }
  return new ControllerRequestError(detail, response.status, code);
}
