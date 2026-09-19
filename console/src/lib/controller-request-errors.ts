export class ControllerRequestError extends Error {
  readonly responseReceived = true

  constructor(
    message: string,
    readonly status: number,
    readonly code?: string,
  ) {
    super(message)
    this.name = 'ControllerRequestError'
  }
}

export class ControllerTransportError extends Error {
  readonly responseReceived = false

  constructor(message: string, readonly cause: unknown) {
    super(message)
    this.name = 'ControllerTransportError'
  }
}

export async function controllerResponseError(response: Response, method: string, path: string) {
  let detail = `${method} ${path} returned ${response.status}`
  let code: string | undefined
  try {
    const problem = (await response.json()) as { code?: string; detail?: string }
    if (problem.detail) detail = problem.detail
    code = problem.code
  } catch {
    // The status remains actionable even when a proxy returned a non-JSON body.
  }
  return new ControllerRequestError(detail, response.status, code)
}
