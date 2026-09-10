import type { operations } from '@/lib/api.generated'

export type ControllerRequest = <Response>(path: string, status: number, init?: {
  method?: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE'
  body?: unknown
  signal?: AbortSignal
  idempotencyKey?: string
}) => Promise<Response>

export type ControllerConfigResponse = operations['controller.config.show']['responses'][200]['content']['application/json']
export type ControllerConfigRequest = operations['controller.config.set']['requestBody']['content']['application/json']
export type ControllerUpdateAccepted = operations['controller.update']['responses'][202]['content']['application/json']
export type ControllerTask = operations['task.show']['responses'][200]['content']['application/json']
