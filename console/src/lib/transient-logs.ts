import type { components } from './api.generated'

export type TransientLogEvent = components['schemas']['LogEvent']

export type LogTarget =
  | { kind: 'environment'; id: string }
  | { kind: 'service'; id: string }

export async function watchTransientLogs(
  target: LogTarget,
  options: { tail: number; follow: boolean; signal: AbortSignal },
  onEvent: (event: TransientLogEvent) => void,
): Promise<void> {
  if (!Number.isSafeInteger(options.tail) || options.tail < 0 || options.tail > 1000) {
    throw new Error('Tail must be between 0 and 1000')
  }
  const resource = target.kind === 'environment' ? 'environments' : 'services'
  const query = new URLSearchParams({ tail: String(options.tail), follow: String(options.follow) })
  const response = await fetch(`/api/v1/${resource}/${encodeURIComponent(target.id)}/logs?${query}`, {
    method: 'GET',
    headers: { Accept: 'text/event-stream' },
    signal: options.signal,
  })
  if (!response.ok) {
    const problem = await response.json().catch(() => null)
    const detail = objectString(problem, 'detail')
    throw new Error(detail ?? `Log stream failed with HTTP ${response.status}`)
  }
  if (!response.headers.get('content-type')?.startsWith('text/event-stream')) {
    throw new Error('Controller returned an invalid log stream content type')
  }
  if (!response.body) throw new Error('Controller returned an empty log stream')

  const reader = response.body.getReader()
  const decoder = new TextDecoder('utf-8', { fatal: false })
  let pending = ''
  while (true) {
    const result = await reader.read()
    pending += decoder.decode(result.value, { stream: !result.done })
    let boundary = pending.indexOf('\n\n')
    while (boundary >= 0) {
      const frame = pending.slice(0, boundary).replaceAll('\r\n', '\n')
      pending = pending.slice(boundary + 2)
      const lines = frame.split('\n')
      if (lines.every((line) => line.startsWith(':'))) {
        boundary = pending.indexOf('\n\n')
        continue
      }
      if (lines[0] !== 'event: log' || lines.length !== 2 || !lines[1].startsWith('data: ')) {
        throw new Error('Controller returned a malformed log event')
      }
      const event = parseLogEvent(JSON.parse(lines[1].slice(6)))
      onEvent(event)
      boundary = pending.indexOf('\n\n')
    }
    if (result.done) {
      if (pending.trim() !== '') throw new Error('Controller ended with a partial log event')
      return
    }
  }
}

function parseLogEvent(value: unknown): TransientLogEvent {
  const keys = [
    'container_id', 'container_name', 'line', 'release_id', 'sequence', 'service_id',
    'service_name', 'slot', 'stream', 'timestamp', 'truncated',
  ]
  if (value === null || typeof value !== 'object' || Object.keys(value).sort().join('|') !== keys.join('|')) {
    throw new Error('Controller returned an invalid log event shape')
  }
  const sequenceValue = Reflect.get(value, 'sequence')
  const serviceId = requiredString(value, 'service_id')
  const serviceName = requiredString(value, 'service_name')
  const containerId = requiredString(value, 'container_id')
  const containerName = requiredString(value, 'container_name')
  const releaseId = requiredString(value, 'release_id')
	const slot = Reflect.get(value, 'slot')
	const stream = Reflect.get(value, 'stream')
  const timestamp = requiredString(value, 'timestamp')
  const line = requiredString(value, 'line')
	const truncated = Reflect.get(value, 'truncated')
  if (typeof sequenceValue !== 'number' || !Number.isSafeInteger(sequenceValue) || sequenceValue < 1 ||
    (slot !== 'blue' && slot !== 'green' && slot !== 'singleton') ||
    (stream !== 'stdout' && stream !== 'stderr') || typeof truncated !== 'boolean') {
    throw new Error('Controller returned invalid log event data')
  }
  return {
    sequence: sequenceValue,
    service_id: serviceId,
    service_name: serviceName,
    container_id: containerId,
    container_name: containerName,
    release_id: releaseId,
    slot,
    stream,
    timestamp,
    line,
    truncated,
  }
}

function requiredString(value: object, key: string): string {
  const candidate = Reflect.get(value, key)
  if (typeof candidate !== 'string') throw new Error('Controller returned invalid log event data')
  return candidate
}

function objectString(value: unknown, key: string): string | null {
  if (value === null || typeof value !== 'object') return null
  const candidate = Reflect.get(value, key)
  return typeof candidate === 'string' ? candidate : null
}
