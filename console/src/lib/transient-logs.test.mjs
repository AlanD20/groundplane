import assert from 'node:assert/strict'
import test from 'node:test'

import { watchTransientLogs } from './transient-logs.ts'

test('watchTransientLogs ignores comment-only SSE frames between log events', async () => {
  const originalFetch = globalThis.fetch
  const event = (sequence, line) => `event: log\ndata: ${JSON.stringify({
    container_id: 'groundplane-api-blue-01',
    container_name: 'groundplane-api-blue-01',
    line,
    release_id: 'dep_01ARZ3NDEKTSV4RRFFQ69G5FAV',
    sequence,
    service_id: 'svc_01ARZ3NDEKTSV4RRFFQ69G5FAV',
    service_name: 'api',
    slot: 'blue',
    stream: 'stdout',
    timestamp: '2026-08-30T12:00:00Z',
    truncated: false,
  })}\n\n`
  const payload = `${event(1, 'first')}: keepalive\n\n${event(2, 'second')}`
  globalThis.fetch = async () => new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(payload))
      controller.close()
    },
  }), {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  })

  try {
    const received = []
    await watchTransientLogs(
      { kind: 'environment', id: 'env_01ARZ3NDEKTSV4RRFFQ69G5FAV' },
      { tail: 100, follow: false, signal: new AbortController().signal },
      (logEvent) => received.push(logEvent.line),
    )
    assert.deepEqual(received, ['first', 'second'])
  } finally {
    globalThis.fetch = originalFetch
  }
})
