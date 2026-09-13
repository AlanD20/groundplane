import assert from 'node:assert/strict'
import test from 'node:test'

import { watchTransientLogs } from './transient-logs.ts'

// QA: LOG-01, LOG-02; local stream decoding, not source selection or reconnect.
// Rationale: keepalive comments must neither terminate the stream nor hide the
// next real log event from the operator.
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

// QA: LOG-01; local protocol framing, not a live container log stream.
// Rationale: valid CRLF framing must deliver the same log line as LF framing.
test('watchTransientLogs accepts CRLF-delimited log events', async () => {
  const originalFetch = globalThis.fetch
  const event = `event: log\r\ndata: ${JSON.stringify({
    container_id: 'groundplane-api-blue-01',
    container_name: 'groundplane-api-blue-01',
    line: 'crlf-ready',
    release_id: 'dep_01ARZ3NDEKTSV4RRFFQ69G5FAV',
    sequence: 1,
    service_id: 'svc_01ARZ3NDEKTSV4RRFFQ69G5FAV',
    service_name: 'api',
    slot: 'blue',
    stream: 'stdout',
    timestamp: '2026-08-30T12:00:00Z',
    truncated: false,
  })}\r\n\r\n`
  globalThis.fetch = async () => new Response(new ReadableStream({
    start(controller) {
      controller.enqueue(new TextEncoder().encode(event))
      controller.close()
    },
  }), {
    status: 200,
    headers: { 'content-type': 'text/event-stream' },
  })

  try {
    const received = []
    await watchTransientLogs(
      { kind: 'service', id: 'svc_01ARZ3NDEKTSV4RRFFQ69G5FAV' },
      { tail: 1, follow: false, signal: new AbortController().signal },
      (logEvent) => received.push(logEvent.line),
    )
    assert.deepEqual(received, ['crlf-ready'])
  } finally {
    globalThis.fetch = originalFetch
  }
})
