# ADR 0014: Bound HTTP server resources without breaking SSE

- Status: Accepted
- Date: 2026-08-20

## Context

The Controller serves the human REST API and long-lived server-sent event
(SSE) streams from one `net/http.Server`. The accepted product contract
includes indefinite log, Task-event, and activity streams. It also requires
RFC 7807 problem responses from the application API and defines strict decoded
limits for Blueprint Apply.

The current server does not bound header reads, idle connections, header size,
or request bodies. It also derives every request context directly from the
process context and calls `Server.Close` as soon as that context is canceled.
That immediately interrupts in-flight requests rather than draining them.

A conventional nonzero server-wide `ReadTimeout` covers the entire request,
including its body, and cannot express the different limits needed by JSON,
multipart uploads, and bodyless SSE requests. A conventional nonzero
server-wide `WriteTimeout` is an absolute response-write limit and would
eventually terminate every otherwise healthy SSE stream. Conversely, leaving
all timeouts and limits unset exposes the Controller to slow or unbounded
clients.

This ADR defines one lifecycle and resource policy that combines narrow
server-wide limits with route-specific body and response deadlines. The owner
accepted every lifecycle choice recorded below.

## Accepted requirements

Any eventual decision must preserve these existing contracts:

- log, Task-event, and activity SSE streams may remain open indefinitely;
- high-volume log output is streamed through short-lived Controller buffers
  and is not persisted;
- an SSE client disconnect cancels the corresponding request work;
- Blueprint Apply accepts at most 64 files, 256 KiB per file, and 768 KiB of
  decoded bundle content in total, in addition to its accepted path and YAML
  limits;
- malformed transport or decoding is HTTP 400 and decoded parameter, schema,
  or semantic validation is HTTP 422 where the accepted Huma adapter permits;
- application and framework API failures use RFC 7807 problem documents; and
- Agent update is bodyless; image selection comes only from the Controller's
  digest-pinned `agent.image` configuration.

## Decision

### 1. Server-wide policy

Construct the production `http.Server` with this exact policy:

| Field | Value | Reason |
| --- | ---: | --- |
| `ReadHeaderTimeout` | 5 seconds | Bound slow request-header delivery. |
| `ReadTimeout` | 0 | Body deadlines are route-specific; a global deadline cannot distinguish SSE and uploads. |
| `WriteTimeout` | 0 | A global absolute deadline would terminate long-lived SSE. |
| `IdleTimeout` | 60 seconds | Bound idle keep-alive connections without affecting an active SSE response. |
| `MaxHeaderBytes` | 64 KiB | Bound the request line and header fields before routing. |

These values are Controller invariants, not operator configuration. Keep them
in one unexported HTTP policy with production defaults and injectable shorter
values for tests. Do not add environment variables or flags for them in the
MVP.

For a non-streaming route:

- apply a 30-second request-processing context deadline;
- apply a 30-second body-read deadline when a body is allowed; and
- set a 10-second response-write deadline when the first response write
  begins, then clear it after the response completes.

The response wrapper used to establish the first-write deadline must expose
`Unwrap() http.ResponseWriter` so `http.ResponseController` and protocol
features continue to reach the underlying writer.

Do not use `http.TimeoutHandler` as global middleware or around an SSE route.

### 2. Request bodies

Every route that accepts a body must check a known `Content-Length` before
decoding and wrap the body with `http.MaxBytesReader` before Huma or any other
decoder reads it. The wrapper is authoritative for chunked requests and for a
client that sends more bytes than its declared length.

Blueprint Apply has a 2 MiB raw HTTP-body ceiling. The raw allowance covers
multipart boundaries, part headers, the Blueprint manifest, and other framing
around the accepted 768 KiB decoded-content limit. Decode it with
`Request.MultipartReader`; do not call `ParseMultipartForm` or buffer the
request as one byte slice. Enforce all accepted decoded Blueprint limits while
streaming. The 2 MiB transport ceiling supplements rather than replaces those
semantic limits.

Ordinary JSON bodies have a 1 MiB ceiling. A body exceeding the route ceiling
has an HTTP 413 RFC 7807 response using the request catalog's existing
`request.failed` fallback code.

Bodyless routes, including SSE `GET` routes and Agent update, do not consume a
request body.

### 3. SSE lifecycle

An SSE handler writes the normal stream headers and immediately flushes them.
It then selects over its event source, the request context, the server drain
signal, and a heartbeat ticker.

The heartbeat is this SSE comment every 15 seconds:

```text
: keepalive

```

For the initial flush, every event, and every heartbeat, the handler:

1. obtains an `http.ResponseController` for the response writer;
2. sets the write deadline to five seconds from the current time;
3. writes one complete SSE frame;
4. calls `ResponseController.Flush`; and
5. clears the write deadline after a successful flush.

If setting the deadline, writing, or flushing fails, the handler exits. It also
exits when the client cancels the request, its event source closes, or the
server begins draining. Shutdown does not invent a product event; closing the
HTTP response is the stream termination signal.

Any reverse proxy or ingress in front of the Controller must have an active
response-idle timeout longer than the heartbeat interval. A successful Go
flush does not guarantee that an intermediary has forwarded the bytes.

### 4. Graceful shutdown and request contexts

Do not use the process context directly as `Server.BaseContext`. Derive a
request base that preserves the process context's values but is detached from
its cancellation, then add a separate `cancelAll` function. Maintain an atomic
draining flag and a channel whose closure tells streaming handlers to exit.

On process-context cancellation:

1. atomically mark the server as draining;
2. close the stream-drain channel so SSE handlers return promptly;
3. make any request accepted during the listener-close race return HTTP 503
   with `Connection: close`;
4. call `Server.Shutdown` with a fresh background-based 20-second deadline;
5. if shutdown succeeds, call `cancelAll` to release request-base resources;
6. if the deadline expires, call `cancelAll`, then `Server.Close` to force
   remaining active connections closed; and
7. always wait for the serving goroutine to return and preserve unexpected
   serving, shutdown, or close errors.

The systemd unit has `TimeoutStopSec=30s`, leaving ten seconds
after the Controller's grace deadline for forced closure and process cleanup.
Grace expiry is a structured warning. Expected shutdown after process-context
cancellation is not reported as a serving failure once cleanup succeeds.

This ordering lets ordinary in-flight requests finish during the grace period
without allowing indefinite SSE streams to hold `Shutdown` open forever.

### 5. Pre-handler transport failures

`net/http` parses request headers and enforces `ReadHeaderTimeout` and
`MaxHeaderBytes` before dispatching to Huma or a Groundplane handler. Those
failures therefore cannot pass through the Controller's RFC 7807 formatter.

The contract explicitly exempts failures produced before handler
dispatch from the RFC 7807 requirement. Groundplane does not replace
`net/http` parsing solely to format those failures.

## Rejected alternatives

### Nonzero global `ReadTimeout`

Rejected because it includes the complete body read and cannot express route
size, duration, or streaming differences. Per-route read deadlines and body
ceilings are narrower.

### Nonzero global `WriteTimeout`

Rejected because its absolute deadline would eventually terminate an active
SSE response. Per-response deadlines bound non-stream writes and individual
SSE flushes.

### Immediate `Server.Close`

Rejected because it interrupts ordinary in-flight requests and gives handlers
no drain interval.

### `Server.Shutdown` without an SSE drain signal

Rejected because an active SSE handler need never become idle, so graceful
shutdown would wait until its deadline on every open stream.

### Configurable timeout knobs in the MVP

Rejected because these values are transport safety invariants and API limits,
not workload tuning. An unexported policy provides deterministic production
behavior while allowing fast tests.

## Verification contract

Implementation must include focused tests that:

- assert every exact production default;
- prove an incomplete header is closed at an injected header timeout;
- prove oversized headers are rejected before the handler runs;
- prove an idle keep-alive connection closes at an injected idle timeout;
- prove SSE survives beyond an injected ordinary-response deadline and emits
  heartbeats;
- assert each SSE write sets a deadline, writes, flushes, and clears the
  deadline;
- prove a stalled SSE client causes the handler to exit at its write deadline;
- prove client disconnect cancels the SSE request context;
- exercise known-length and chunked bodies at and one byte above every route
  ceiling;
- exercise Blueprint raw-body, file-count, per-file, and decoded-total
  boundaries;
- prove an ordinary in-flight handler can finish inside the graceful period;
- prove the drain signal closes an open SSE stream and allows `Shutdown` to
  complete;
- prove a handler that outlives the grace period receives request-base
  cancellation and is force-closed;
- prove a request in the listener-close race receives 503 and no new
  connection is accepted afterward; and
- run lifecycle and stream tests under the race detector with goroutine-leak
  checks.

Use real listeners for connection-lifecycle tests. Inject short durations into
the private policy rather than sleeping for production durations, and keep a
separate unit test that pins the production values.

## Consequences

- Long-lived SSE remains supported without leaving slow readers unbounded.
- Header, body, idle-connection, and response-write resource use gains
  explicit bounds.
- Shutdown distinguishes stream draining, ordinary request draining, and
  forced termination.
- Route declarations must identify body class and streaming behavior before
  decoding begins.
- The Blueprint transport ceiling becomes distinct from its accepted decoded
  limits.
- Some malformed connections remain outside the application problem envelope
  under the accepted pre-handler exception.
- Adding a new body class requires an explicit transport ceiling rather than
  inheriting an unrelated route's limit.

## Accepted owner decisions

1. ordinary JSON bodies are capped at 1 MiB and an over-limit body returns an
   RFC 7807 problem with HTTP 413;
2. graceful shutdown waits 20 seconds before forced close and the systemd unit
   uses `TimeoutStopSec=30s`;
3. SSE sends a comment heartbeat every 15 seconds; and
4. failures produced by `net/http` before handler dispatch are exempt from the
   Groundplane RFC 7807 response requirement.

The owner accepted all four decisions above. Agent update has no request-body
transport contract in the MVP.

## Official Go references

- [`http.Server`](https://pkg.go.dev/net/http#Server) defines
  `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`,
  `MaxHeaderBytes`, and `BaseContext`.
- [`Server.Shutdown`](https://pkg.go.dev/net/http#Server.Shutdown) closes
  listeners and idle connections, then waits for active connections to become
  idle until its context expires.
- [`Server.Close`](https://pkg.go.dev/net/http#Server.Close) immediately closes
  active, idle, and new connections.
- [`ResponseController.SetWriteDeadline`](https://pkg.go.dev/net/http#ResponseController.SetWriteDeadline)
  applies a response-specific write deadline.
- [`ResponseController.Flush`](https://pkg.go.dev/net/http#ResponseController.Flush)
  flushes buffered response data through supported response writers.
- [`MaxBytesReader`](https://pkg.go.dev/net/http#MaxBytesReader) bounds an
  incoming request body.
- [`Request.MultipartReader`](https://pkg.go.dev/net/http#Request.MultipartReader)
  provides streaming multipart access.
- [`Request.Context`](https://pkg.go.dev/net/http#Request.Context) documents
  incoming request cancellation on client disconnect and handler completion.
- The official [`net/http` server source](https://go.dev/src/net/http/server.go)
  documents shutdown's treatment of active and hijacked connections.
