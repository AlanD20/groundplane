# Human API and production Console

Status: Accepted architecture. Generated artifacts, implementation status, and
qualification remain separate from decision acceptance.

The Controller owns one human REST API used by the Console and CLI. The API
contract, surface-parity rule, HTTP resource policy, embedded Console, and
transient log streams are one boundary: none may grow an independent route or
transport contract.

Product authority: [Console and API](../features/console-and-api.md),
[Tasks and logs](../features/tasks-and-logs.md), and [API and CLI](../api-cli.md).

## Operator surface parity

Every operator-facing Controller capability has exactly one Console action,
one CLI command, and one REST endpoint. The stable Console action id and OpenAPI
`operationId` are the same lowercase `noun.verb` identity. Presentation labels
may change without changing that identity.

The local-tooling exceptions are process commands, same-host diagnostics, CLI
version and shell completion. They do not call the human API. The explicit
Secret and Attach-fact reveal policy permits Console/API reveal without a CLI command;
it does not grant general plaintext access. The closed exception list is owned by
[surface parity](../api-cli.md#surface-parity-boundary). Agent enrollment
is not an exception; its Controller action participates in parity. The
Controller-to-Agent channel is a machine protocol outside this manifest.

The parity manifest describes the complete operation, including method,
success status, Task dispatch, and addressing. A generated client proves wire
shape, not parity by itself. Adding another exception or a second operation for
the same capability requires a product-contract change.

## Generated human API contract

Huma v2 typed handlers are the code-first REST boundary. The running API and
generation use the same `huma.API.OpenAPI()` object; there is no shadow registry
for handwritten routes. Framework types stop at the transport boundary and
RFC 7807 remains the public error envelope.

Huma's schema registry requires a Go `reflect.Type`. Route registration may
pass `reflect.TypeFor[T]()` directly to that registry; this is type metadata,
not runtime model conversion. The architecture gate permits only that exact
registration shape in HTTP handlers and still rejects other production
reflection.

The deterministic OpenAPI 3.1 document is committed at
[openapi.json](../../openapi.json). It generates the Go client at
[client.gen.go](../../internal/cli/apiclient/generated/client.gen.go) and the
runtime-free Console contract at
[api.generated.ts](../../console/src/lib/api.generated.ts). The CLI transport
layer retains idempotency, timeout, streaming, and problem-response policy;
the Console retains its runtime request policy. Generated files are never
edited.

The CLI's Cobra execution context carries its scoped App, and an HTTP request
context carries the streaming lifecycle that admitted it. Those two private
keys are installed and recovered by their owning boundary; the architecture
gate permits only those exact context assertions. They do not authorize
general dynamic model recovery.

Generator versions are exact repository dependencies. Generation emits the
document before both clients, and drift is a delivery failure. Only live typed
operations enter the document: a migration removes the superseded route and
moves both callers in the same slice. A partial document or generated type file
does not prove full operator parity.

Current source: [OpenAPI registration](../../internal/controller/handlers/openapi.go),
[generator](../../internal/openapigen), and
[CLI API policy](../../internal/cli/apiclient).

## HTTP and SSE lifecycle

One `net/http.Server` serves ordinary API requests and indefinite SSE streams.
Global body or write deadlines cannot express both safely, so the server has a
short header deadline, bounded header size and idle lifetime, while body reads,
ordinary request processing, response writes, and SSE flushes use route-specific
deadlines.

The accepted production policy is a 5-second header deadline, 60-second idle
timeout, and 64 KiB header ceiling. Ordinary non-streaming work has 30-second
request/body deadlines and a 10-second first-write deadline. JSON bodies are
bounded to 1 MiB; Blueprint multipart transport is bounded to 2 MiB in addition
to the decoded bundle limits. Known length is checked before decoding and the
stream is still wrapped so chunked input cannot bypass the limit.

SSE uses no global write timeout. Each frame or heartbeat receives a fresh
five-second write deadline and flush, and a keepalive comment is sent every 15
seconds. A client disconnect cancels request work. Proxies must permit an
active response-idle interval longer than the heartbeat.

Shutdown first marks the server draining, asks streams to exit, and rejects the
listener-close race. It then gives ordinary work 20 seconds to drain before
canceling and force-closing remaining connections; the systemd service allows
30 seconds. Pre-handler `net/http` parsing failures are the deliberate exception
to RFC 7807 because application code never receives them.

These values are safety invariants, not MVP configuration knobs. Current source:
[HTTP lifecycle](../../internal/controller/handlers/http_lifecycle.go) and
[listeners](../../internal/controller/handlers/listeners.go).

## Console packaging

The production Controller embeds the Vite output in one self-contained binary.
`console/dist/` and `node_modules/` stay ignored and are never release payloads.
The tagged Console package lives beside the Vite output so `go:embed` consumes
the exact build without a copied tree or generated Go blob. Untagged builds are
development-only and return a structured unavailable response for valid Console
requests.

The release build verifies the pinned Node and npm toolchain, uses `npm ci`,
builds Vite, verifies fingerprinted assets, and then builds the Controller with
the Console tag. The production binary has no Node or adjacent asset dependency.

An outer dispatcher validates the original decoded and escaped path before
`ServeMux` can clean or redirect it. API and operational namespaces always win
and never fall through to the SPA. Static serving is `fs.FS` only: no host
directory fallback, directory listing, hidden asset path, MIME sniffing, or
runtime filesystem dependency.

Only canonical `GET` and `HEAD` requests reach static content. An exact file
wins; HTML-capable navigation may fall back to `index.html`, but asset space
never does. Fingerprinted assets are immutable-cache content; HTML and other
non-fingerprinted files revalidate; problems are not cached. The handler owns a
closed MIME map and `nosniff` behavior so the host MIME database cannot change
responses.

Current source: [embedded assets](../../console/embed.go),
[Console handler](../../internal/controller/handlers/console.go), and
[Vite configuration](../../console/vite.config.ts).

This decision accepted the packaging architecture but did not by itself prove
the complete Console capability or generated-client adoption.

## Transient log streams

Service and Environment Logs are one bounded SSE capability across Console,
CLI, and API. They stream workload-authored Docker stdout/stderr; they are not
Task events and never enter etcd, files, ordinary daemon logs, or a resumable
journal. `Last-Event-ID` is rejected and reconnecting starts a fresh bounded
tail.

Before headers, the Controller resolves the current serving Release set at one
fixed revision. Service Logs use that Service's serving Release; Environment
Logs use the serving Release of each Service in stable order. The set is frozen
for the connection, excludes stable proxies and inactive release slots, and
does not traverse Attaches into backing Environments.

The Controller sends a typed subscription over the existing authenticated
Agent channel. The Agent alone reads Docker, verifies managed ownership labels,
and returns bounded normalized frames; the Controller alone selects serving
authority and encodes SSE. A private ready signal separates setup failures that
can still return an RFC 7807 response from failures after headers, which close
the stream.

Buffers, targets, sources, and concurrency are bounded. Normal replay bursts use
bounded backpressure rather than being mistaken for overflow. An exceeded hard
limit fails explicitly rather than dropping silently or growing memory. Lines are UTF-8
normalized and truncated at the contract limit. Groundplane does not inject
Secret-store material, but it also does not redact secrets printed by a
workload; workloads remain responsible for their own output.

Current source: [log routes](../../internal/controller/handlers/log_routes.go),
[Controller log service](../../internal/controller/servicelogs),
[channel broker](../../internal/controller/agentchannel/logs.go),
[Agent subscriptions](../../internal/agent/logstream), and
[Console viewer](../../console/src/features/logs).

Verification for these boundaries is behavioral, generation-based, or
architecture-based. Do not replace it with static source-text assertions.
