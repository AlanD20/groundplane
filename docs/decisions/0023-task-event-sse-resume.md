# ADR 0023: Stream durable Task events with sequence-based SSE resume

- Status: Accepted
- Date: 2026-08-20
- Accepted: 2026-08-23

## Context

The human API already declares `GET /tasks/{id}/events` as a Server-Sent
Events stream. Accepted ADR 0013 fixes the durable Task-event keys, contiguous
Controller-owned sequence, Agent-event deduplication identity, fixed-revision
reads, and the general snapshot-then-watch handoff. The existing Task
repository can append events atomically and return a complete event journal at
one etcd revision. The Store can watch a prefix from an explicit revision.

The public stream contract remains incomplete. It does not define the SSE
event id, `Last-Event-ID` grammar, initial replay, a resume point beyond the
current journal, duplicate filtering, terminal closure, compaction recovery,
or what happens when a failure occurs after HTTP headers have committed.
Choosing those behaviors in a handler or repository would create observable
retry semantics by accident.

This ADR fixes that missing contract. The owner approved every checklist item
and the operator-safe public event representation on 2026-08-23.

## Existing constraints and conflicts

The decision must preserve these accepted rules:

- Task events are append-only under
  `/v1/runtime/task-events/<task-id>/<20-digit-sequence>`;
- the Controller assigns a contiguous `uint64` sequence starting at 1;
- each Agent event has stable `(task id, step id, attempt, ordinal)` identity,
  and identical Agent redelivery returns the already allocated sequence;
- each assignment delivery carries one positive Controller-authored event
  attempt execution epoch used by every step event from that delivery;
- one Task has at most 1,000 durable events, each with at most 32 KiB of durable
  JSON; subprocess and log streams are excluded;
- a complete snapshot is read at fixed revision `R`, and a gap-free watch
  starts at `R + 1`;
- a terminal step event does not make the Task terminal; only the separate
  Task status is authoritative;
- Task operation-index and active-operation-index value schemas remain
  unresolved and must not gate an event stream over already accepted Task and
  event keys;
- caller cancellation is control flow, live-context storage failure is
  `storage.unavailable`, corrupt durable state is internal, and a missing Task
  is `task.not_found`; and
- raw etcd keys and revisions never cross the infrastructure boundary or
  appear in a public response, SSE field, log, metric, or trace.

SSE reconnect is not the same as ADR 0013 pagination. A pagination cursor pins
one historical etcd view and may expire after compaction. A live Task stream
resumes by immutable Task-event sequence and can safely rebuild from a new
current snapshot without exposing or retaining an etcd revision.

## Decision

### 1. Use Task-event sequence as the SSE id

Every emitted durable Task event carries:

```text
id: <canonical decimal Task-event sequence>
```

The id is the event record's Controller-owned sequence in ordinary base-10
ASCII. It is never an etcd revision.

The SSE `data` field is one compact JSON object with exactly:

```json
{
  "sequence": 1,
  "step_id": "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
  "state": "running",
  "attempt": 1,
  "ordinal": 1,
  "received_at": "2026-08-23T04:30:00Z"
}
```

`state` uses `pending | running | completed | failed | aborted | timed_out`.
`received_at` is RFC 3339. The route already scopes the Task, so `task_id` is
not duplicated. The durable payload is currently the closed empty progress
object; Agent output chunks are rejected from durable storage and belong to
resource log streams. Neither that payload, its deduplication hash, a raw
durable record, an etcd key, nor an etcd revision enters the human response.
A future typed operator diagnostic requires an explicit API-contract change;
it may not pass arbitrary Agent data through this shape.

The request accepts either no `Last-Event-ID` field or exactly one field. Its
post-HTTP-parsing field value must match:

```text
0|[1-9][0-9]{0,19}
```

It must also parse as a `uint64`. Leading zeroes, a sign, whitespace, an empty
value, non-ASCII digits, overflow, or multiple field instances are malformed.
An absent field and the canonical value `0` both mean replay from the start.

The server validates the header against a fixed-revision Task snapshot before
committing SSE headers. If `N` is greater than that snapshot's latest sequence,
the request returns `validation.failed` with HTTP 400. For a Task with no
events, only absent or `0` is valid. This is a malformed resume request rather
than a wait for a sequence that may never belong to this Task.

### 2. Replay one fixed snapshot, then watch from its successor revision

For a validated Task id and resume sequence `N`, the repository and handler
perform this handoff:

1. read the Task and its complete event journal at one fixed revision `R`;
2. validate the Task summary, contiguous event keys and records, and `N` against
   that snapshot;
3. before writing SSE headers, return any validation, not-found, storage, or
   corruption error through the ordinary RFC 7807 path;
4. commit the SSE response and emit snapshot events with sequence greater than
   `N`, strictly in ascending sequence order;
5. start watches for both the exact Task primary and the Task-event prefix at
   `R + 1`; and
6. emit each subsequently committed event once in ascending sequence order.

Starting both watches is part of the handoff, not an invitation to infer an
aggregate from flattened etcd events. Event values are decoded and validated.
Task-primary changes are decoded and used only to observe authoritative Task
status. The stream never watches Task secondary indexes and therefore remains
independent of their unresolved value schemas.

The implementation tracks the last emitted Task-event sequence. A watched
event with a sequence less than or equal to that value is a replay and is
ignored. The next emitted event must be exactly `last + 1`. A gap, unexpected
event delete, mismatched key or Task id, noncanonical record, or impossible
Task summary is durable corruption and follows the post-header failure rule
below.

The server guarantees exactly-once emission of a sequence within one live HTTP
connection. Delivery across connections is at-least-once: a connection can
break after bytes reach the client but before the user agent durably advances
its `Last-Event-ID`. Clients deduplicate across reconnects by the SSE id, and
the server's `<= last` filter makes replay harmless.

### 3. Close only from authoritative terminal Task state

The terminal Task statuses are `completed`, `failed`, `aborted`, and
`timed_out`. A terminal step event never closes the stream.

If the initial fixed-revision snapshot already contains a terminal Task, the
server emits every requested snapshot event and closes the SSE response. It
does not start live watches.

For a nonterminal snapshot, the server follows both watches. When the Task
primary watch first exposes a terminal status at revision `T`, the repository
performs a final fixed-revision Task-event snapshot at `T`. That view includes
every event committed before or atomically with the terminal transition. The
server emits each still-unseen sequence in order, then cancels both watches and
closes the connection normally.

If repository composition cannot read exactly at `T`, it may take a newer
linearizable snapshot. Append rejects every new event identity after terminal
Task state, so a validated newer snapshot has the same final journal. The
newer etcd revision remains private.

An event-watch notification may arrive before or after the Task-primary
notification from the same or an earlier transaction. The final drain and
sequence filter make that arrival order unobservable: every final event is
emitted once before closure.

### 4. Recover compaction by resnapshotting

Watch compaction is not a public cursor expiry. The repository cancels the
affected watches, takes a new current fixed-revision Task-and-event snapshot,
validates it, emits only sequences greater than the last emitted sequence, and
restarts both watches at the new `R + 1`.

If the resnapshot is terminal, it performs the same final drain and closes. If
the journal has regressed, contains a gap, or no longer matches the Task
summary, the condition is corruption rather than a reason to reset the client
resume position.

No compaction revision, watch revision, resnapshot count, or etcd key is sent
to the client.

### 5. Disconnect on live failure and let the client resume

Before SSE headers commit, ordinary error mapping applies:

| Condition | Result |
| --- | --- |
| malformed `Last-Event-ID`, including a value ahead of the snapshot | `validation.failed`, HTTP 400 |
| missing or already retention-pruned Task | `task.not_found`, HTTP 404 |
| live-context storage failure | `storage.unavailable`, HTTP 503 |
| canceled caller context | preserve cancellation; no problem response is required after disconnect |
| corrupt Task, journal, key, sequence, or record | generic internal, HTTP 500 |

After headers commit, HTTP status and RFC 7807 are no longer available. A live
transport or storage failure, corruption, unexpected deletion, or Task removal
is logged through the safe internal error boundary and the connection is
closed. The server does not invent an `event: error` record, does not put error
text in `data`, and does not advance the SSE id. The client reconnects with the
last successfully observed `Last-Event-ID`.

Caller cancellation closes the stream without an error event and without
logging it as a storage or corruption failure. Watch goroutines are owned by
that context and must terminate before the stream method returns.

Compaction is the one transparently recovered watch failure because a current
append-only snapshot plus the last sequence proves a safe continuation. Other
live storage failures disconnect rather than retrying indefinitely inside one
request.

### 6. Treat retention deletion according to response phase

A Task missing from the initial snapshot returns `task.not_found` before SSE
headers. A Task that disappears during a live stream causes a safe logged
disconnect with no synthetic event. The normal path closes a terminal stream
long before its 90-day retention deletion, so an in-stream deletion indicates
concurrent retention or invariant failure but does not disclose which internal
key disappeared.

Reconnecting after retention deletion returns `task.not_found`; there is no
tombstone stream and no attempt to recreate historical events.

### 7. Allocate an event attempt for each assignment execution epoch

`TaskAssignment.event_attempt` is a required positive `uint32` authored only
by the Controller. The Agent validates it at the protobuf boundary, copies it
into its owned assignment, and uses it for every step event emitted by that
delivery. It never substitutes a local constant or counter.

The first delivery of a newly claimed assignment uses attempt 1. On recovery,
the Controller reads the complete Task event journal at the recovered claim's
fixed Task read revision. Every durable event must match the exact Task,
assignment, Agent, and Agent generation. No events selects attempt 1; otherwise
the next attempt is one greater than the maximum durable attempt. An identity
mismatch, zero durable attempt, or `uint32` overflow is internal corruption.

This rule deliberately does not persist delivery itself. If a delivery fails
before any event from its attempt is durable, recovery reuses that attempt. As
soon as one event is durable, the next recovery advances the epoch, preventing
the same `(task, step, attempt, ordinal)` identity from describing a second
execution. A recovered assignment at or after its immutable deadline is not
dispatched; the existing timeout scheduler remains the sole terminalizer.

There is no zero-value interpretation, Agent-authored fallback, legacy
hardcoded attempt, or compatibility path.

## Exact stream state machine

```text
validate_request
  invalid ------------------------------> fail_before_headers(400)
  valid --------------------------------> snapshot

snapshot
  missing ------------------------------> fail_before_headers(404)
  storage unavailable ------------------> fail_before_headers(503)
  corrupt ------------------------------> fail_before_headers(500)
  resume > latest ----------------------> fail_before_headers(400)
  valid --------------------------------> replay

replay
  emit each sequence > last in order
  terminal snapshot --------------------> close_normal
  nonterminal snapshot -----------------> follow(R + 1)

follow
  event sequence == last + 1 -----------> emit; follow
  event sequence <= last ---------------> ignore; follow
  terminal Task primary ----------------> final_drain(terminal revision)
  watch compacted ----------------------> resnapshot
  caller canceled ----------------------> close_normal
  storage/transport/corruption/delete --> log_safe; disconnect

resnapshot
  valid nonterminal --------------------> emit unseen; follow(new R + 1)
  valid terminal -----------------------> emit unseen; close_normal
  missing/corrupt/storage failure ------> log_safe; disconnect

final_drain
  valid --------------------------------> emit unseen in order; close_normal
  missing/corrupt/storage failure ------> log_safe; disconnect
```

No transition decreases `last`, emits a duplicate sequence on the same
connection, or substitutes a new etcd revision as public resume state.

## Security and error boundary

- `Last-Event-ID` is bounded to 20 ASCII digits before numeric parsing. It is
  not copied into logs, metrics, or traces.
- Task ids and event sequences are public resource metadata, but raw event
  payloads are never logged while classifying stream failure.
- Safe diagnostics may identify the Task id, stream phase, and closed error
  kind. They never include an etcd key, revision, event payload, Agent-provided
  error text, secret value, Blueprint content, or serialized durable record.
- Every watched value is independently decoded and validated before use. SSE
  framing is generated from typed public event data, never by concatenating a
  durable JSON record or error string into protocol lines.
- A client-provided id chooses only a suffix of one validated Task journal. It
  cannot select an etcd revision, key prefix, another Task, or a secondary
  index.
- Authentication and Task visibility remain the HTTP boundary's responsibility;
  this ADR creates no bypass or new authorization scope.

## Verification

Implementation requires deterministic rationale tests covering:

1. absent and `0` headers replay the complete journal;
2. canonical positive decimal values replay only greater sequences;
3. empty, signed, whitespace-padded, non-ASCII, leading-zero, overflowing,
   multiple, and ahead-of-snapshot values fail with HTTP 400 before headers;
4. an append committed between snapshot and watch is delivered from `R + 1`;
5. an event present at `R` is not duplicated by the watch;
6. replayed watch events are filtered and delivered events remain strictly
   sequence-ascending;
7. a sequence gap, delete, mismatched key/record, or corrupt value disconnects
   after safe logging without emitting an SSE error record;
8. an initially terminal Task replays the requested suffix and closes without
   starting watches;
9. a live terminal Task transition performs a fixed-revision final drain,
   including both Task-first and event-first watch delivery orders, then closes;
10. compaction resnapshot emits only unseen events, restarts at the new
    revision plus one, and closes if the resnapshot is terminal;
11. transport or storage failure before headers maps normally, while the same
    failure after headers disconnects without an invented event;
12. retention deletion is 404 before headers and disconnect-only afterward;
13. caller cancellation closes both watches and every forwarding goroutine;
14. blocked and slow consumers do not lose events, reorder sequences, leak a
    goroutine, or outlive their context; and
15. focused repository and handler suites pass under `-race`.
16. first delivery uses attempt 1, while two recovered sessions advance only
    after an event is durable and reuse the next attempt while it remains
    eventless;
17. recovered event identity mismatch, zero, and `uint32` exhaustion fail as
    internal corruption at the claim's exact fixed read revision; and
18. recovery at and after the immutable deadline sends no assignment and
    leaves terminalization to the timeout scheduler.

No test may assert or expose a raw etcd revision through SSE output or a public
problem.

## Alternatives rejected

### Use etcd revision as SSE id

Rejected. It exposes an infrastructure detail, couples clients to compaction,
and gives one Task stream ids affected by unrelated writes.

### Start a watch without a snapshot

Rejected. A write between an initial read and watch registration can be lost,
and a new client would not receive the durable journal it asked to follow.

### Encode an opaque cursor in `Last-Event-ID`

Rejected. Task-event sequence is already stable, bounded, ordered, and scoped
by the route's Task id. An opaque cursor would add versioning and could
accidentally carry a private etcd revision.

### Treat a terminal step event as stream completion

Rejected. Step state and Task state are deliberately separate. A final Agent
acknowledgement and Controller Task transition may occur after the last step
event.

### Surface compaction as `cursor.expired`

Rejected. Sequence-based replay can safely recover from a current snapshot,
whereas pagination must preserve one historical view. Exposing compaction
would unnecessarily couple clients to etcd retention.

### Emit an SSE error event after failure

Rejected. No public error-event schema exists, an error record could be
mistaken for durable Task history, and dependency diagnostics must not enter
the event stream.

### Retry every live storage failure inside the request

Rejected. Unbounded server-side retry hides liveness failure and complicates
shutdown. Disconnect plus standard SSE reconnection reuses the accepted
sequence resume point.

## Consequences

- Task-event replay and resume become independent of etcd revisions and the
  unresolved Task index value schemas.
- The repository needs one composed stream seam that owns a Task-primary watch,
  an event-prefix watch, fixed-revision snapshots, compaction resnapshot, and
  context-bound shutdown.
- The handler must complete request validation and its initial snapshot before
  committing SSE headers.
- Every connection emits each sequence at most once, while reconnect remains
  at-least-once and client-deduplicable.
- Terminal streams close deterministically only after a final durable drain.
- Compaction adds a bounded resnapshot of at most 1,000 events but creates no
  public error or cursor format.
- Live failures after headers deliberately trade an in-band diagnostic for a
  safe disconnect and standard client resume.
- Retention remains the authority for historical availability; no new
  tombstone, checkpoint, event key, or compatibility format is introduced.
- A Controller-authored assignment epoch keeps Agent event deduplication
  identity unique across executions without adding mutable delivery state.

## Accepted owner checklist

The owner approved each item on 2026-08-23:

- [x] SSE `id` is the canonical decimal Task-event sequence, never an etcd
  revision.
- [x] `Last-Event-ID` is absent or exactly one canonical `uint64` decimal;
  absent and `0` replay all, leading zeroes are rejected, and a value ahead of
  the fixed snapshot returns HTTP 400 before headers.
- [x] Initial delivery is a complete fixed-revision snapshot suffix followed
  by both watches at `R + 1`.
- [x] Delivery is sequence-ordered and exactly once per connection, with
  at-least-once delivery across reconnects and sequence filtering.
- [x] Terminal closure comes only from authoritative Task status and performs a
  fixed-revision final event drain.
- [x] Watch compaction transparently resnapshots; no etcd revision or
  `cursor.expired` result reaches the client.
- [x] Live storage, transport, corruption, and deletion failures after headers
  log safely and disconnect without an invented SSE error event.
- [x] Missing retention-pruned Tasks are 404 before headers and disconnect-only
  if deletion is observed after streaming starts.
- [x] Client reconnection uses `Last-Event-ID`; the server does not retry
  non-compaction storage failures indefinitely inside one request.
- [x] The implementation remains confined to the accepted Task primary and
  event keys and does not choose Task operation-index or active-operation-index
  value schemas.
- [x] The verification matrix, including focused `-race` coverage, is required
  before the slice is declared complete.
- [x] Every assignment delivery carries one positive Controller-authored event
  attempt; fixed-revision recovery advances only after durable event evidence,
  and expired recovered work is never dispatched.
