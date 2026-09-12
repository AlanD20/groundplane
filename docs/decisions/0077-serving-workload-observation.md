# ADR 0077: Bounded serving-workload observation

- Status: Accepted within the owner's approved hosting/visibility initiative
- Date: 2026-09-12
- Capability: Service list/show; no new operator action or mutation

## Context

Service reads expose desired fields and runtime intent, but neither proves what
is running. Historical Task success is not current health. Counting every
container bearing a Service id would include retained blue/green workloads and
the stable proxy, hiding missing serving replicas.

## Decision

Use an on-demand, read-only exchange on the existing authenticated Agent stream.
The Controller captures the current serving Release and its immutable render
input at one storage revision. The selected workload carries Environment,
Service, Release, plan, render-generation, Compose-name and slot identity. Its
expected replica count comes from the Release's workload seal, never current
desired replicas. Only singleton/selected-slot workload containers count; stable
proxies, Script runners, Components and retained releases are excluded. This
reports workload process/healthcheck state, not routing or application reachability.

`internal/common/serviceobservation` owns the closed machine validation and pure
count aggregation. `internal/infra/docker/serviceobserver` owns Docker reads via
a consumer-owned list/inspect-only port. Agent/channel own exchange lifetime;
the Controller's Service observation module owns source selection, freshness and
public projection. No observation query renders Compose, resolves Entry values,
reads logs, executes a command, inspects storage contents or mutates Docker.

One request contains 1..200 unique Services from one Environment, no arbitrary
filters, labels, paths or Docker arguments. Each protobuf envelope is at most
262,144 bytes. Docker accepts at most 4,096 candidate containers; listing may
fetch one extra row solely to detect overflow. Only selected serving workloads
are inspected. Exceeding a bound is unavailable, not a
truncated healthy sample. Inspect rechecks ownership against the frozen target;
wrong generation, duplicate replica identity, malformed state, inspection error
or mid-read removal makes that Service unavailable. An empty complete selection
is an observed absent workload, distinct from unavailable evidence.

Each result contains exactly one row per target in request order. A row is
either closed replica counts or an unavailable outcome, never partial counts
plus failure. Counts partition containers into running without a healthcheck,
running healthy, running with a starting healthcheck, running unhealthy,
transitional (created/paused/restarting/removing), exited successfully, and
failed (dead or nonzero exit). No container environment, labels, names, images,
log output or healthcheck output crosses back to the Controller.

One observation worker per connection is independent of image lookup and Task
capacity. Both peers cap the whole exchange at five seconds. Busy/disconnected
reads produce unavailable observations without Task creation or automatic
reconnect retry. Correlation uses a raw ULID from the existing id allocator plus
the exact authenticated session fence. Cancellation and disconnect cancel/join
the worker; late, malformed or replacement-session results are unusable.

`ControllerMessage.cancel_service_observation` (tag 15) carries only the
request's raw ULID. The stream sends cancellation before a subsequent observation
request on that connection. The Agent ignores stale cancellation, joins the
matching worker and discards its output before reusing the slot. Read failures
return unavailable rows; they do not terminate otherwise usable Task traffic.

Before returning live evidence, the Controller rechecks the serving projection
and Service runtime-intent revisions. A changed source yields unavailable, not
evidence attached to a different Release. The public observation window starts
at the Controller's request-start time (a conservative lower bound) and expires
15 seconds later. There is no persisted observation cache or Agent clock
dependency. Missing serving authority, observation failure or expired evidence
is unavailable. Desired reads still succeed when live evidence is unavailable.

Existing Service list/show must include `observation` with state `unavailable`, or a
bounded snapshot with `observed_at`, `expires_at`, `serving_release_id`, sealed
`expected_replicas`, and replica counts. Available state is exactly `absent`
(zero containers), `failed` (all failed), `stopped` (all exited successfully),
`starting` (all transitional/starting), `healthy` (exact count, all running and
healthchecked healthy), `running` (exact count, all running, some without a
healthcheck), or `degraded` (every other nonempty combination). Runtime intent
remains a separate field. Create/edit responses do not claim a fresh observation.
Console and CLI show the same states/counts. The Console expires old evidence
locally and refreshes visible Services; Environment summaries do not turn missing,
stopped, absent or starting workloads into Healthy. Provisioning state is separate.

## Alternatives and consequences

Unsolicited host-wide drift feeds and a durable health cache add scan, retention
and freshness authority unnecessary for this read surface. Reusing execution
plans would import materialization and mutation authority into a read. Desired
intent or the most recent Task cannot substitute for runtime evidence.

This leaves detailed replica logs, routing probes, backing-resource health and
Component observation with their existing owners. There is no Blueprint field,
new public endpoint, Task, durable schema or compatibility reader. The native
upgrade installs a matching Controller/Agent protocol before this is qualified.
Local protocol/Docker proof precedes channel/source/surface integration; none
alone constitutes delivered live Service visibility. QA mutation remains paused
behind storage/source integrity qualification.

## Implementation boundary

The first slice implements protobuf messages, closed request/result validation,
pure count aggregation and the list/inspect-only Docker observer. The tests cover
selection, ownership changes, malformed evidence, duplicate replicas, overflow,
cancellation, per-Service failures and the actual Docker client/protobuf seam.
Agent/channel wiring and composition are implemented with focused proof: one
observation worker, request/session matching, explicit cancellation and join,
and unavailable responses independent of image lookup and Task capacity.
Controller source selection and freshness are implemented in
`internal/controller/serviceobservation`: fixed-revision capture, immutable digest
binding, sealed expectations and a post-read revision recheck. Public observation
fields, generated API clients, Console/CLI expiry and Environment aggregation
remain subsequent work. See
[Service status](../features/services-and-releases.md#current-status) for proof.
