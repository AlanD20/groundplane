# Monitoring design review

Status: the visual direction and scoped navigation have been selected. The
Console's Environment health roster and Service/Backing monitoring destinations
use existing live runtime observations, including expiry, search and pagination.
Shared read-only collection, bounded derived history, production lifecycle and
Console/CLI/API integration are implemented in source. Real Docker/operator and
upgrade qualification remain pending; production charts do not substitute
demonstration data. The [monitoring guide](../features/monitoring.md) now owns
operator behavior, and [Platform runtime](../decisions/platform-runtime.md#resource-telemetry)
owns the implementation rationale. This review retains the selected visual
direction and qualification concerns; it is not a delivery claim.

The selected presentation is a health-first overview using the existing Console
design system. There is one monitoring destination within each resource's own
workspace, with breadcrumbs and an explicit return path when inspecting shared
infrastructure. Changing scope changes the page's contents and attribution, not
only a filter on a global dashboard.

## Scope and drilldown

| Workspace | Monitoring contents | Next inspection |
| --- | --- | --- |
| Platform / Host | Live host utilization and history for CPU, memory, filesystems, interfaces and block devices, including processes outside GP. | Host resource breakdown. |
| Tenant | Aggregate owned workload usage, health, missing observations and Projects. | Project monitoring. |
| Project | Aggregate owned workload usage, health and Environments. | Environment monitoring. |
| Environment | Health of every Service: observed state, ready replicas, restarts and observation freshness. | That Service's monitoring. |
| Service | Its resource history, health, configured limits and individual containers. | Container readings within the same Service. |
| Backing Service | Instance-wide resource history, health, containers and consuming connections. | Consumer context without inventing a usage allocation. |

Environment monitoring is a health overview; detailed CPU, memory, network,
storage and I/O charts belong to Service monitoring. Tenant and Project views
can show aggregate usage because their purpose includes comparing descendants.

Backing Services remain Platform-owned. An Attach is a relationship, not a copy
of the instance. Shared PostgreSQL, Valkey or ClamAV usage must not be counted
again in every consuming Environment, Project or Tenant. Per-consumer database
usage requires independent instrumentation and cannot be inferred from the
Backing container's totals.

Live utilization and historical inspection share the same resource attribution.
Visible pages may refresh current readings more frequently than historical
retention buckets, within bounded collection and query budgets. Show the sample
time and live/paused state; a refresh control must not imply that an old reading
was just measured. Platform totals come from host accounting, never a sum of
containers. Keep physical and virtual network interfaces distinguishable to avoid
counting the same traffic twice. Disk-device readings and filesystem capacity
also have different meanings; neither may be silently added across overlapping
devices or mounts.

## Metric semantics

| Reading | Presentation and constraints |
| --- | --- |
| Health | Keep observed health, desired replicas and readiness distinct. Stopped, degraded, unknown and stale are separate states. |
| CPU | Core consumption with an explicit unit. Show utilization against an actual configured allocation only when that denominator is known. |
| Memory | Define working set versus total usage and file cache. Show the configured limit separately from host capacity. |
| Network | Receive/transmit byte rates derived from cumulative counters. Explain internal traffic; summed container traffic is not host internet egress. |
| Storage | Separate persistent-volume usage from writable container layers. Count a shared Volume once; do not present host filesystem capacity as a per-Volume quota. |
| Disk I/O | Read/write byte rates, and operation rates when the runtime reports them. Availability depends on the runtime and storage driver. |
| Coverage | Show sample time and gaps. A missing or unsupported reading is unavailable, not zero. |

Counter resets and container replacement must start a new series segment rather
than produce a negative rate or join unrelated container identities. Aggregates
must indicate coverage when only some descendants have fresh samples.

Chart inspection shows the actual timestamp or bucket interval with its time
zone, units, aggregation method, valid/expected sample count and contributor
coverage. Aggregated points retain average, minimum and maximum from underlying
measurements; the average line and min/max band have distinct labels. Inspection
must remain stable while live updates arrive, with keyboard navigation and a
way to pin or dismiss the selected point. Hover does not issue another query.

Extrema of a parent total require aligned total samples. Adding independent
Service minima or maxima does not establish the minimum or maximum of their
sum. If retained evidence cannot establish an extremum, show it as unavailable
rather than label a bound or an average as the actual extreme.

## Collection and query constraints

Resolve samples through GP's stable resource and container identities. A name or
Docker label alone must not authorize attributing a foreign container to a GP
resource. Keep application, Backing and Platform infrastructure attribution
distinct.

Historical metric samples must not be written into etcd. Its role is authoritative
coordination state; the [upgrade recovery review](upgrade-recovery.md) records
why disk latency and query load matter to that role. Collection, retention,
storage location and resource budgets need an explicit implementation decision.

Queries need bounded time ranges, sample counts and resource cardinality. Return
downsampled series appropriate to the chart width. Paginate resource inventories;
do not fetch every container or every historical sample to render a Tenant page.
Hover, selection and chart inspection must not trigger fresh backend queries.
Losing Controller or Agent coordination may create an observation gap; it must
not affect serving workloads or turn missing metrics into an unhealthy-runtime
claim.

Before implementation is declared complete, define a common read interface and
its Console, CLI and API surfaces under the [surface parity rules](../api-cli.md).
The prototype's synthetic readings and time controls are visual evidence only,
not qualification of collection, retention or resource attribution.
