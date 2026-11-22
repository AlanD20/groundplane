# Monitoring design review

Status: the visual direction and scoped navigation have been selected. General
workload resource collection, historical queries and these monitoring pages are
not implemented. This review records the intended behavior before that work is
promoted into the product and feature contracts.

The selected presentation is a health-first overview using the existing Console
design system. There is one monitoring destination within each resource's own
workspace, with breadcrumbs and an explicit return path when inspecting shared
infrastructure. Changing scope changes the page's contents and attribution, not
only a filter on a global dashboard.

## Scope and drilldown

| Workspace | Monitoring contents | Next inspection |
| --- | --- | --- |
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
