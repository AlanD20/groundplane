# Monitoring

Groundplane shows runtime health and resource usage for each scope. Readings
include their sample time and coverage so you can see how much of the selected
scope is reporting.

## Choose a scope

Open **Monitoring** in the sidebar of the Host, Tenant, Project or Environment
workspace. From Environment Monitoring, select **Inspect** on a Service to
open that Service's charts.

| Workspace | Contents |
| --- | --- |
| Host | Whole-host utilization, including non-GP processes; interface and block-device details. |
| Tenant | Owned workload totals and Projects. |
| Project | Owned workload totals and Environments. |
| Environment | Workload totals, Service health, replica readiness, restarts and observation freshness. |
| Service | Its resource history, limits, containers and persistent storage. |
| Backing Service | Instance-wide readings and consumer connections. |

An Attach does not allocate part of a shared database, cache or scanner's usage to
its consumer. Inspect the Backing instance for those readings. Per-consumer usage
requires independent application or database instrumentation.

## Read the measurements

CPU consumption uses cores. Utilization against an allocation needs an actual
configured limit; an absent limit is not zero capacity. Memory working set removes
reported inactive file cache from raw usage and remains distinct from the limit.
Whole-host memory and filesystem capacity are separate measurements.

Service readings cover the selected serving workload replicas. Retained releases,
Scripts and GP runtime helpers are not added to that workload total; their host
footprint remains part of whole-host utilization.

Network RX/TX and disk read/write rates come from consecutive cumulative counters.
Replacement, restart, reset or a long sampling gap begins a new segment. First
readings and unsupported runtime counters remain unavailable. Summed container
traffic is not host internet egress. Host totals use physical interfaces, while
the interface breakdown distinguishes virtual interfaces. Block devices and
partitions are listed separately and are not silently added together.

Container counter continuity includes the actual process start time. A manual
restart starts a new segment even when Docker retains the same container id and
restart count.

Persistent-volume usage and writable container layers are distinct. Shared Volumes
count once in parent totals. Each Volume shows its own observation time because
storage is measured less frequently than CPU, memory and I/O. After container
replacement, writable-layer usage stays unavailable until the next storage sample.

Health remains separate from usage and runtime intent. Running without a
healthcheck does not prove application readiness. A deliberately stopped Service
is different from an unexpected stop; neither an old reading nor a successful
Deploy Task proves current health.

## Inspect a chart

Select **Fullscreen** to expand the chart inside the Console tab while keeping
the time range, metric and live refresh controls available. Press Escape to exit.

Charts default to UTC. Use the time zone control to display times in your browser's
local zone. Groundplane remembers this choice in the browser. Hover a point to
inspect its exact interval, units, average, minimum, maximum,
sample counts and available contributor coverage. The line represents averages;
the band represents recorded extrema. Missing buckets are gaps rather than zero.
A parent total's extrema come from aligned total measurements, not the sum of
independent child minima or maxima.

Click or press Enter to pin an interval, use arrow keys to inspect adjacent
intervals, and press Escape to dismiss it. Inspection freezes the displayed
snapshot while new readings arrive; hovering does not send another query. Live
refresh and pause controls must retain the actual observation timestamp.

## Collection, retention and failures

One shared collector supplies all viewers. Host and workload live reads normally
run every five seconds. History keeps the latest accepted reading in each closed
15-second bucket. Storage is collected once per UTC minute; startup outside its
collection slot can leave storage unavailable until the next minute.

Environment totals begin recording when the feature is installed. Existing Host,
Tenant, Project and Service history stays available across the update.

History retains 15-second samples for two hours, one-minute buckets for 24 hours,
and five-minute buckets for seven days. Queries select 1h, 6h, 24h or 7d and return
at most 720 points. Counts distinguish valid samples from the expected cadence:
one minute normally expects four CPU samples and one storage sample.

The Controller keeps this derived history in
`/var/lib/groundplane/monitoring/metrics.sqlite`, separately from etcd and
application Volumes. The database has a 256 MiB page ceiling and a 16 MiB WAL
checkpoint target; the latter is not a hard transient file-size limit. History
failure leaves live collection available and preserves a corrupt file for
diagnosis. If the page ceiling blocks maintenance, the Controller removes
already-expired derived samples before retrying rollup. This can leave gaps
where expired samples were removed before rollup.

Collection and queries are bounded. A collection page has at most 512 Services;
when a larger inventory needs continuation, GP withholds a purported complete
Tenant/Project total. Container details and counter baselines have separate
4,096-entry cache limits, and an incomplete container list is explicitly
unavailable. These bounds do not imply support for unlimited historical or
per-container queries.

The current channel guard requires matching Controller/Agent build provenance
before requesting workload telemetry. A mismatched or unavailable Agent leaves
workload measurements unavailable while Host collection continues. Independent
upgrade coverage and real Docker/operator qualification remain pending; see
[capability status](../capabilities.md) and the
[integration review](../issues/monitoring.md).

Design ownership: [Platform runtime](../decisions/platform-runtime.md).
