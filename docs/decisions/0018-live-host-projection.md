# ADR 0018: Deterministic live Host projection

- Status: Proposed
- Date: 2026-08-20

## Context

The Console store defines one `HostInfo` projection and the human API exposes
it through `GET /host` and `groundplane host show`. The typed Controller
service already preserves that shape behind independent system, etcd, and
Agent snapshot ports. It intentionally leaves source formatting undefined.

That gap prevents production wiring. The current fixture uses presentation
strings such as `34 days`, `5.1 GB`, and `18 MB`, while `cpu.load` is an
integer meter without a declared meaning. The etcd projection does not say
which configured endpoint supplies `node` and `db_size`. The Agent status does
not yet state how durable lifecycle phase and authenticated `Ready` freshness
map to the closed health vocabulary.

The Host page also renders `Last report` as the hard-coded text `2m ago`, but
`HostInfo` contains no corresponding field. That value cannot become live
without either deleting the row or adding an API field. A hard-coded live
observation is not an acceptable production fallback.

This ADR proposes the complete collection, formatting, and failure contract.
It remains Proposed: none of these choices may be implemented or copied into
the authoritative product documents until the owner accepts them.

## Accepted constraints

Any accepted decision must preserve these existing contracts:

- the MVP has one native Linux Controller, one local Controller-owned Agent
  OCI container, one host, and one logical single-node etcd;
- Host is a read-only projection, not a durable resource;
- `GET /host`, `host show`, and the Platform Host view are one capability;
- etcd is host-level and is not a Component;
- the Controller owns one fixed loopback etcd endpoint backed by its managed
  `groundplane-etcd` OCI container;
- authenticated `Ready`, not HTTP/2 keepalive, is the Agent liveness signal;
- the accepted Agent stale window is `max(3 * pull_interval, 30s)`;
- expected unhealthy dependency state is data and does not by itself turn a
  successfully collected Host projection into a failed request;
- infrastructure errors and endpoint addresses never cross the human API;
  and
- there are no compatibility fields, legacy collectors, disk fallbacks, or
  fixture values in production.

## Proposed decision

### 1. Public Host shape

Keep the current `HostInfo` shape and meanings, with one clean addition:

```text
agent.last_ready_at: string | null
```

The Go API field is a nullable timestamp. A non-null value is the
Controller-received time of the latest authenticated `Ready` for the current
Agent generation, normalized to UTC, truncated to whole seconds, and encoded
with `time.RFC3339`. The Console derives relative display text such as `2m
ago`; the Controller never stores or returns relative time. `null` means the
current generation has never produced an authenticated `Ready`, or no Agent
record exists.

No other Host field is added or renamed. In particular, `docker` remains the
Docker server version string, `etcd.node` remains a string projection, and
resource totals remain formatted strings because those are already part of
the Console store contract.

The Console's Controller API row is not Host state and does not create another
`HostInfo` field. It derives the displayed address from the Console client's
configured Controller base URL, or from the current document origin when no
explicit base URL is configured. It must not retain the fixture literal
`127.0.0.1:8080`, because the accepted Controller listener is configurable.

Remove the Host page's Scheduler row and its hard-coded `next backup 03:15`
text. Backup scheduling belongs to each environment's backup policy and its
Controller-owned Tasks; there is no host-level next-backup value, Host field,
or parallel scheduler status contract.

### 2. Host identity, uptime, and CPU load

The Linux system source collects:

- `hostname` from `os.Hostname`;
- `arch` from `runtime.GOARCH`;
- `os` from `PRETTY_NAME` in `/etc/os-release`;
- `cpu.cores` from `runtime.NumCPU`, meaning logical CPUs visible to the native
  Controller process; and
- `cpu.model` from the first non-empty `model name` in `/proc/cpuinfo`, or the
  literal `unknown` when Linux exposes no model-name field.

Uptime is the non-negative integral seconds from the first field of
`/proc/uptime`, with any fractional part discarded. It is formatted by
selecting exactly one largest whole unit and truncating toward zero:

| Raw uptime | Format |
| --- | --- |
| less than 60 seconds | `<n> second` or `<n> seconds` |
| less than 60 minutes | `<n> minute` or `<n> minutes` |
| less than 24 hours | `<n> hour` or `<n> hours` |
| 24 hours or more | `<n> day` or `<n> days` |

There is no week, month, or year unit and no rounding up. Thus 34 days and 23
hours is `34 days`.

`cpu.load` is not sampled CPU busy time. It is the one-minute Linux load
average from the first field of `/proc/loadavg`, normalized by visible logical
cores and expressed as an integer percentage:

```text
round-half-up(load_1m * 100 / logical_cores)
```

The decimal input is evaluated without binary floating-point boundary drift.
The result is non-negative and is not clamped: a four-core host with load 6.0
reports `150`. A Console meter may clamp only its visual width at 100; warning
severity and API output use the original value. This preserves overload
information and gives the existing field name `load` its ordinary Linux
meaning without a request-time sampling delay.

### 3. Memory, root disk, swap, and byte formatting

Memory comes from `/proc/meminfo`:

```text
memory.total = MemTotal
memory.used  = MemTotal - MemAvailable
swap.total   = SwapTotal
swap.used    = SwapTotal - SwapFree
```

The collector does not substitute `MemFree` for `MemAvailable`. Missing or
contradictory required values are malformed host observations.

Disk is the filesystem containing `/`, collected with `statfs`:

```text
disk.total = blocks * fragment_size
disk.used  = (blocks - free_blocks) * fragment_size
```

`free_blocks`, rather than unprivileged `available_blocks`, keeps the displayed
`used / total` pair arithmetically consistent. No Groundplane data-directory,
Docker-root, or aggregate-filesystem heuristic replaces `/` in the one-host
MVP.

Every byte total and used value uses the same formatter. Units are binary and
named accurately: `B`, `KiB`, `MiB`, `GiB`, `TiB`, then `PiB`. Select the
largest unit whose value is at least one. Round half-up to one decimal place,
promoting to the next unit if that rounding would produce `1024.0`, and remove
a trailing `.0`. Bytes are emitted as whole integers. Examples are `0 B`, `768
MiB`, `5.1 GiB`, and `256 GiB`.

Each `used_pct` is calculated from the unformatted byte values and rounded
half-up to the nearest integer:

```text
round-half-up(used_bytes * 100 / total_bytes)
```

The value is clamped to 0 through 100 only after malformed `used > total` has
been rejected. Zero swap is valid and projects `0 B`, `0 B`, and `0`. Zero
memory or disk total is invalid.

### 4. Docker version

The Docker value is the daemon's server version from the Docker Engine API,
not the client library version, CLI version, Compose version, image version,
or a subprocess output. A successful non-empty server version is trimmed of
surrounding ASCII whitespace and otherwise preserved verbatim, for example
`27.3.1`.

Docker daemon unavailability is an expected Host observation. It projects the
literal `unavailable` and leaves the rest of the Host response readable. A
malformed successful response, such as an empty server version, is an internal
collector error rather than an alternate sentinel. The adapter logs the
private cause; the API exposes no socket path or daemon error text.

### 5. Single-node etcd projection

The production etcd source evaluates every configured endpoint in configured
order with the same Status-RPC validation used by the local etcd diagnostic.
Each endpoint gets a five-second child deadline. A valid result has a non-nil
header, no reported errors, a non-zero member id, and a non-negative `DbSize`.

The selected row is the first valid result in configured order:

- `etcd.node` is that row's member id encoded as unsigned decimal;
- `etcd.db_size` is that row's `DbSize` formatted by the common IEC byte
  formatter; and
- configured endpoint addresses, leader ids, revisions, latency, and error
  strings are not part of Host.

Health is deterministic:

- `healthy` when every configured endpoint succeeds and every result names the
  selected single member;
- `degraded` when at least one endpoint succeeds but another fails or names a
  different member; and
- `failed` when no endpoint succeeds.

When no endpoint succeeds, `node` and `db_size` are both the literal `unknown`.
An endpoint timeout, connection failure, or invalid Status response is a
per-endpoint unhealthy result and does not fail `GET /host`. Caller
cancellation still stops the complete request. Failures in collector setup or
cleanup are internal errors because they do not describe etcd health.

This rule supports multiple configured addresses for one logical member
without silently accepting a multi-member topology. The Host projection does
not use the fixture literal `single-node`: topology is already known, while
the safe member id identifies the node actually observed.

### 6. Agent source, freshness, and display projection

The Agent source is the existing local Agent lifecycle manager's `Health`
projection. It combines the singleton durable record and config with the
current generation's authenticated session snapshot. It does not use Docker
container presence, HTTP/2 keepalive, an Agent-reported wall clock, or a
generic resource-observation record as a substitute for `Ready`.

Status maps exactly as follows:

| Durable/session state | Host status |
| --- | --- |
| no Agent record | `stopped` |
| phase `provisioning` | `pending` |
| phase `deleting` | `stopped` |
| phase `ready` and current-generation `Health.Healthy` | `healthy` |
| phase `ready` without a current, online, non-stale authenticated `Ready` | `failed` |

At exactly `last_ready + max(3 * pull_interval, 30s)` the Agent remains fresh;
it becomes stale only when the Controller clock is later than that instant.
A stale or disconnected Agent is failed rather than degraded because it may
not receive new assignments. Unknown lifecycle phases or impossible health
combinations are malformed durable/internal state, not new public statuses.

The remaining fields come only from the same durable Agent config:

- `pull_interval` is the positive integer seconds followed by `s`, without
  unit folding, so 120 seconds is `120s`;
- `max_concurrent` is `max_concurrent_tasks` without transformation; and
- `labels` is the config map rendered as `key=value` strings sorted by raw key
  and then value. It is a display projection and is never parsed back into
  configuration.

When no Agent record exists, the exact empty projection is `pull_interval:
n/a`, `max_concurrent: 0`, `labels: []`, and `last_ready_at: null`. A matching
session snapshot may retain `last_ready_at` after disconnect; a missing or
different-generation snapshot may not.

### 7. Request and unhealthy behavior

`GET /host` returns HTTP 200 when all required local system values were
collected, even when Docker is unavailable, etcd is degraded/failed, the Agent
is pending/stopped/failed, or swap is absent. Those conditions are represented
by the exact values above.

The request fails instead of inventing data when:

- hostname, OS release, uptime, CPU load, memory, or root-filesystem
  collection is unreadable or malformed;
- the Agent durable record exists but is malformed;
- the Agent repository cannot be read, including `storage.unavailable`; or
- source setup/cleanup violates an infrastructure invariant.

Already classified errors retain the closed error taxonomy. In particular,
Agent repository unavailability returns canonical `storage.unavailable` HTTP
503; malformed host/durable data returns the opaque internal HTTP 500 problem.
Raw paths, endpoint errors, Docker errors, and record contents remain private.
Caller cancellation and deadline sentinels propagate unchanged.

The Controller projection remains `healthy` whenever it is serving this
response. Its service name and build version are explicit application wiring,
not host probes.

### 8. Production seams and ownership

Raw collection and presentation formatting remain separate:

- add dependency-light raw observation DTOs under
  `internal/common/hostobs`; they contain integer seconds/bytes, the parsed
  decimal load value, endpoint outcomes, and Agent health/config values, but
  no `pkg/api` types or formatted strings;
- implement Linux identity, procfs, statfs, and Docker Engine collection under
  `internal/infra/host`; inject file reads, statfs, and a narrow Docker
  server-version client for hermetic tests;
- extend `internal/infra/etcd` with a production Host observation that uses
  the Controller-owned client and shares the pure Status-response mapper with
  `controller etcd show`; the diagnostic may still create direct clients
  because it is an independent same-host tool;
- adapt `internal/controller/localagent.Manager.Health` to the raw Agent
  observation behind a narrow Controller port;
- keep all public formatting, health mapping, cloning, and error normalization
  in `HostService`; and
- construct and inject the three sources in `internal/app`, which also owns
  Docker/etcd client shutdown.

The existing formatted `SystemSnapshot`, `EtcdSnapshot`, and `AgentSnapshot`
ports are replaced cleanly by raw observation ports when this ADR is accepted.
They are not kept as compatibility adapters. Infrastructure does not import
`pkg/api` or `internal/controller`, and the application layer does not become
a second formatting implementation.

Procfs and statfs use Go file/syscall APIs. Docker and etcd use their official
clients. No new subprocess or Runner exception is introduced.

## Alternatives considered

### Treat CPU load as sampled busy time

Rejected because the accepted field is named `load`, the Linux one-minute load
average is available atomically, and a busy-time percentage would require a
request-dependent sampling interval or a background cache with startup
semantics. Normalized load also retains queued work; values over 100 make
overload visible.

### Use decimal `KB`, `MB`, and `GB`

Rejected because procfs and filesystem counters resolve to bytes and the
fixture's labels would make binary quantities ambiguous. IEC units make the
conversion explicit and reproducible.

### Return the literal `single-node` or an endpoint as `etcd.node`

Rejected because `single-node` repeats topology without identifying the
observed member, while an endpoint leaks deployment configuration and becomes
unstable when addresses change. The member id is safe and stable.

### Derive Agent health from Docker or generic observed state

Rejected because neither proves that the authenticated task loop is ready.
ADR 0011 already makes current-generation `Ready` authoritative and fixes its
stale window.

### Keep `Last report` hard-coded or format relative time in the API

Rejected because fixture text is false in production and relative text changes
without the resource changing. A nullable absolute timestamp is stable and
lets every frontend render the same observation appropriately.

### Fail the whole request for every unhealthy dependency

Rejected because the Host capability exists to display Docker, etcd, and Agent
failure. Only an inability to construct required fields or read required
durable state is a request error.

## Consequences if accepted

- Host values have one reproducible meaning across Console, CLI, API, tests,
  and real-host acceptance.
- Existing fixture resource labels change from ambiguous `GB`/`MB` to IEC
  units, and the etcd fixture uses a decimal member id.
- The Console/API Agent shape gains `last_ready_at` and removes its hard-coded
  report age without a compatibility alias.
- The Console derives the Controller API address from its active client origin
  and never persists or receives it through `HostInfo`.
- The false Host-level scheduler/next-backup row is removed; environment
  backup policy and Tasks remain the only scheduling surfaces.
- Host collection remains diagnostic during Docker and etcd endpoint failure
  while malformed local/durable state fails closed.
- Infrastructure returns raw observations and cannot silently define public
  formatting.
- C01 remains Scaffolded until the accepted contract is implemented across
  application wiring, generated clients, Console, CLI, tests, and the L2 host
  scenario.

## Owner approval required

The owner must explicitly approve this complete ADR, including:

- the `agent.last_ready_at` public-field addition and removal of the hard-coded
  Console value;
- deriving the Controller API display from the configured/current Console
  origin without adding a Host field, and removing the Host Scheduler row;
- one-unit truncated uptime, normalized one-minute Linux load, IEC byte
  formatting, and half-up percentages;
- Docker's `unavailable` sentinel and the system hard-failure boundary;
- first-successful configured etcd selection, decimal member-id projection,
  `unknown` sentinels, and healthy/degraded/failed rules;
- the exact Agent phase/Ready mapping, empty projection, label rendering, and
  repository-error behavior; and
- clean replacement of formatted collector ports with raw observation seams.

Until that approval, this ADR stays Proposed, the authoritative documents and
Console store remain unchanged, and C01 remains Scaffolded.
