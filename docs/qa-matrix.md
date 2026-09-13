# Product QA matrix

This is the case catalogue for Groundplane product qualification. It answers
what must be tested, what result is required, and what evidence exists. It is
organized around operator and application outcomes, not packages or CLI flags.
The requirements remain owned by [mvp.md](mvp.md), [Blueprint grammar](blueprint.md),
[operator surfaces](api-cli.md), and the feature documents linked below.

Building, formatting, generated-code checks and unit coverage are delivery
prerequisites, not passing product cases. A completed Task or healthy process
alone does not prove that an application works, its data survived, or recovery
restored the correct configuration.

## Qualification rules

- Every product test has a stable case ID here, a setup/action, and an independently
  observable expected result. Name the case ID in its automated test or manual
  run record. Multiple implementation tests may support one product case; their
  success does not substitute for that case's required operator journey.
- Document a new behavior, regression or fault case before running it. Update
  the affected cases when requirements change. Preserve previous failed runs.
  Do not add cases merely to mirror branches in the implementation.
- Tests start from a known configuration and known data. Submit actions through
  normal product surfaces. Check actual application protocols, content, identities,
  permissions and persisted data as appropriate. Expected values must come from
  the fixture and requirement, not from the same GP projection being checked.
- Each named variant needs its own result. Use `<case-id>/<variant>` in records;
  a row passes only when every required variant passes. Record non-applicability
  with its contract reason; a skip, missing prerequisite or unavailable feature
  is not a pass. Do not silently shrink the selected test set.
- Each feature's actions require Console, CLI and API parity. Reuse one expensive
  runtime journey where equivalent surface behavior has separate evidence; do
  not repeat destructive work simply to obtain three identical runs. Generated
  clients or mocked Console fixtures alone do not establish live surface parity.
- One intentional fault at a time. Record baseline, fault point, deadline,
  application behavior throughout, final state and cleanup. A fault that was not
  actually injected is not a recovery test. A manual repair is mitigation, not
  successful automatic recovery. Stop dependent faults after a failed recovery.
- Record reliability bounds before execution. Use an existing contract deadline
  where specified; otherwise obtain an operator decision. Do not invent an uptime,
  latency, capacity or recovery-time target and then claim production readiness.
- A case or gap here is not permission to implement a feature, fix a failure,
  mutate a host, or change a requirement. Report failures and limitations for the
  user's decision. [head.md](head.md) records current operational authority.

The catalogue covers all current feature areas, including accepted but incomplete
features. It is not a claim that every existing test has been correlated or that
all possible failures have been discovered. Unmapped coverage stays visible.

## Reading results

Initial evidence review: 2026-09-13. A clean integrated candidate has not yet been
selected for the full run. Historical results below do **not** qualify current
uncommitted source or a future release.

| Record | Meaning |
| --- | --- |
| U | Unverified: no reviewed execution record covers the complete case. Existing tests may exist; do not infer either success or absence. |
| NOT RUN | The named operator journey is known not to have run. |
| PARTIAL H… | The linked historical record covers only the stated subset, surfaces or conditions. |
| PASS H… | The exact dated case passed on the recorded build and target; not a claim about another build. |
| FAIL H… | A required outcome failed. It stays failed until a subsequent qualifying run explicitly closes it. |
| BLOCKED D… | Execution or an expected outcome needs the named implementation, authority or requirement decision. |

H references resolve in [the evidence register](acceptance.md#matrix-evidence-register).
D references resolve under [decisions and blocked coverage](#decisions-and-blocked-coverage).
Every new run follows [the case run record](acceptance.md#case-run-record).
Automation availability and execution result are separate fields in that record.

## Coverage map

| Product area | Cases | Requirement owner |
| --- | --- | --- |
| Host, bootstrap and settings | HOST | [Platform](features/platform.md) |
| Ownership and hierarchy | OWN | [Hierarchy](features/hierarchy.md) |
| Console, CLI and API behavior | UI | [Console and API](features/console-and-api.md) |
| Blueprint authoring and reconciliation | BP | [Blueprints](features/blueprints.md) |
| Services, observation and Release Groups | SVC, OBS, GRP | [Services and Releases](features/services-and-releases.md) |
| Tasks, Activity and logs | TASK, LOG | [Tasks and logs](features/tasks-and-logs.md) |
| Zones, DNS, Components and ingress | NET, CMP, DNS, HTTP | [Components](features/components.md), [router templates](features/router-template.md) |
| Volumes and Entries | VOL, ENT | [Storage and Entries](features/storage-and-entries.md) |
| Backing services and Attaches | BACK, ATT | [Backing services](features/backing-services.md) |
| Secrets and Connectors | SEC, CON | [Secrets and Connectors](features/secrets-and-connectors.md) |
| Scripts and hooks | SCRIPT | [Setup Scripts](features/setup-scripts.md) |
| Backups, Restore and keys | BAK | [Backups](features/backups.md) |
| Isolated CI execution | RUN | [Runners](features/runners.md) |
| Controller and Agent self-upgrade | UP | [Safe updates](features/upgrade-safety.md) |
| Cross-feature hosting and reliability | JOURNEY, REL | [MVP acceptance gates](mvp.md#acceptance-gates) and the feature owners above |

## Host, bootstrap and settings

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| HOST-01 | Read an existing healthy Host through API and CLI. | Canonical Host fields and safe dependency states agree semantically; service state is unchanged and owned tunnel/runtime cleanup succeeds. | PASS H1 |
| HOST-02 | Bootstrap a clean supported machine; separately test amd64 and arm64 release variants. | Native Controller owns exactly the intended etcd and Agent runtimes; Agent authenticates and executes a harmless owned Task; no cyclic bootstrap or additional persistent GP unit. | U |
| HOST-03 | Try overlapping pools, a protected host-route overlap, unsupported platform and wrong-platform release. | Reject before installation effects; host routing and existing Docker resources remain unchanged. | U |
| HOST-04 | Enroll, remove, then re-enroll the Agent through normal actions. | Singleton ownership and new-generation authentication hold; removed credentials cannot execute; API/CLI never return the channel token. | U |
| HOST-05 | Lose Agent connectivity; restore the authenticated current generation. | Health becomes degraded rather than falsely healthy; valid Ready restores health; stale sessions cannot claim or acknowledge work. | U |
| HOST-06 | Make Docker or etcd unavailable, separately; restore it. | Public health represents actual availability without raw secrets/endpoints; Controller-owned runtimes recover without duplicate ownership; checked application data survives. | U |
| HOST-07 | Edit Agent concurrency, interval and labels while work is admitted. | Drain/reconfigure applies the exact saved settings without restarting running Tasks or exceeding either worker or durable-claim capacity. | U |
| HOST-08 | Save exact Controller YAML, then restart; separately revert exact startup bytes. | Comments and bytes persist with mode 0600; restart-required is correct before/after restart or revert; runtime changes only when specified. | U |
| HOST-09 | Submit stale, malformed, multi-document, unknown-field or semantically invalid Controller config. | Invalid/stale input leaves file and effective configuration unchanged; exact protected replay returns the original result; changed intent under the same key is rejected. | U |
| HOST-10 | Exercise missing/unreadable host sources and health-format boundaries. | Documented error or unavailable state, IEC units and rounding; no invented values, persisted health history or credential disclosure. Confirm expected raw values independently. | U |
| HOST-11 | Attempt wildcard/public human-API binding and public Tunnel exposure. | Unsupported unauthenticated exposure is refused; authorized loopback/private access remains functional. | U |

## Ownership and hierarchy

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| OWN-01 | Create/list/show Tenant, Project and Environment; include same labels in allowed distinct scopes. | Exact owners and stable IDs persist across restart; scoped lookup never returns a different owner's resource. | U |
| OWN-02 | Rename each hierarchy level while descendants contain running Services and data. | Old label stops resolving, new label resolves; IDs, runtime names, physical paths, mounts, references and data do not change. | U |
| OWN-03 | Edit display name; separately submit a no-op rename, duplicate slug and invalid slug. | Only intended labels change; no-op causes no mutation; duplicates/invalid grammar are rejected rather than normalized. | U |
| OWN-04 | Race rename with unrelated edit and with deletion start. | No lost field update or write after the deletion fence; bounded conflicts are reported, not retried indefinitely. | U |
| OWN-05 | Delete an owned populated aggregate through its confirmation workflow. | Descendants finalize in defined order and parent last; only selected resources/data are removed; unrelated applications still work. | U |
| OWN-06 | Fail, Abort, time out or restart midway through aggregate deletion; Retry where supported. | Same operation/fences survive; finalized children are not resurrected; reserved names/IDs are not reused early; unfinished work is visible and resumable. | U |
| OWN-07 | Delete a parent containing separately active work. | No silent adoption/cancellation of unrelated Tasks; conflict and remaining ownership are accurate. | U |
| OWN-08 | Submit foreign IDs through each resource's read, mutation and reference fields. | Documented ownership rules are enforced without leaking values or publishing foreign effects. This is resource authorization, not a strict tenant-network isolation claim. | U |

## Console, CLI and API behavior

Apply UI-01 through UI-06 to the actions of every area above; identify the exact
operation and surface as run variants. [api-cli.md](api-cli.md) supplies the closed
operation inventory and local-tooling exemptions, not an alternative test plan.

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| UI-01 | Perform the same supported action/read through each operator surface. | Same intent, validation, resource/Task identity and observable effect; no missing Console action or frontend-only product decision. | U |
| UI-02 | Use slug lookup, stable ID lookup and explicit scope overrides with colliding labels. | Selected resource is the intended one; JSON/YAML/table reflect the same values; rename does not break ID-based use. | U |
| UI-03 | Submit missing, unknown, conflicting and boundary-valued inputs. | Actionable contract error before effects; no accidental default, partial resource or accepted Task. | PARTIAL H6 |
| UI-04 | Repeat a mutation's exact key/intent, change its intent, then lose the acceptance response. | Exact replay resolves the original operation; changed intent fails; uncertainty is shown and safely resolved without a duplicate action. | PARTIAL H6 |
| UI-05 | Disconnect Controller while loading or mutating; reconnect or reload the Console. | Explicit unavailable/progress state, no fixture fallback or false success; accepted operation identity survives and action controls prevent unsafe repeats. | U |
| UI-06 | Use keyboard and mobile/desktop views for each feature's forms, errors, confirmations and progress. | Controls remain reachable and labeled; focus/error feedback and navigation preserve the same capability and scope. | U |
| UI-07 | Navigate and reload canonical SPA routes using the installed production binary without an adjacent asset directory. | Correct UI assets and deep links work; API, malformed/traversal paths and operational endpoints never become HTML or arbitrary file reads. | U |
| UI-08 | Exercise GET/HEAD, HTML negotiation and browser cache reload after an upgrade. | Correct body/MIME/cache/security headers; new HTML resolves the matching assets; stale cached UI does not misreport an operation. | U |

## Blueprint authoring and reconciliation

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| BP-01 | Export, import, validate and export a complete Environment with every supported resource kind. | Canonical decisions round-trip, including Script bodies, exposure, grants and routing; no credentials, generated runtime, observations or lost required fields. | U |
| BP-02 | Validate a proposed create/update against existing resources. | Accurate create/update/retain diff and zero desired, Task, runtime or data writes. | U |
| BP-03 | Try missing/stale/malformed If-Match, concurrent edits and exact request replay. | Only correctly revision-bound input is accepted; stale input never overwrites current desired state; exact replay has one operation. | U |
| BP-04 | First Apply a valid complete workload with host-local images and ordered setup hooks. | Declared resources and exact replicas serve real requests; all selected pre-hooks clean up before any candidate consumer starts. | PARTIAL H3 |
| BP-05 | Reapply the exact successful Blueprint. | Same stable IDs, generated values and serving Releases; no duplicate resources, restarted workloads or rerun hooks. | PARTIAL H3 |
| BP-06 | Omit existing direct, Blueprint and Component-owned resources from a new valid input. | Omitted resources remain; no implicit delete/rename; ambiguous preservation fails closed. | U |
| BP-07 | Import unsafe/incomplete bundles: escaping paths/symlinks, absent referenced files, unknown fields, invalid YAML and unresolved variables. | Closed-bundle and grammar errors before publication or host effects; no external file disclosure. | U |
| BP-08 | Supply dependency cycles, missing dependencies, unsupported Compose behavior, mutable/missing images or invalid rendered configuration. | Specific validation failure before dispatch; running state and persisted data remain unchanged. | U |
| BP-09 | Edit an Entry source/exposure under the same authored key; separately change an immutable identity field. | Supported edits retain ID with exact new value generation; identity-changing edit is rejected; API/Component-owned Entries remain intact. | U |
| BP-10 | Change desired input after a Task is accepted but before claim or terminal acknowledgement. | Execution uses its sealed authority; stale execution cannot overwrite newer desired intent or claim unexecuted applied success; immutable history remains accurate. | U |
| BP-11 | Fail publication/terminal persistence at record and transaction bounds; include 10 and 32 candidates with hooks. | No partial public state, premature source deletion or truncated recovery evidence; accepted maximum input completes through the actual boundary. | U |
| BP-12 | Lose terminal response or restart during hook-source cleanup, then reconnect/replay. | Same Task/result and timestamp; cleanup resumes without another hook start or lost source; no fabricated successful completion. | U |
| BP-13 | Submit valid A, B, C while A runs; separately submit invalid B. | Only current relevant units execute after safe handoff; queued obsolete work is cancelled; invalid input does not cancel valid work; unrelated resources continue. | BLOCKED D1 |
| BP-14 | Supersede after a shared configuration write; separately leave its effect unknown or its executor unconfirmed stopped. | Accounted divergence repairs forward only after proven stop; unknown effects retain conflicting claims; original partial outcome and actual Service health stay visible. | BLOCKED D1 |
| BP-15 | Revert desired values to an earlier value after partial effects or mixed member success. | Fingerprints compare verified per-resource inputs/effects, not an aggregate desired document; required repair is not skipped and successful unrelated work is not repeated. | BLOCKED D1 |

## Services and Releases

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| SVC-01 | Add/edit a Service without deploying, including replica/image/config changes. | Desired state changes only; serving runtime, data and Release identity do not change until an explicit lifecycle/reconcile action. | U |
| SVC-02 | Start, Stop, then Start a configured Service; repeat each action. | Exact runtime intent and replica count, no duplicate workload; persisted Volume data survives; repeated action follows the documented state rule. | U |
| SVC-03 | Destroy runtime, then Start. | Runtime disappears but desired definition, Volumes/data and immutable history remain; Start recreates the intended workload. | U |
| SVC-04 | Remove a Service using fixed-revision impact confirmation. | Referencing resources handled as declared; only selected runtime/definition removed; unrelated Volumes, Services and Release history preserved. | U |
| SVC-05 | Deploy recreate with one and multiple replicas, including a portless worker. | Exact desired count and image run; application/job results are correct; downtime is measured, not hidden or called zero-downtime. | U |
| SVC-06 | Deploy a healthy blue-green singleton while sending requests and holding a WebSocket. | Old workload serves until candidate health passes; stable proxy switches to exact candidate; recorded traffic and retained predecessor meet the contract. | U |
| SVC-07 | Request blue-green with more than one replica or an unsupported strategy. | Reject before Task/runtime mutation; previous serving application remains unchanged. | U |
| SVC-08 | Fail first startup with no serving predecessor; repeat beside an unrelated running Service. | Remove only plan-owned failed candidate/proxy; no false serving Release; unrelated workload and data stay unchanged. | U |
| SVC-09 | Fail a candidate before health/promotion with a real serving predecessor. | Exact predecessor remains/restores, independent application check succeeds, original failure persists on the same Task; no migration reversal. | U |
| SVC-10 | Roll back after repeated tags and mixed successful/failed Deploys; test explicit and omitted tag. | Select the documented previous successful different tag, not merely the last Task; correct image/config serves; history remains immutable. | U |
| SVC-11 | Race Deploy/Rollback on one Service; separately deploy independent Services. | No conflicting executor or double switch; rejection/serialization follows the owning conflict contract without corrupting unrelated work. | U |
| SVC-12 | Abort, time out or lose Agent acknowledgement after a rollout effect. | Recorded effects and exact predecessor authority govern recovery; no false no-effect result, duplicate switch or silent success. | U |
| SVC-13 | Submit altered/missing/foreign predecessor evidence or stale source authority at publication, claim and acknowledgement. | Fail closed without selecting latest desired or inventing historical state; preserve last acknowledged runtime and immutable records. | PARTIAL H9 |
| SVC-14 | Change Attach memberships, then Deploy with existing hooks. | Only current declared memberships reach the new workload; hooks use their captured authored context; detached endpoints stay absent. | PARTIAL H4 |
| SVC-15 | Change Attach and Entry configuration successfully, then fail the next rollout. | Actual predecessor uses the last successfully applied bindings and serves authenticated requests; original Release input/history is not rewritten. | FAIL H5 |
| SVC-16 | Prune the earlier configuration Task, restart Controller, then perform supported rollout/recovery. | Required acknowledged inputs remain available independent of pruned Task history; no fallback to obsolete Release configuration. | PARTIAL H9 |
| SVC-17 | Fail persistence between runtime acknowledgement and serving projection publication; replay the report. | Runtime receipt, serving state and Task outcome are atomic and exact; no promotion from failed, skipped, compensated or merely staged work. | PARTIAL H9 |

## Serving observations

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| OBS-01 | Observe healthy singleton, replicated and portless Services through all surfaces. | Counts correspond to actual selected workload containers; proxy/inactive/unrelated containers are not counted as replicas. | PARTIAL H3 |
| OBS-02 | Make replicas starting, unhealthy, missing, stopped and restarting, separately. | Correct counts and runtime status; desired intent, provisioning and route reachability remain distinct. | U |
| OBS-03 | Stall observation past its 15-second window and the Console's next refresh. | Evidence expires to unavailable rather than stale healthy; desired reads still work; no overlapping refresh loop. | U |
| OBS-04 | Change serving Release or Agent generation while a read is in flight. | Old response cannot become current health; fresh read binds the current serving expectation and authenticated Agent. | U |

## Release Groups

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| GRP-01 | Add/edit/show/remove group definitions without execution. | Declared member order, policy and tag persist; no incidental Deploy or deletion of member Services. | U |
| GRP-02 | Deploy mixed singleton/replicated members with hooks and saved/request tag variants. | Correct tag precedence, declared serial order, once-per-Service hooks and exact replica counts; application dependencies work. | U |
| GRP-03 | Fail a later member under switch_back and leave_active, separately. | Per-policy member outcomes and selected recovery are accurate; skipped members are not promoted; original failure and real application state agree. | U |
| GRP-04 | Preview then execute automatic or explicit rollback; change eligible history between the requests. | Preview is read-only; execution resolves and validates its own immutable selected members/tags; no frontend history guess or stale unchecked selection. | U |
| GRP-05 | Abort, lose acknowledgement or restart during group execution/recovery. | Resume the owning operation without repeated successful hooks, reordered members or fabricated group success. | U |

## Tasks, Activity and logs

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| TASK-01 | Observe pending/running/terminal Tasks and Activity in Platform and Tenant scopes. | Same records, ordering and cursor boundaries; correct immutable owner, actor and Controller times, including after parent rename/deletion. | U |
| TASK-02 | Replay concurrent equal requests and changed intent under the same key. | One resource/Task/effect for equal intent; changed intent rejected durably, including after Controller restart. | PARTIAL H6 |
| TASK-03 | Abort pending, Agent-running and Controller-running work; repeat Abort. | Exact target terminates according to its effect contract; no extra Task; already-aborted replay returns the same result. | PARTIAL H6 |
| TASK-04 | Race Abort with natural completion; Abort terminal work or an offline/stale Agent assignment. | Correct conflict or safe delivery failure; no false undone effect or released ownership; eventual timeout remains bounded. | PARTIAL H6 |
| TASK-05 | Retry an eligible failed Task; try unsafe, superseded and unsupported retries separately. | Allowed attempt preserves original operation inputs/owner; forbidden retry cannot restart effects or silently rebase desired state. | PARTIAL H6 |
| TASK-06 | Disconnect/resume event stream at boundaries, during compaction and terminal drain. | Immutable sequences have no loss or duplicate effects; terminal state is consistent; context cancellation releases subscriptions. | U |
| TASK-07 | Exceed event count/size limits or submit output/secret-bearing diagnostics. | Bounded typed events and safe errors; no secret/subprocess output in history and no unbounded growth. | U |
| TASK-08 | Restart during 90-day terminal retention cleanup with shared retry inputs. | Only eligible history removed; live Tasks, required runtime receipts and shared inputs remain; cleanup resumes without orphaning records. | U |
| TASK-09 | Deliver a valid report that conflicts during durable publication. | Assignment is quarantined for that session without an Agent crash loop, false success or lost claim; unrelated work obeys both capacity limits. | U |
| TASK-10 | Reconnect with stale identity, wrong epoch, mismatched plan or malformed report. | Reject invalid authority; valid generation can reconcile the original immutable operation; no repeated unknown effects. | U |
| LOG-01 | Read finite and follow logs for Service and Environment, including replicas and stopped workloads. | Correct selected sources and tail bounds; no unrelated scope or retained inactive-slot output. | U |
| LOG-02 | Disconnect/reconnect, cancel or stall log streams. | Transient stream semantics are explicit; no invented historical resume, leaked subscription or durable Task-output storage. | U |

## Zones and Components

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| NET-01 | Allocate multiple Environment pools/Zones and restart Controller. | Globally disjoint durable reservations; actual bridges have declared subnet/internal settings; only Zone children become workload networks. | U |
| NET-02 | Race exact Zone creation and overlapping subnet requests; edit an Environment pool containing existing Zones. | Exact replay creates once; one overlapping reservation wins; edits retain all owned Zones and exclude other reservations. | U |
| NET-03 | Place Services on distinct Zones, then explicitly join a reachable target. | Actual permitted routing follows declared membership; no accidental host publication or claim of strict shared-bridge peer isolation. | U |
| NET-04 | Remove a Zone using full impact confirmation, including backing consumers and last membership. | Correct warning/dependencies; exact network cleanup before reservation release; no-zone projection and unrelated networks stay correct. | U |
| NET-05 | Interrupt Zone cleanup or encounter a foreign Docker network/ownership label. | Reservation remains fenced while owned cleanup is incomplete; no adoption or deletion of unmanaged resources. | U |
| CMP-01 | Enable/configure/disable/update each supported Component in its correct scope. | Closed typed settings, immutable image and intended effect; disabled/unconfigured config is null; no duplicate router or wrong owner. | U |
| CMP-02 | Read/preview Component configuration while its desired inputs change. | One consistent read-only view and managed-file array; no writes or claim that preview proves serving bytes. | U |
| CMP-03 | Read Component-owned resources directly and through collections; attempt direct mutation. | Ordinary collections exclude them, detail groups them, known-ID reads work; unauthorized direct mutation fails. | U |
| CMP-04 | Reject invalid grants/catalog/source revision or race candidate publication. | No partial Component graph or address publication; current active state remains authoritative until successful terminal commit. | U |
| CMP-05 | Fail, Abort or time out a Component candidate; lose terminal acknowledgement. | Preserve old active config/address; release only candidate reservations; replay causes no duplicate mutation or address leak. | U |

## DNS and application ingress

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| DNS-01 | Enable CoreDNS and resolve declared internal Routes plus configured upstream domains from real clients. | Exact names resolve to the correct Environment router address; network Zone labels are not invented DNS zones. | U |
| DNS-02 | Configure full Corefile, upstreams, per-domain forwarders and explicit override of detected defaults. | Actual DNS answers follow accepted configuration; exactly one managed marker; invalid input preserves serving DNS. | U |
| DNS-03 | Disable/remove DNS or fail its activation after resolver-file staging. | Restore the owned predecessor resolver configuration; unrelated host resolver state is not overwritten; name resolution remains demonstrably correct. | U |
| HTTP-01 | Create Routes with router disabled, then enable and disable it. | Routes persist as unserved without router; enable serves them; disable preserves declarations without false reachability. | U |
| HTTP-02 | Request distinct host/path matches, missing routes, catch-all and denied paths. | Correct application/content or explicit denial on each authorized ingress; unmatched requests never leak another Service. | PARTIAL H3 |
| HTTP-03 | Edit a full Caddyfile with aggregate/per-Route references and native placeholders. | Byte-preserving policy outside substitutions; all Routes covered; upstream is stable Service/port, not a transient slot. | U |
| HTTP-04 | Supply missing Route references, legacy/unknown markers, bad UTF-8, NUL or over-32-KiB template. | Reject before runtime mutation; declared Routes and currently served requests remain unchanged. | U |
| HTTP-05 | Submit renderable but Caddy-invalid configuration; separately fail reload/restoration. | Pinned native validation precedes activation; old serving bytes/traffic retained on rejection; failed restoration is visibly failed. | U |
| HTTP-06 | Reload only Route/template files while holding traffic and WebSockets. | Correct new policy with unchanged router/Tunnel container ownership; actual long-lived connections and requests satisfy continuity assertions. | U |
| HTTP-07 | Change primary/secondary router Zones and optional alias; test alias collision and disable/re-enable. | Pinned primary address/alias rules and stable IDs hold; unreachable targets/collisions reject; Tunnel membership does not follow implicitly. | U |
| HTTP-08 | Enable Tunnel with its Secret and explicit Zones; test private and provider-facing ingress separately. | Actual intended public HTTP/WebSocket path works with configured policy; token stays scoped; GP does not mutate provider DNS/ingress. | PARTIAL H7 |
| HTTP-09 | Reach the stable proxy across both replicas and a Release switch; issue callbacks and long-lived connections. | Correct routing/content and authorization throughout; each replica is exercised rather than repeatedly hitting one healthy instance. | PARTIAL H3 |
| HTTP-10 | Use external production-like TLS certificates; test valid, wrong-host, expired and untrusted-chain clients. | Valid chain/hostname succeeds; invalid trust fails; certificate deployment and reload preserve intended ownership without issuing a production CA implicitly. | U |

## Volumes and Entries

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| VOL-01 | Create a managed Volume, write known content, then rename its slug and parent labels. | Stable ID, Compose key, managed root, Docker object, mount/Backup references and content remain unchanged. | U |
| VOL-02 | Materialize nested paths; try absolute/traversal/backslash/noncanonical/outside-root paths and symlink substitution. | Only authorized canonical paths and exact ownership exist; invalid or substituted paths cannot alter outside files. | U |
| VOL-03 | Remove a Service that consumes a Volume. | Service removal leaves Volume ownership, root and known data intact. | U |
| VOL-04 | Preview Volume removal across all impact pages; change references before confirmation or provide a wrong key/token. | Complete fixed-revision impact is required; stale/incomplete/wrong confirmation cannot start destruction. | U |
| VOL-05 | Remove an applied Volume through its accepted detach-and-cleanup operation. | Only confirmed consumers detach; owned root is removed before final publication; unrelated data and mounts remain. | U |
| VOL-06 | Remove a never-applied Volume beside a similarly named unmanaged path. | No filesystem deletion effect; unmanaged path unchanged; intended definition is removed. | U |
| VOL-07 | Fail, Abort, time out or restart during Volume removal, then use allowed Retry. | Exact ownership/removal authority survives; reconcile proven effects without losing retry state or reporting partial cleanup as success. | U |
| VOL-08 | Interrupt create before/after ownership-marker publication; provide a pre-existing unexpected leaf. | Resume only matching owned creation; never adopt a foreign leaf or delete it as cleanup. | U |
| VOL-09 | Race Volume changes with another Environment mutation. | Revision-bound publication prevents lost edits and premature promotion of unapplied storage. | U |
| ENT-01 | Create plain and secret literal Entries and inspect reads/history plus controlled storage/materialization. | Only documented plain values are public; secret values remain encrypted at rest and confined to authorized reveal/materialization. | U |
| ENT-02 | Create Secret-backed and Attach-fact Entries; change source availability after Task acceptance, then resume it. | Captured bytes/generation remain exact for accepted execution; a new operation resolves current sources, never silently rebases the old Task. | U |
| ENT-03 | Edit source/exposure; separately attempt immutable kind/destination/secret-class/ownership changes. | Permitted edits retain identity and choose a new generation; forbidden identity changes publish nothing. | U |
| ENT-04 | Materialize file Entries with numeric uid/gid including zero; try missing file ownership or ownership on env Entries. | Exact file content/ownership and secret-safe mode; invalid ownership combinations rejected before effects. | U |
| ENT-05 | Try reserved, escaping, conflicting and noncanonical destinations. | No outside-root write, conflicting ownership or stale generated reference; valid paths remain relative to the owned Environment root. | U |
| ENT-06 | Change exposure from all to selected Services, then remove one exposure; replay around value publication. | Exact eligible consumers receive the value; stale env/file references disappear; metadata/value generation publishes atomically once. | U |
| ENT-07 | Change Entry exposed to running, stopped and absent Services. | Only captured exposed running workloads reconcile; no dependency, Component, stable proxy or inactive-slot startup. | U |
| ENT-08 | Attach new backing, Detach old backing, then edit an Entry and Deploy. | Current complete Attach union persists; old networks/deleted decorations do not return from historical Release artifacts. | PARTIAL H4 |
| ENT-09 | Change Environment epoch or captured source between preparation, publication and terminal commit. | Stale authority rejects; no substituted live slot, latest Entry value or premature applied state. | U |
| ENT-10 | Remove an applied Entry; repeat with failure, Abort and timeout. | Success removes exact owned metadata/generations and runtime references; non-success preserves required value and cleanup/recovery authority. | U |
| ENT-11 | Remove never-applied Entry; complete materialization-only edit while unrelated Volume/Compose work is pending. | No invented host cleanup or workload startup; no promotion of the pending Volume or replacement of independently applied Compose state. | U |

## Backing services and Attaches

Protocol variants are explicit: PostgreSQL and Valkey `username_password`,
`password`, and `none`. No authentication option may be silently defaulted.
Shared backing bridges are not strict peer-isolation boundaries.

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| BACK-01 | Create each managed adapter using valid explicit pool/Zone inputs. | One protected operation creates exactly one backing Project, main Environment, owned Zone, Service, data Volume and Task; actual protocol becomes usable. | U |
| BACK-02 | Use overlapping/reserved/out-of-pool/noncanonical subnets; race conflicting creates. | Rejected aggregate publishes no partial resource, reservation, index or Task. | U |
| BACK-03 | Replay exact create and reuse its key with changed intent. | Original linked IDs/response replay; no second hierarchy, database initialization or leaked reservation. | U |
| BACK-04 | Try unsupported adapter, existing-Zone branch, uploaded backing Blueprint, image or procedure override. | Reject unsupported managed creation before effects; only compiled adapter behavior executes. | U |
| BACK-05 | Omit Valkey authentication or submit empty/null/unknown; select each valid mode through all surfaces. | No default/preselection; invalid input publishes nothing; valid immutable instance policy is preserved through Attach preparation. | PARTIAL H6; PARTIAL H9 |
| BACK-06 | Connect real clients in every supported mode; try missing, wrong and valid credentials. | Exact mode-specific fact fields and authentication behavior; no Attach weakens instance policy; password-only works without a username. | PARTIAL H3 |
| BACK-07 | Write known data, Stop/Start, Destroy/Start backing runtime. | Only runtime intent changes; actual data, credentials, Volumes, configuration and Attach ownership survive. | U |
| BACK-08 | Attempt permanent backing deletion. | No unsupported permanent-delete operation or Task; Destroy cannot silently delete persistent data. | U |
| BACK-09 | Restart Valkey with several credential owners and grants. | Persisted ACL identities still authenticate; users are not regenerated, erased or given wider privileges. | U |
| BACK-10 | Attempt consumer administration; induce managed image/ACL/procedure mismatch. | Forbidden admin operations denied; management fails closed without mutable-image or arbitrary-shell substitution. | PARTIAL H3 |
| ATT-01 | Create new credential owner with zero/eight/over-eight grants. | Supported grants provision exactly once; excess/foreign grants reject before effects; actual database access matches selected grants. | U |
| ATT-02 | Reuse a ready direct owner; try dependent/non-ready/wrong-backing/wrong-Environment owner and additional grants. | Only allowed direct-owner reuse succeeds; invalid selection creates no Attach, Task or broadened access. | U |
| ATT-03 | Attach several consumers to one credential and one consumer to multiple backings. | Each consumer retains its own stable Attach/network edge; actual network union is sorted/deduplicated and scoped; no automatic env injection. | U |
| ATT-04 | Create with boundary and over-56-byte Service names; rename Attach afterward. | Bounded database/role identity derives from the Attach random tail, not its label/timestamp; no truncation/collision; rename preserves credentials and references. | U |
| ATT-05 | List/show and explicitly reveal ready facts; try non-ready reveal or caller scope override. | Metadata has keys/classification only; authorized exact facts derive scope from durable ownership; invalid access reveals no values. | U |
| ATT-06 | Map ordinary and granted fact sets into Entries; try an absent/unrelated grant. | Exact selected database/role/value reaches only exposed consumers; omitted grant uses owner's set; unauthorized reference fails. | U |
| ATT-07 | Detach dependent, then owner; race new dependent with owner Detach. | Owner remains protected while referenced; dependent removal removes only its edge; final revoke/deprovision/network removal follows recorded order. | U |
| ATT-08 | Attach/Detach a running native consumer beside dependencies, Components, proxy and inactive slot. | Only selected active workload membership changes; retained runtime identities, Release and data remain intact. | PARTIAL H4; PARTIAL H9 |
| ATT-09 | Attach/Detach stopped and configured-only consumers with historical Releases. | Validate configuration without starting or recreating a workload. | PARTIAL H9 |
| ATT-10 | Change sources/epoch during publication, claim and acknowledgement; replay and Retry. | Stale authority cannot execute/promote; exact replay preserves IDs/plan/current union and does not repeat credential provisioning. | PARTIAL H9 |
| ATT-11 | Fail provisioning, fact publication, credential use or acknowledgement. | No false ready facts, leaked plaintext or widened access; exact effects and retry/recovery authority remain accounted for. | PARTIAL H9 |
| ATT-12 | Overlap two same-name backings during consumer cutover; check DNS and actual authentication before and after Detach. | New endpoint selection is unambiguous at each claimed successful stage; any shared-name collision remains a reported limitation, not a pass inferred from direct-IP access. | FAIL H8 |
| ATT-13 | Exercise an authored manual/network-only Attach where permitted by the Blueprint contract. | Network edge only: no generated database, role, password, managed facts or GP backup; do not invent a managed manual-backing create operation. | U |

## Secrets and Connectors

Secret/credential rotation is deferred; these cases do not introduce a rotation
operation. Test existing add, resolve, reveal, delete and fallback behavior.

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| SEC-01 | Create Project and Platform Secrets; duplicate keys within/across scopes and try Environment ownership. | Exactly one permitted owner; scoped uniqueness; no Environment or ambiguous ownership. | U |
| SEC-02 | Resolve matching Project/Platform keys while one changes or is deleted. | One fixed-revision Project-first selection; documented fallback or failure on later resolution, never mixed-version bytes. | U |
| SEC-03 | Resolve stable IDs from owning Project, foreign Project and Platform contexts. | Only permitted Project/Platform references succeed; no cross-Project plaintext disclosure. | U |
| SEC-04 | Create env/file Secrets with valid and escaping/reserved paths; test UTF-8 at/above 255 KiB and invalid text. | Correct derived/relative references; invalid paths/text/size publish neither metadata nor ciphertext/replay records. | U |
| SEC-05 | Create via CLI file/stdin; inspect normal responses, history and controlled process/durable evidence; explicitly reveal. | No plaintext argv or normal read value/digest; value visible only through authorized reveal; protected intent is encrypted. | U |
| SEC-06 | Replay create; delete a referenced Secret and fail/Abort/time out deletion. | Exact replay; tombstone hides resolution while finalizing; success removes exact value, failure restores prescribed visibility; references do not silently preserve deleted data. | U |
| CON-01 | Create/list/show/remove same-named Environment Connectors; try rename or cross-scope fallback. | Environment-only unique immutable name and stable ID; no cross-Environment lookup fallback. | U |
| CON-02 | Test endpoint scheme/authority/path, bucket length/case and normalized prefix boundaries. | Exact S3 addressing validation before publication; no userinfo/query/fragment, unsafe prefix or undeclared address normalization. | U |
| CON-03 | Omit region/path_style; test region length/characters and explicit auto. | Required explicit values; valid region preserved exactly; invalid/absent choices do not default silently. | U |
| CON-04 | Use direct and reusable Secret credentials; omit/add fields, mix source forms or reference file-kind Secret. | Exactly access_key and secret_key with one valid source each; normal responses expose source metadata only, not resolved credentials. | U |
| CON-05 | Make endpoint unavailable during Connector CRUD; change/delete Secret source before first real use. | CRUD performs no provider probe; actual operation resolves current authorized source/fallback or fails safely without rewriting Connector intent. | U |
| CON-06 | Delete while policy, Recovery Point or orphan references remain; fail/interrupt permitted cleanup. | Required deletion blockers and credential/object authority remain until exact cleanup completes; no abandoned remote data ownership. | U |
| CON-07 | Request operation credentials with wrong Task/step/purpose; interrupt/disconnect the correct request. | Only exact active bounded tuple receives transient values; terminal/session paths clear slots without leaking data or resending unknown effects. | U |
| CON-08 | Use authorized S3 operations against the selected provider; test ambient credentials/proxy/redirect/CA influence. | Sealed addressing/credentials govern I/O; immutable metadata, conditional create, multipart completion, delete and VersionId/ETag behavior match the accepted contract. | U |

## Backups, Restore and encryption keys

BAK cases require real known source data and application-level reads after
restoration. A local archive, successful upload or visible Recovery Point alone
does not qualify recoverability. A multi-source Backup is not a cross-source
transactional snapshot. Detailed fixture/format limits come from the linked
[Backup contracts](features/backups.md), not a new matrix-specific format.

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| BAK-01 | Replace/disable/enable policy; vary frequency, keep, Connector, sources and encryption; try backing Environment policy. | Exact valid singleton replacement; invalid enabled policy creates nothing; no consumer backup/age key on backing Environment. | U |
| BAK-02 | Add/remove/re-add/reorder sources; duplicate tuples, over-12 sources or second Config. | Stable referenced source IDs and stored order; unsupported cardinality/duplicates rejected without losing Recovery Point references. | U |
| BAK-03 | Select Valkey, manual or unknown Backup source. | strategy.not_implemented before Task publication; no unsafe capture of a live data directory. Successful Valkey recovery remains a separate unresolved requirement. | U; D2 |
| BAK-04 | Trigger daily/weekly schedules with downtime, overlap, repeated ticks and backward clock movement. | Only latest missed occurrence dispatches once; overlap is recorded; no repeated backup for one occurrence. | U |
| BAK-05 | Run bodyless manual backup with enabled/disabled policy; fail a later source and Retry after policy changes. | Frozen ordered source set; verified earlier points remain; Retry skips committed uploads and uses original intent, not the new policy. | BLOCKED D2 |
| BAK-06 | Capture PostgreSQL Attach, Config and Volume separately with known data; upload and verify. | Per-source correct immutable artifact and exact remote verification before point visibility; source usability/data unchanged by capture. | BLOCKED D2 |
| BAK-07 | Fail upload/Head verification or lose point-commit acknowledgement; paginate points concurrently. | No unverified point exposed; exact reconciliation without duplicate object/point; fixed-revision safe newest-first metadata. | BLOCKED D2 |
| BAK-08 | Lower retention keep, run source and interrupt pruning at each destructive boundary. | Retain required newest verified points per source; delete only exact sealed objects, no bucket-list inference; uncertain/mismatched ownership retains authority. | BLOCKED D2 |
| BAK-09 | Select explicit/default Restore point while newer point races acceptance; replay/Retry and remove source from policy. | Same fixed selected point/key era and original surviving target; no switch to latest snapshot; policy removal alone does not destroy restoration authority. | BLOCKED D2 |
| BAK-10 | Corrupt/truncate/append object, mismatch evidence or exceed archive/staging bounds. | Full download, length/hash, decryption and strict decoded validation complete before any live-target overwrite; failure preserves target data. | BLOCKED D2 |
| BAK-11 | Restore PostgreSQL with running and stopped consumers; fail validation/apply. | Only selected database overwritten transactionally after quiescence; verified content works; only previously running consumers restart; point unchanged. | BLOCKED D2 |
| BAK-12 | Restore Config with additions/replacements/deletions and secret/fact values; interrupt roll-forward. | Complete canonical Entry set from authenticated artifact, no intermediate public set or reread of current Secrets; one captured render generation. | BLOCKED D2 |
| BAK-13 | Restore Volume; fail before/after verified hidden-tree exchange and during old-tree cleanup. | Same-filesystem verified replacement, exact resumable cleanup and correct permissions/data; prior running consumers restart only after required verification. | BLOCKED D2 |
| BAK-14 | First enable age encryption, rotate key, create later point and export key. | Lazy era creation; rotation changes future points only; export is explicit and creates no Task/store record or secret-bearing history. | U |
| BAK-15 | Restore old era with valid/missing/wrong supplied identity; attempt generic Retry. | Recipient-bound transient identity required, not persisted or leaked; wrong era fails before overwrite; no unsafe generic retry without its key authority. | BLOCKED D2 |
| BAK-16 | Restart/reconnect/lose checkpoints or terminal ACK during backup/restore/cleanup. | Original six-hour deadline and exact operation survive; no repeated committed effect; cleanup failure overrides apparent success. | BLOCKED D2 |
| BAK-17 | Delete Environment with retained remote points and uncertain remote deletion. | Parent, credentials and orphan authority retained until every exact object is proven absent; unrelated provider objects untouched. | BLOCKED D2 |

## Isolated Runners

RUN cases qualify GP's isolated CI execution, not hosted application runtime.
They stay visible in full-product coverage; a private hosting run must explicitly
declare whether Runner qualification is selected. Fresh registration credentials
and any external GitHub effects require appropriate authority.

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| RUN-01 | Add direct Tenant and Project Runners; validate GitHub URL/labels; rename slug. | One immutable owner and canonical registration fields; slug edit changes no runtime identity, epoch, image or registration. | U |
| RUN-02 | Race creation across direct/Project scopes at the five-per-Tenant quota. | All non-deleted lifecycle states count; no sixth allocation; failed/pending removal does not release quota early. | U |
| RUN-03 | Allocate/remove/reallocate Runners; exhaust slots, subordinate ranges and network inventory. | Lowest free persisted UID/GID slot, unique 65,536 subordinate blocks and owned /29; fail safely without expanding pools or guessing. | U |
| RUN-04 | Run an actual build/job and service container; probe host Docker, privileged capabilities, host ports and other private scopes. | Job uses its dedicated rootless daemon; forbidden host/other-runner/Environment/backing access denied; documented DNS/internet/authenticated Controller access works. | U |
| RUN-05 | Create with invalid owner/URL/labels or missing fresh token; observe valid provisioning order. | No invalid host effect; valid resource/quota/allocation/Task authority durably published before host mutation. | U |
| RUN-06 | Inspect token handling across configure, job start, publication failure, partial delivery and reconnect. | Token consumed once by configure and absent from Listener/jobs, logs, argv, persistent records/digests; uncertain delivery is not resent. | U |
| RUN-07 | Fail registration; attempt generic Task Retry, then Runner Retry with fresh token; replay original create. | Correct registration_token_required failure; only supported retry creates new attempt preserving allocation; original replay never feeds a replacement token. | U |
| RUN-08 | Make local ownership/readiness evidence stale, missing or mismatched; attempt operator online mutation. | Actual offline/observed-time projection without private allocation details; online is observation, not operator desired state. | U |
| RUN-09 | Remove Runner; fail cleanup then Retry; check registration separately. | Local exact-ownership absence precedes quota/allocation release; failure retains authority; GP does not silently deregister GitHub. | U |
| RUN-10 | Restart same boot and reboot at journal boundaries; use zero/one/multiple discovered runtimes and corrupt authority. | Only uniquely sealed identity is adopted; uncertain authority quiesces; old epoch is cleaned before next; no foreign cleanup. | U |
| RUN-11 | Repeat supported job, denial and lifecycle journeys on amd64 and arm64. | Same accepted product outcomes on each real architecture, with exact release/runtime identities; no emulation substituted for host proof. | U |

## Scripts and hooks

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| SCRIPT-01 | Add/edit/remove Script body, order and complete execution context through all surfaces and Blueprint export. | Correct round-trip; whole context replaced rather than merged; edits affect future captures only, not accepted snapshots. | U |
| SCRIPT-02 | Run manual Script against serving Release; attempt it without one. | Exactly one scoped runner uses captured serving context; absent serving Release rejects; no synthetic initializer Service or Release. | PARTIAL H6 |
| SCRIPT-03 | First Deploy with pre-hooks of different/equal order, multiple Services and replicas. | Numeric then ASCII order within phase; dependency/group order across Services; one run per Service, all pre-hooks cleaned before any candidate starts. | PARTIAL H3 |
| SCRIPT-04 | Execute inherited hook after current Attach changes and after source edits. | Correct captured Release networking/image and authored hook sources; future edits do not change accepted execution. | PARTIAL H4 |
| SCRIPT-05 | Execute explicit setup with writable Volume and exact eligible Entries; then start read-only consumer. | Intended files/content/uid:gid reach consumer; no image-to-empty-Volume copy, inherited environment, network, host path or Docker socket access. | U |
| SCRIPT-06 | Try missing/foreign grants, overlap/traversal/reserved mount targets, mutable/missing image and invalid user/context. | Reject before effects; no access to other Service credentials; declared grant/count/input limits enforced. | U |
| SCRIPT-07 | Fail hook before consumer start; fail post/on-failure hooks separately under their recorded policy. | Correct Release context and typed outcome; no premature consumer start, false rollout success or claimed reversal of Script data writes. | U |
| SCRIPT-08 | Abort or reach 900-second timeout after runner start. | Exact runner terminated and cleaned; durable outcome is truthful; unsafe Retry cannot repeat potentially completed effects. | PARTIAL H6 |
| SCRIPT-09 | Lose start/terminal acknowledgement or reconnect during cleanup; change or remove source concurrently. | Captured sources retained as required; no second runner/start; uncertain effects stay fenced until safely resolved. | U |
| SCRIPT-10 | Emit large/secret-bearing stdout/stderr. | Output drained/discarded; no secret-bearing Task events/log store, blocked runner or unbounded retained output. | U |

## Controller and Agent self-upgrade

For UP cases, baseline records include real application transactions, a durable
sentinel, every relevant container identity/image/start time and a held WebSocket.
Measure traffic before, during and after the update with a probe that keeps
running through API disconnects. Record failures, reconnects and latency, not
only the final healthy state. Controller/API reconnection is distinct from
application interruption. A machine reboot is not an interruption-free GP update.

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| UP-01 | Install a valid staged Controller/Agent release through the normal protected update action. | Exact candidate identities selected; same update Task survives API reconnect; application containers/data/routing remain intact with no observed request failure or held-WebSocket reconnect during the recorded window. | PARTIAL H2 |
| UP-02 | Perform standalone idle Agent update and subsequent enrollment using the qualified native selection. | Correct desired Agent image/generation; application containers and traffic remain unchanged; no self-owned Agent replacement. | U |
| UP-03 | Request an already selected image or invalid/incompatible/corrupt/missing release candidate. | Documented refusal before replacement; same serving versions/history/data and no restart or new application effect. | PARTIAL H6 |
| UP-04 | Start update with active work and race assignment admission against drain. | Busy refusal or bounded drain; active work not aborted; no new admitted send crosses the pause and no Script/migration replay. | U |
| UP-05 | Abort or fail preparation before activation; race with permanent removal/revocation. | Release only the operation-owned pause; no replacement or revival of a removed/stale generation. | U |
| UP-06 | Abort after activation is committed or uncertain. | Refuse unsafe cancellation; preserve recovery and dispatch hold until exact commit outcome is resolved. | U |
| UP-07 | Activate a Controller candidate that cannot run at all. | Product-owned predecessor recovery works without the failed binary; exact same Task remains failed/recovered; real application data/traffic/identities survive. | PARTIAL H2 |
| UP-08 | Fail candidate readiness or coordinated Agent startup after native activation. | Restore compatible predecessor binary/Agent identities, preserve selected qualified release and application state; recovery failure is explicit. | U |
| UP-09 | Restart/kill Controller at each declared activation-journal phase, including lost commit response. | Durable original Task and holds restored before dispatch; finish or recover exact candidate once without reopening an obsolete generation. | U |
| UP-10 | Attempt ordinary writes and scheduled work during an unfinished native trial. | Guarded writes refuse; permitted reads, exact acceptance replay and pre-activation Abort still work; mere HTTP readiness grants no write authority. | U |
| UP-11 | Reload Console or lose update acceptance response and retry with the same key. | One retained protected request and original Task; no duplicate native update; visible running digest/candidate/result match actual executable. | U |
| UP-12 | Exercise successful and failed update through every selected ingress, with ongoing worker jobs and held connections. | Private/router/provider path and background work each meet recorded continuity assertions; no route is inferred from another path's success. | PARTIAL H2; PARTIAL H7 |
| UP-13 | Perform two successive qualified upgrades and a failed third candidate, then restart normally. | Latest successful selection remains authoritative; predecessor recovery and future Agent selection use correct retained release, not bootstrap or failed trial values. | U |

## Cross-feature hosting and reliability

| Case | Setup and action | Pass condition | Record |
| --- | --- | --- | --- |
| JOURNEY-01 | Fresh hierarchy → backing resources → Entries/Volumes → Blueprint/hooks → Routes → application use. | Real authenticated transaction, durable read/write, background work and realtime result; configuration/data ownership and denied access independently verified. | PARTIAL H3 |
| JOURNEY-02 | Reapply → Attach new backing → rebind Entry → detach old backing → Deploy → fail next candidate. | Each completed change remains effective through later operations/recovery; actual clients use only intended endpoints and credentials; original failure is not hidden by redeploy. | FAIL H5 |
| JOURNEY-03 | Deploy a second independent application and operate on the first. | Unselected runtime identities, data and working requests remain unchanged; declared shared dependencies are recorded rather than assumed isolated. | PARTIAL H4 |
| JOURNEY-04 | Write known data → backup selected sources → mutate data → restore exact point/key era. | Original surviving targets match the selected recovery point; unrelated data unchanged; an application-level query proves usability, not just file/object presence. | BLOCKED D2 |
| JOURNEY-05 | Application traffic and background work → GP update → rejected/broken GP update → later successful update. | Original application functionality and data survive the complete sequence; same operation outcomes and measured continuity, no manual runtime repairs. | PARTIAL H2 |
| REL-01 | Restart Controller normally under an application workload. | Correct recovered platform/task state and preserved data/serving Releases; measure actual traffic and dependent runtime restarts against the agreed restart contract. | BLOCKED D3 |
| REL-02 | Reboot the authorized QA machine under known state and unfinished work. | Bounded recovery of owned runtimes, data and safely resumable work; no duplicate effects or lost acknowledged state. Record reboot downtime, never claim single-host HA. | NOT RUN; D4 |
| REL-03 | Interrupt an actually running Task by Agent/process/channel loss at an identified effect boundary. | Exact operation resumes/terminates safely; unknown effects are fenced; no duplicate Script, data mutation or detached claim. | NOT RUN; D4 |
| REL-04 | Run an agreed sustained mixed workload with repeated reads/writes and realtime activity. | Every expected result/sequence accounted for; error/latency/resource/restart measurements satisfy predeclared limits with no unexplained drift or data loss. | BLOCKED D5 |
| REL-05 | Apply bounded CPU/memory/disk pressure separately on disposable QA. | Truthful unavailable/failure states, bounded recovery and no silent data corruption, leaked resources or unrelated cleanup. Record exact pressure and safety stop. | BLOCKED D5 |
| REL-06 | Interrupt persistence at accepted-operation and terminal-commit boundaries, including storage full/unavailable. | Acknowledged durable state is not lost; unknown outcomes reconcile exactly; no partial success, duplicate effects or fabricated recovery evidence. | U |
| REL-07 | Repeat create/use/remove and failed-operation cleanup cycles. | Owned runtime, network, Volume, runner and temporary credential inventory returns to the declared baseline; no accumulating leaks; failed evidence retained. | U |
| REL-08 | Introduce drift in an owned resource beside an unmanaged lookalike. | Reconciliation touches only exact GP-owned authority; unmanaged resources remain byte/identity unchanged; drift and recovery are visible. | U |
| REL-09 | Exercise concurrent desired edits and disjoint operations during a runtime failure. | Source revisions, claims and locks prevent lost updates; permitted unrelated work progresses; blocked operations have an accurate reason and no false completion. | U |

## Decisions and blocked coverage

| ID | Gap | Qualification consequence |
| --- | --- | --- |
| D1 | Latest-wins is accepted but not connected to production publication/execution. | Record its required cases; do not implement or enable it as an incidental QA task. |
| D2 | End-to-end Backup/Restore is incomplete; Valkey's safe recovery source remains undecided. | No recovery-readiness pass. Preserve the required rejection case separately from unsupported successful capture/Restore. User decides any implementation or external recovery plan. |
| D3 | The restart verifier expects unchanged Agent start time, but normal Controller shutdown stops owned Agent/etcd containers. | Resolve the requirement/verifier disagreement with the user. Neither relax the assertion nor change lifecycle behavior automatically. |
| D4 | Failed application-rollout recovery after runtime configuration changes remains unresolved. | Do not resume dependent interrupted-Task/reboot faults until baseline and required recovery are qualified and current permission allows them. |
| D5 | Production load, duration, latency, acceptable recovery time and pressure safety bounds are not yet agreed for a full reliability run. | Obtain these inputs before calling load/soak/recovery qualification complete. Existing 600-second evidence is bounded, not a production capacity guarantee. |

The MVP assumes a trusted operator organization and explicitly does not promise
strict peer isolation on shared backing bridges, multi-host HA, database migration
reversal or zero downtime for recreate. Test and report these boundaries; do not
invent guarantees or silently remove relevant limitations from the report.

## Running and maintaining the matrix

1. Select the exact clean candidate, scope and case IDs with the operator. Keep
   full-product coverage visible even when a particular run selects a subset.
2. Expand variants and record every selected case before execution, initially
   NOT RUN. Link the exact automated test or write bounded manual steps, fixture,
   independent observations and cleanup. A broad suite name is not a case mapping.
3. Reuse reviewed evidence only where code, dependency, configuration and topology
   changes do not invalidate its assertions. Record that decision, never silently
   carry a green cell to another release.
4. Run normal paths, then applicable boundaries and operation sequences, then
   authorized faults. Stop on failures that invalidate the next case's baseline.
5. Keep per-case outcomes, raw evidence and cleanup receipts. Report PASS, FAIL,
   BLOCKED, NOT RUN and justified NOT APPLICABLE separately; PARTIAL is not PASS.
6. Summarize by area and named cases, including missing automation/evidence. Never
   derive readiness from line coverage, a build, a Task count or a percentage that
   excludes failed/unrun requirements. The operator decides follow-up work.

Existing reusable entrypoints are in the
[verification feature map](../.agents/skills/verify-groundplane/features/README.md):
Host health supports HOST-01; foundation host supports portions of HOST/REL;
real-etcd Zone/Route checks support NET-02 persistence, not live networking;
Volume lifecycle supports portions of VOL; Attach L2 supports portions of ATT.
Inspect each guide's actual assertions before mapping it to a whole case. Missing
automation is a visible gap, not permission to create another harness or skip QA.
