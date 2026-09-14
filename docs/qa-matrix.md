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
| TASK-06 | Disconnect/resume event stream at boundaries, during compaction and terminal drain. | Immutable sequences have no loss or duplicate effects; terminal state is consistent; context cancellation releases subscriptions. | U; D6 repaired locally |
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
| CMP-04 | Reject invalid grants/catalog/source revision, mutate caller-owned catalog inputs, or race candidate publication. | Catalog authority remains an immutable snapshot; no partial Component graph or address publication; current active state remains authoritative until successful terminal commit. | U; local copy regression below |
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
| D6 | The Task stream discarded queued compaction errors when a closed event channel won selection. | Owner-approved repair passes the local regressions below. Both watches now consume terminal errors after event-channel closure. The local failure is closed; real etcd/SSE and full TASK-06 qualification remain unverified. Nothing was deployed. |

The MVP assumes a trusted operator organization and explicitly does not promise
strict peer isolation on shared backing bridges, multi-host HA, database migration
reversal or zero downtime for recreate. Test and report these boundaries; do not
invent guarantees or silently remove relevant limitations from the report.

## Existing test review

Each existing test must answer: which requirement or real failure does it guard,
what independent expectation does it assert, and would it fail if that behavior
broke? Map it to the relevant case and state its proof level and limitations.
A short pure-function test may guard an important boundary; a long test with
many assertions may only confirm its own fixture. Length and coverage percentage
are not review criteria.

Retain tests with a concrete reason; add missing rationale and case links. Remove
confirmed tautologies, duplicate tests with no distinct failure mode, tests of
unused/superseded behavior, and checks that exist only to raise coverage. Do not
delete a useful regression merely because its author omitted a comment. A mock's
configured return value is not evidence of the external effect being claimed.

Source-text checks may enforce an actual static delivery constraint, but finding
a callback name or UI string does not prove action wiring, validation, persistence
or rendered behavior. Such checks cannot stand in for behavioral tests. Deleting
one does not qualify its feature: keep the missing interaction/runtime proof
visible until a corresponding test is written and executed. Do not add a new
framework or change product behavior as an incidental test cleanup.

### Reviewed dispositions

This is a partial audit, not completion of the repository-wide review. Git retains
removed files. Update this table by coherent test area, not by creating another
document for each test cleanup. Passing a retained local check still does not
qualify its full product case.

| Reviewed tests | Disposition and reason | Matrix coverage |
| --- | --- | --- |
| Three tests in `console/src/lib/console-parity-contract.test.mjs` | Removed. Source regexes checked old field text, helper names and Task-dialog strings without invoking a Service action, dispatching a Task or rendering no-op feedback. They did not prove their claimed behavior. | UI-01/03/05 and CMP-02 retain unverified interaction coverage. |
| One test in `console/src/lib/environment-blueprint-script-contract.test.mjs` | Removed. Presence of JSX and get/validate/apply names can pass in dead code or comments; no Blueprint is read, validated, applied or rendered. | BP-01/02/04 and UI-01 remain independently unqualified. |
| Two tests in [controller-update-contract.test.mjs](../console/src/lib/controller-update-contract.test.mjs) | Retained and linked. Execute the actual request-state owner through lost acceptance, reload, exact replay, definitive rejection and blocked storage; assert protected identity and absence of duplicate activation intent. | UP-11, UI-04: local behavior only. |
| [Blueprint step budget](../internal/controller/taskcontract/environment_test.go) | Retained with rationale. Independently expected count includes each candidate's recovery pair, guarding under-budgeted plans. | BP-11: arithmetic only, not maximum-size live Apply. |
| [Never-applied Entry selection](../internal/controller/entry/removal_projection_test.go) | Retained with rationale. Prevents absence from being treated as authority for host deletion. | ENT-11: local selection only. |
| [Backing request admission](../internal/controller/backing_service_route_policy_test.go) | Retained with rationale. Valid JSON must reach its owning handler instead of being rejected by an unrelated body policy. | BACK-01, UI-01: dispatch policy only, not provisioning. |
| [Disabled Backup authoring](../internal/controller/environment_blueprint_backup_validation_test.go) | Retained with rationale. Incomplete execution must not make a valid disabled desired declaration impossible to import. | BP-01, BAK-01: local validation only. |

First-batch verification: `.tmp/qa-test-review-retained-go.log` records the four
retained Go checks with `-race`; `.tmp/qa-test-review-console-assertions.log`
records 75 individual assertions with pinned Node 24.19.0 and in-process test
execution. The default runner also returned success, but its log summarizes file
wrappers rather than named assertions. These are local cleanup results, not
row-wide product passes or an endorsement of unreviewed tests.

#### Console review

All committed Console test files were reviewed, including the compile-time
generated-error assertion. This second batch removed 37 source/document-text
tests and one test of an unused parser. It retained 35 behavioral tests and two
Node-run static checks, plus the compile-time assertion. Every retained behavioral
test names its cases, reason and proof limit next to its assertions.

| Reviewed tests | Disposition and reason | Matrix coverage or remaining gap |
| --- | --- | --- |
| Four tests in `c07-network-contract.test.mjs` | Removed. Callback names, alert strings and a command-tree sentence do not execute Zone/Route reads, path rejection, Component actions or CLI impact. | NET-04, CMP-01, HTTP-02, UI-01/03 remain unqualified by these checks. |
| Four tests in `c14-console-contract.test.mjs` | Removed. Source patterns do not enforce Secret scope, execute removal, observe plaintext lifetime or validate Task navigation. | SEC-01/02/04/06, TASK-01, UI-05 need actual request/state or interaction proof. |
| Four tests in `c20-release-group-contract.test.mjs` | Removed. Matching callbacks, refresh names, input labels and preview markup cannot detect broken dispatch or stale rendered state. The independent preview-state tests below remain. | GRP-01/02/04, UI-01/05 lack interaction proof. |
| Five tests in `environment-c04-final-contract.test.mjs` | Removed. No deletion fence, accepted retry generation, hydration race, failed create or observer cancellation is executed. | OWN-01/04/06, UI-04/05 remain unqualified by these checks. |
| Five tests in `environment-c04-recovery-contract.test.mjs` | Removed. Source presence cannot prove stale-load suppression, missing-Task handling, definite-rejection cleanup, uncertain DELETE replay or startup recovery. | OWN-06, UI-04/05 require state-sequence tests. |
| One test each in `environment-deletion-contract.test.mjs` and `environment-lifecycle-contract.test.mjs` | Removed. Broad source matching repeats lifecycle claims without a create, rename, deletion or protected request. | OWN-01/02/05/06 and UI-04/05 remain open. |
| One test each in `environment-projection-contract.test.mjs`, `environment-release-live-state-contract.test.mjs` and `platform-component-live-state-contract.test.mjs` | Removed. Text in projection, store and pages does not prove failed health, serving summaries, Component configuration or DNS read/edit behavior. | OBS-02, SVC-01, CMP-01/02, DNS-02 and UI-05 remain open. |
| Two export-lifecycle tests in [backup-key-export-contract.test.mjs](../console/src/lib/backup-key-export-contract.test.mjs) | Removed. Any matching no-store/finally/abort text could satisfy them without executing key export, revocation of the download URL or unmount cleanup. | BAK-14 and UI-05 require browser lifecycle/secret-safety proof. |
| Three source/document tests in [backup-policy-contract.test.mjs](../console/src/lib/backup-policy-contract.test.mjs) | Removed. Cursor-query text, JSX nesting and a status paragraph do not exercise pagination or prove implementation. | BAK-07 needs actual empty-page continuation and request checks; status prose is not a product test. |
| Two behavioral tests in that Backup Policy file | Retained. Validate the exact public numeric interval and reject unsafe response integers. Expectations now use the contract's numeric bound, not the implementation's exported constant. | BAK-01, UI-03/05: local validation only, not retention or persistence. |
| Two authentication projection/presentation tests and the source assertions in [valkey-authentication-contract.test.mjs](../console/src/lib/valkey-authentication-contract.test.mjs) | Removed. Type/fixture strings and JSX cannot prove selected modes, form reset or mode-specific Attach facts. Retained the actual request-field test: reject empty/invalid Valkey choice, preserve all three explicit modes, omit authentication for PostgreSQL. | BACK-05, UI-03: local request fields only. BACK-06 and form/no-preselection interactions still need direct proof. |
| One wiring test each in [caddy-template-contract.test.mjs](../console/src/lib/caddy-template-contract.test.mjs), [route-summary.test.mjs](../console/src/lib/route-summary.test.mjs) and [service-observation-contract.test.mjs](../console/src/lib/service-observation-contract.test.mjs) | Removed. A component/helper name or loop string does not prove editing, rendering or serial visible refresh. | HTTP-03, HTTP-01 and OBS-03 interaction/refresh coverage remains open. |
| `Agent labels preserve keyed values and reject ambiguous input` in `agent-read-model.test.mjs` | Removed. It only invokes `parseAgentLabels`, which has no production caller; the current Agent editor uses the configuration surface. The production helper was not changed. | HOST-07 needs the actual settings path, not an unused parser. |
| Two retained Caddyfile tests | Preserve BOM/native placeholders and exact text; reject byte-size, NUL and UTF-8 violations, including dishonest reported file size. | HTTP-03/04, UI-03: input handling only, not rendering/reload. |
| Four tests in [component-zone-picker.test.mjs](../console/src/lib/component-zone-picker.test.mjs) | Retained. Preserve multi-selection, select the exact returned ID, leave no phantom selection on rejection, and exclude backing-owned options without guessing names/internal state. | NET-02, CMP-01/03, HTTP-07, UI-03: local callbacks and selection only. |
| One test in [entry-api.test.mjs](../console/src/lib/entry-api.test.mjs) | Retained. Keep reconciliation identity, zero uid/gid and Secret reference/exposure metadata during decoding. | ENT-04, SCRIPT-01: response projection only, not materialization or grant enforcement. |
| One test in [environment-authoritative.test.mjs](../console/src/lib/environment-authoritative.test.mjs) | Retained. Returned scalars replace stale ones without erasing independently hydrated children. Its name no longer claims unasserted fields. | OWN-02, NET-02, UI-01: local merge only, not stable-ID validation or a real edit. |
| Three tests in [release-group-rollback-preview-authority.test.mjs](../console/src/lib/release-group-rollback-preview-authority.test.mjs) | Retained. Exact immutable scope/revision, rejection before caller dispatch, and late success/failure ownership after edit/reopen each guard a distinct stale-preview failure. | GRP-04, UI-05: local authority helpers, not dialog wiring or Controller rollback. |
| Two retained Route-summary tests | Distinguish no Routes, public/internal/mixed exposure and all non-applied states regardless of list order. | HTTP-01, UI-05: summary computation only, not provider reachability. |
| Three tests in [script-execution.test.mjs](../console/src/lib/script-execution.test.mjs) and two in [script-order.test.mjs](../console/src/lib/script-order.test.mjs) | Retained. Preserve complete explicit/inherited contexts, reject malformed grants, distinguish replacement from omission, preserve bounded order and reject ambiguous authoring. | SCRIPT-01/03/05/06, UI-03/05: request/response validation only, not actual hook order or isolation. |
| Four retained observation tests | Reject inconsistent snapshots; expire exactly at the local boundary; never turn incomplete evidence into healthy aggregate state; refresh only selected evidence without replacing desired fields. | OBS-01/02/03, UI-05: local decoding/merge/expiry, not actual observations, refresh serialization or generation races. |
| Four tests in [task-detail-actions.test.mjs](../console/src/lib/task-detail-actions.test.mjs) | Retained. Exclude native-update Retry and internal-prune controls while preserving ordinary Backup Retry and lifecycle-specific controls. | TASK-03/05, BAK-05/08, UP-07: visibility decisions only, not effect cancellation or recovery. |
| Two tests in [task-read-model.test.mjs](../console/src/lib/task-read-model.test.mjs) | Retained. Reject unknown Task types and operator-owned internal pruning before display. | TASK-01, BAK-08, UI-05: local decoding only, not server provenance enforcement. |
| Two tests in [transient-logs.test.mjs](../console/src/lib/transient-logs.test.mjs) | Retained. Real stream decoder delivers known lines across keepalive comments and valid CRLF framing. | LOG-01/02: finite local stream framing, not actual source selection, cancellation or reconnect. |
| Two Controller update-intent tests | Already reviewed above; unchanged. | UP-11, UI-04: local protected-request state only. |
| Remaining Backup-key rotation test; [Environment edit schema test](../console/src/lib/environment-edit-contract.test.mjs); [generated error tuple](../console/src/lib/api.generated.contract.test.ts) | Retained as delivery checks. Enforce generated wire-type ownership, parse the emitted closed edit schema and protected header, and reject extra generated RFC 7807 keys. Each has a concrete static constraint, not a behavior claim. | No product case passes from these checks. |

Verification: `.tmp/qa-test-review-console-complete.log` records all 37 named Node
tests passing with pinned Node 24.19.0 and `--test-isolation=none` (35 behavioral,
two static). `.tmp/qa-test-review-console-types.log` records the TypeScript check,
including the generated-error assertion. No runner configuration, dependency,
production behavior or deployed state changed. Deleted source remains in Git.

No committed Console browser/interaction suite was found. The retained tests do
not establish action wiring, keyboard/mobile behavior, secret-download cleanup,
or Environment deletion/reload recovery. Earlier isolated browser evidence stays
bounded to its recorded build and scenarios; it does not fill these whole-case
gaps. Adding the missing interaction suite or changing production behavior is not
part of this existing-test cleanup.

#### CLI inputs and scoped resolution

Reviewed all 19 tests in the nine files below; retained all 19. Each calls the
real CLI parser, request mapper or command against independent expected values
or a local HTTP recorder. No production code changed.

| Reviewed tests | Retention reason | Matrix coverage |
| --- | --- | --- |
| Three in [backingservice_test.go](../internal/cli/backingservice_test.go) | Missing/empty Valkey mode rejects before HTTP; explicit password mode reaches the exact request; show preserves the returned mode. The rejection test now detects any unexpected request. | BACK-05, UI-01/03: CLI admission/transport/display only, not protocol authentication. |
| Three in [entry_test.go](../internal/cli/entry_test.go) | Follow opaque pagination; require both numeric ownership flags while accepting zero; encode the API's flat fact source. Missing either UID or GID is now independently asserted. | ENT-02/04, UI-01/03: helper/request behavior only. |
| Two in [entry_create_test.go](../internal/cli/entry_create_test.go) | Preserve exact owner/grant/fact operands; reject Entry input above its 256 KiB ceiling. The grant assertion now rejects the wrong nonempty ID, not just absence. | ATT-06, ENT-01/02, UI-01/03: no grant authorization or materialization proof. |
| One in [entry_edit_test.go](../internal/cli/entry_edit_test.go) | Preserve complete mutable PATCH state and bounded stdin transport without printing the secret response value. | ENT-01/03/06, UI-01: CLI-to-HTTP only, not encryption or runtime exposure. |
| One in [secret_target_test.go](../internal/cli/secret_target_test.go) | Resolve the Project-scoped mutable key, then address detail by stable Secret ID. | SEC-03, UI-02: lookup routing only, not Controller authorization. |
| Two in [secret_create_test.go](../internal/cli/secret_create_test.go) | Send one protected Platform create from stdin with redacted output; reject input above the distinct 255 KiB Secret ceiling. | SEC-04/05, UI-01/03: local input/output only, not durable encryption or reveal. |
| One in [script_order_test.go](../internal/cli/script_order_test.go) | Preserve maximum create order and explicit zero edit through generated request conversion. | SCRIPT-01, UI-01/03: encoding only, not actual hook order. |
| Four in [script_execution_test.go](../internal/cli/script_execution_test.go) | Resolve scoped Volume/Entry names across pages; encode inherited reset and reject conflicting flags; bypass lookup in ID mode; reject absent or ambiguous Entry keys before mutation. | SCRIPT-01/06, UI-02/03: local lookup and requests, not runner isolation. |
| Two in [script_execution_file_test.go](../internal/cli/script_execution_file_test.go) | Preserve complete typed YAML with explicit false/inherited mode; reject null/coerced/duplicate/unknown/multi-document or oversized decisions. | SCRIPT-01/06, UI-03: file decoding only, not persistence or execution. |

All 19 tests pass with `-race -count=1`; evidence is
`.tmp/qa-test-review-cli-20260913.log`. The successful run allowed local loopback
listeners after the sandbox-only attempt could not create them; no QA host was
contacted. These results do not qualify any entire product row.

#### CLI lifecycle, observation and Blueprint transport

Reviewed all 48 tests in this second CLI batch; retained 46 behavioral tests and
two separate command-inventory checks. Each has a reason and proof limit. The
two CLI batches together cover 67 tests, not the entire CLI suite.

| Reviewed tests | Retention reason and corrections | Matrix coverage or gap |
| --- | --- | --- |
| Three in [blueprint_bundle_test.go](../internal/cli/blueprint_bundle_test.go) | Preserve canonical multipart namespace and authored Compose order, reject symlinks, and send the revision-fenced protected Apply. Manifest checks now require independently expected part names, sizes and SHA-256 values. | BP-01/03/07, UI-01/03: local input/HTTP sequencing, not Apply effects or revision races. |
| Seven in [releasegroup_test.go](../internal/cli/releasegroup_test.go) | Preserve create policy, reject empty edit/invalid enum, encode policy changes, distinguish omitted/explicit rollback tag, send exact preview tag and reject blank tag. Renamed the empty-edit test to match its actual assertion. | GRP-01/04, UI-01/03: local requests. Preserving an omitted policy during an unrelated edit remains unproved; the former test name did not supply that proof. |
| Four in [task_journal_test.go](../internal/cli/task_journal_test.go) | Resolve Environment/workspace scopes, preserve both nouns' cursor/limit, bypass lookup for IDs and reject conflicting scopes. | TASK-01, UI-02/03: local query semantics, not durable journal ownership or concurrent pages. |
| One in [controller_config_command_test.go](../internal/cli/controller_config_command_test.go) | Read the current revision and send the operator's exact file bytes in a protected replacement. | HOST-08/09, UI-01: CLI request only, not disk permissions or effective config after restart. |
| Two in [controller_update_test.go](../internal/cli/controller_update_test.go) | Send a protected digest-only update; reject absent/path/URL/tag/noncanonical digest input. Invalid explicit values now require the correct validation kind and no HTTP request. | UP-01/03, UI-01/03: local dispatch/refusal, not activation, recovery or continuity. |
| Four in [service_observation_test.go](../internal/cli/service_observation_test.go) | Present list/show evidence, expire machine-readable snapshots, and keep read-only observations out of mutation fields. Distinct count values are now checked against their actual rendered columns/keys; desired replicas remain distinct from expected replicas. | OBS-01/02/03, SVC-01, UI-01: local rendering, not live container counts or refresh races. |
| Two in [environment_target_test.go](../internal/cli/environment_target_test.go) and two in [hierarchy_delete_test.go](../internal/cli/hierarchy_delete_test.go) | Resolve colliding scoped labels before stable-ID rename/pool edit or Tenant/Project deletion dispatch. | OWN-02/05, NET-02, UI-02: routing only, not persisted labels, descendant preservation or deletion completion. |
| Four in [network_parity_test.go](../internal/cli/network_parity_test.go) | Keep the closed command inventory; encode Zone create; obtain impact before delete; use the canonical Route mutation endpoints. | Three local behavioral tests support NET-01/04, HTTP-01, UI-01. One inventory is delivery-only, not product parity. |
| Nineteen in [parity_test.go](../internal/cli/parity_test.go) | Guard router reads, Agent lifecycle/config, rename/pool edits, stable Component IDs and Caddy replacement, Task streams/Retry, Attach targets, bodyless Backup and raw key export. Renamed the rename-routing test to stop claiming unasserted response values. Retain the Release Group command inventory separately. | Eighteen local HTTP/CLI tests support CMP-01/02, HOST-04/07, UP-02, OWN-02, NET-02, HTTP-03, TASK-05/06, ATT-01/03, BAK-05/14, UI-01/02/03. One delivery-only inventory. No real effects or cross-surface parity. |

All 48 pass with the race detector in `.tmp/qa-test-review-cli-second.log`.
After strengthening numeric output checks, the two affected observation tests
pass again in `.tmp/qa-test-review-cli-observation-values.log`. Unchanged proof
is reused. Environment pool-edit output values and Task-stream reconnect,
compaction, cancellation and terminal drain remain unproved by this batch.

#### Remaining CLI commands and presentation

Reviewed the remaining 59 top-level CLI/common tests in 21 files. Retained
45 behavioral tests and 14 delivery/tooling checks; none lacked a distinct
requirement or failure-prevention reason. Together with the two earlier CLI
batches and the API-client audit below, all 64 committed CLI test files are
reviewed: 186 original tests, one duplicate removed, 185 retained. This is test
review completion for the CLI, not completion of its product QA cases.

| Reviewed tests | Retention reason | Matrix coverage or gap |
| --- | --- | --- |
| One in [tenant_description_test.go](../internal/cli/tenant_description_test.go), one in [tenant_target_test.go](../internal/cli/tenant_target_test.go), two in [project_target_test.go](../internal/cli/project_target_test.go) | Preserve authored descriptions, resolve mutable labels to scoped stable IDs, and omit the response-only Project kind from create. | OWN-01/02/03, UI-01/02/03: local requests, not persistence, rename races or descendant safety. |
| One each in [connector_create_test.go](../internal/cli/connector_create_test.go), [connector_remove_test.go](../internal/cli/connector_remove_test.go), [connector_surface_test.go](../internal/cli/connector_surface_test.go) | Preserve explicit addressing and distinct credential sources; resolve names or bypass lookup in ID mode; retain the closed operation inventory as delivery-only. | Two behavioral tests support CON-01/03/04/06 and UI-01/02/03 locally. No provider access, secret storage or deletion finalization. |
| Three in [backup_key_export_test.go](../internal/cli/backup_key_export_test.go) | Write exact private bytes with mode 0600; reject a symlink without altering its target; refuse structured output before raw export. | BAK-14, UI-03: actual temporary-file safety and local admission, not HTTP cache headers or remote recovery. |
| Eleven in [backup_policy_test.go](../internal/cli/backup_policy_test.go), two in [backup_points_test.go](../internal/cli/backup_points_test.go), two in [volume_list_test.go](../internal/cli/volume_list_test.go) | Preserve source order/stable references and configured versus absent fields on disable; reject invalid source cardinality and decimal/integer boundaries; resolve scoped Environment labels or bypass lookup with stable IDs. | BAK-01/02/07, VOL-01, UI-01/02/03: parser/lookup/request proof, not atomic persistence, point verification or data preservation. |
| Ten in [component_zone_test.go](../internal/cli/component_zone_test.go), two in [component_config_file_test.go](../internal/cli/component_config_file_test.go), one in [component_config_test.go](../internal/cli/component_config_test.go) | Keep ordered Zone placement, Tunnel credentials, explicit first-enable inputs and non-normalizing CoreDNS parsing. Partial-failure/validation fixtures now require exact HTTP methods, paths and bodies, so unrelated calls cannot consume a success response. The removed flag inventory is delivery-only. | Twelve behavioral tests support CMP-01/04/05, NET-02, HTTP-07/08, DNS-02, UI-01/02/03. Created-Zone retention is CLI reporting only, not Controller persistence or actual ingress. |
| Four in [controllerctl_test.go](../internal/cli/controllerctl_test.go), seven in [root_test.go](../internal/cli/root_test.go), two in [grammar_test.go](../internal/cli/grammar_test.go), one in [route_flags_test.go](../internal/cli/route_flags_test.go) | Preserve same-host/tool exemptions, safe diagnostic output, injected runner errors, fail-closed unavailable local commands, closed completions/flags and exact operands. Two are behavioral API admission tests; twelve are local tooling/static delivery checks. | UI-03: local admission only. Injected diagnostics are not live etcd/key/Controller observations; command inventory is not 1:1 product parity. |
| One in [resource_test.go](../internal/cli/resource_test.go), two in [common/errors_test.go](../internal/cli/common/errors_test.go), one in [common/recovery_points_test.go](../internal/cli/common/recovery_points_test.go), three in [common/service_observation_test.go](../internal/cli/common/service_observation_test.go) | Preserve Task follow-up output, canonical/secret-safe errors, exact Recovery Point columns and honest observation freshness/empty arrays. Recovery Point checks now compare every header/value. Observation checks compare an independent full projection; distinct counts 1..7 detect swapped fields instead of seven identical values masking them. | TASK-01, SEC-05, BAK-07, OBS-01/02/03, UI-01/03: presentation only, not API publication, live observation or process exit behavior. |

The focused 59-test race run passes in `.tmp/qa-test-review-cli-remaining.log`.
The final distinct-count correction passes separately in
`.tmp/qa-test-review-cli-common-values.log`; unchanged proof is reused. Formatting
passes. No production code, dependency or live state changed.

#### CLI API-client boundary

Reviewed all 60 tests in the 24 API-client test files. Retained 57 behavioral
checks and two static delivery checks; removed one duplicate. These execute
local conversions, generated transport or loopback HTTP, not a Controller or
Agent. Every retained test names its reason and limited matrix contribution.

| Reviewed tests | Disposition and reason | Matrix coverage or gap |
| --- | --- | --- |
| Ten in [client_test.go](../internal/cli/apiclient/client_test.go) | Retained. Normalize requests, preserve one intent key across retry, reject invalid keys/statuses/JSON and negotiate SSE. Invalid-key tests previously also supplied an invalid expected status, allowing an unrelated rejection to pass. They now use valid mutation statuses, assert zero HTTP and independently check safe-method key rejection. Valid JSON must preserve its value; generated keys are checked as ULIDs, not against the client's own regex. | UI-01/03/04/05: local admission/transport, not durable replay or cross-surface parity. |
| Three in [task_events_test.go](../internal/cli/apiclient/task_events_test.go) and two in [logs_test.go](../internal/cli/apiclient/logs_test.go) | Retained. Resume an interrupted Task stream with its last sequence; deduplicate replay, reject gaps, and preserve exact LogEvent fields while rejecting malformed frames. Incomplete Task frames now must leave the cursor unchanged and invoke no callback. Reconnect proof has a five-second deadline. | TASK-06, LOG-01/02: local stream behavior. Real compaction, cancellation cleanup, source selection and durable terminal drain remain unproved. |
| Two in [problem_test.go](../internal/cli/apiclient/problem_test.go), one in [problem_envelope_test.go](../internal/cli/apiclient/problem_envelope_test.go), ten in [generated_response_boundary_integration_test.go](../internal/cli/apiclient/generated_response_boundary_integration_test.go) | Retained. Reconstruct trusted error kinds; reject malformed/mismatched envelopes; enforce the existing 1 MiB error-body budget for declared, unknown, fragmented and actual chunked responses; close bodies once on success/error. Expected sizes no longer come from the implementation constant, and exact-bound content is compared byte-for-byte. Renamed the synthetic fragmented-reader test so it does not claim actual HTTP chunking; the separate socket test provides that proof. | UI-03/05: client trust/resource boundary only. A no-byte read failure proves cause/closure, not partial-body redaction. |
| Two in [problem_generated_contract_test.go](../internal/cli/apiclient/problem_generated_contract_test.go) | Retained as delivery checks: exact generated Error fields/types, and close-before-bounded-read structure across every generated parser. The latter enforces generator output structure, not arbitrary UI source presence. | Static generation constraints; neither is a product-case pass. |
| Two in [raw_response_test.go](../internal/cli/apiclient/raw_response_test.go) and one in [backup_key_generated_test.go](../internal/cli/apiclient/backup_key_generated_test.go) | Retained. Reject exports above the independent 4 KiB ceiling with no returned partial value or intent key; clear scratch on partial read error; send protected rotation and preserve its accepted Task. | BAK-14: local secret transport, not key creation, persistence or browser cleanup. |
| Two in [backup_policy_test.go](../internal/cli/apiclient/backup_policy_test.go), one each in [backup_policy_generated_test.go](../internal/cli/apiclient/backup_policy_generated_test.go), [backup_policy_projection_test.go](../internal/cli/apiclient/backup_policy_projection_test.go) and [backup_points_test.go](../internal/cli/apiclient/backup_points_test.go) | Retained. Preserve policy inputs/source identity, nullable next-run time, Volume scope/cursors and exact Recovery Point metadata. The generated-operation recorder now fails on unexpected routes and requires the explicit empty Volume array. A single-source fixture does not prove source ordering. | BAK-01/04/07, VOL-01: transport/projection only, not scheduling, capture or remote verification. |
| Four in [volume_test.go](../internal/cli/apiclient/volume_test.go) | Retained three: protected lifecycle request shapes, optional create key, and deletion-impact revision/token projection. Removed `TestGeneratedVolumeEditBodyMapsSlug`: the lifecycle test executes that same mapper and checks the same slug at the HTTP boundary. Its former assertion did not enforce its claimed prohibition on untyped intermediates. | VOL-01/04/05: no filesystem effects, complete impact pagination or stale-token rejection proof. |
| Four in [task_test.go](../internal/cli/apiclient/task_test.go) and three in [task_projection_test.go](../internal/cli/apiclient/task_projection_test.go) | Retained. Preserve journal scope/ownership/step identity, reject contradictory steps and unknown Task types, and propagate item failure through the page. Journal timestamps now require exact instants rather than merely nonzero values. | TASK-01, BAK-08, UI-03/05: local models, not durable journal or pruning. |
| Two in [script_execution_test.go](../internal/cli/apiclient/script_execution_test.go) | Retained. Preserve complete execution grants in both directions and reject malformed execution. Assertions now compare every grant field, explicit false, user and Entry identity, and require exact create/edit routes. | SCRIPT-01/06: transport only, not actual mounts, credentials or runner isolation. |
| Three in [component_config_response_test.go](../internal/cli/apiclient/component_config_response_test.go), one in [component_enable_test.go](../internal/cli/apiclient/component_enable_test.go) | Retained. Preserve nullable config/empty managed-file arrays, decode valid CoreDNS through the strict public model, reject incomplete variants and distinguish absent enable body from typed config. | CMP-01/02, DNS-02: small local responses; large config responses and live activation remain unproved. |
| One in [connector_test.go](../internal/cli/apiclient/connector_test.go) | Retained. Preserve exact scoped CRUD, explicit false, direct credential input, response metadata and accepted deletion Task. All returned Connector fields are now compared. The fixture is already redacted; this cannot prove server-side redaction. | CON-01/03/04: no provider access, secret resolution or deletion cleanup. |
| One each in [environment_delete_test.go](../internal/cli/apiclient/environment_delete_test.go), [controller_config_test.go](../internal/cli/apiclient/controller_config_test.go), [host_test.go](../internal/cli/apiclient/host_test.go), [host_update_test.go](../internal/cli/apiclient/host_update_test.go) | Retained. Preserve protected Environment deletion identity, exact revision-fenced YAML replacement, nested Host fields and optional update/recovery metadata. | OWN-05/06, HOST-01/08/09, UP-01/05: request/read-model proof only, not deletion, file changes, health or upgrades. |

All 59 retained tests pass with `-race -count=1` in
`.tmp/qa-test-review-apiclient.log`; the three final assertion corrections pass
in `.tmp/qa-test-review-apiclient-final.log`. Local loopback permission was needed
after the sandbox refused the first listener. No production code, generated
artifact, dependency or deployed state changed; whole-case qualification is
unchanged. The removed test remains recoverable from Git.

#### Deployment and repository helper tests

Reviewed all 73 tests in the 11 root-level `scripts/test_*.py` files; retained
all 73. These are not tests that merely ask whether a binary builds. They guard
deployment authority, file safety, protected-request handling, resource limits
or the reliability of the verification tooling. Each test now states its reason
and either a partial case mapping or its separate delivery constraint.

| Reviewed tests | Retention reason and assertion corrections | Proof limit |
| --- | --- | --- |
| Six in [test_controller_update.py](../scripts/test_controller_update.py) | Lost POST replays the same key; known Task resumes with GET only; another release cannot replace uncertainty; failed work stays failed without Abort; timeout retains intent; symlink receipt and wrong Task are rejected. | UP-07/11, UI-04: real local client/receipt with recorded transport, not activation or process-crash recovery. |
| Four in [test_controller_release.py](../scripts/test_controller_release.py) | Exact immutable bytes/modes and selector on replay; changed binary retains previous candidate; symlink/writable input refusal; invalid compatibility/image and corrupted published-manifest refusal. The last test now restores valid state between faults and checks distinct rejection reasons so corruption cannot mask missing validation. | UP-01/03: real local filesystem staging only; no candidate is executed. |
| Five in [test_controller_bootstrap.py](../scripts/test_controller_bootstrap.py) | Partial/duplicate and symlink initialization refusal; remove only an empty owned layout; retain release/journal evidence; require terminal Tasks across every page. Added an active Task on the later page, not just a two-page success. | HOST-02, UP-03/04/09: local layout and preflight only, not installation or durable recovery. |
| Five in [test_deploy_native.py](../scripts/test_deploy_native.py) | Exact installed Controller/guard bytes and mode; native Task branch never falls into SSH overwrite; stage-only never activates; failed/unknown work retains its bundle without SSH rollback; bootstrap flag cannot bypass native ownership. | HOST-02, UP-01/03/07/11: extracted real shell branch with disposable paths and fake external helpers. |
| Nineteen in [test_deploy.py](../scripts/test_deploy.py) | One SSH argument test and seven staging tests guard trust, path collision, symlink ancestors and owned cleanup. Seven bootstrap rollback tests guard invalid predecessor paths, atomic replacement of an executing inode, exact restoration, original errors and retained recovery evidence. Four Agent preflight tests guard busy-to-idle, bounded busy refusal, malformed output and read failure. | Eight delivery checks; seven HOST-02 bootstrap checks are **not native update recovery**; four UP-04 checks use recorded CLI replies and remove sleeps, so they do not prove elapsed timing or atomic admission. |
| Five in [test_deploy_capacity.py](../scripts/test_deploy_capacity.py) | Inclusive selected threshold; measurement failure; capacity recheck before an uncached build; refusal before build commands or target access. | Delivery sequencing with injected measurements, not stress/load or free-space reservation. |
| Six in [test_deploy_images.py](../scripts/test_deploy_images.py) | Bound build concurrency without changing parent/runtime settings; render expected command errors; select Runner only for bootstrap; reject unknown target mode; avoid Runner build/cache commands during native update. | Delivery commands and one static Dockerfile stage constraint, not image execution or runtime continuity. |
| Five in [test_image_reuse.py](../scripts/test_image_reuse.py) | Send only absent content; bind reused alias to exact ID; reject wrong identity, SSH failure and untrusted output; spawn no archive for an empty transfer set. | Delivery command decisions with injected inspection; no real Docker/SSH or registry proof. |
| Three in [test_image_transfer.py](../scripts/test_image_transfer.py) | Preserve paced bytes; terminate/wait owned children after receiver failure; reject invalid rate before I/O. The last assertion now fails on any read/write rather than merely expecting a value error. | Delivery stream/child lifecycle with fake clock and processes, not measured network or application continuity. |
| Two in [test_host_contract.py](../scripts/test_host_contract.py) | Execute the verifier predicate on valid nullable/history state and independently malformed metadata. These are positive/negative controls for the oracle. | Delivery/verifier correctness only; no Host was contacted or qualified. |
| Thirteen in [test_repo_env.py](../scripts/test_repo_env.py) | Local defaults/cache reuse; unsafe, symlink and diagnostic-output path rejection; recursive Make and simulated sudo environment; formatter source/failure handling; owned smoke scratch; verifier success/failure cleanup, outside-path refusal and failed-preflight receipts. | Delivery safety: Go, sudo and remote commands are doubles. Their fake passing binaries do not count as product behavior. |

The first complete local run is `.tmp/qa-test-review-deployment.log`: 59 pass,
14 setup/trust errors because the sandbox presents `/` with an untrusted owner.
The three affected files (15 tests, one overlapping pass) then pass with the real
filesystem ownership view in `.tmp/qa-test-review-deployment-trusted-paths.log`.
Together these runs exercise all 73 tests successfully. No trust check or directory
permission was weakened. Scratch and cleanup stayed in ignored repository-local
paths; no SSH connection, Docker workload, systemd operation or QA fault occurred.
Of these checks, 31 support only portions of named cases and 42 enforce separate
delivery/tooling constraints. None qualifies a complete product row.

#### Component SDK and registered planners

Reviewed all 35 tests in the ten SDK/registered-planner test files; retained all
35 with cases, rationale and local proof limits. Removed six redundant internal
checks, not whole tests: three Caddy rejection inputs already exercised through
the same `Plan` entrypoint in the comprehensive template table; a same-fixture
file-equality assertion; a repeated valid-image equality check already covered
by registration authority; and CoreDNS validation argv already checked by its
dedicated validation-port case.

| Reviewed tests | Retention reason and corrections | Matrix coverage |
| --- | --- | --- |
| Two in [action_test.go](../component-sdk/component/action_test.go) | Canonical action order/digest sensitivity and exact sealed envelope identity. Distinct digests now detect swapped fields; six invalid authority variants reject independently. | CMP-04: SDK values only, not publication or Agent execution. |
| Seven in [environment_plan_test.go](../component-sdk/component/environment_plan_test.go) | Bind Service and gateway behavior into replay identity; select authenticated platform; reject invalid OCI authority/origin/ordered Zones; detach immutable Route snapshots. | CMP-01/02/04, NET-02, HTTP-03/04/07/08: pure validation, selection, copying and digest behavior. |
| One in [managed_healthcheck_test.go](../component-sdk/component/managed_healthcheck_test.go) | Bound, copy and digest-bind readiness argv/timing rather than permitting mutable or invisible execution behavior. | CMP-04: local authority, not runtime readiness. |
| Two in [baseline_test.go](../component-sdk/dnsresolver/baseline_test.go) | Canonical immutable resolver/host inputs and rejection of one hostname with competing addresses. Now assert source identity, sort order and detachment after caller mutation. | DNS-01/02: input snapshots only, not DNS answers. |
| Three in [caddy_test.go](../registered-components/caddy/caddy_test.go) and four in [template_test.go](../registered-components/caddy/template_test.go) | Deterministic Route plans; no resources when disabled; Route/origin injection rejection; exact native policy substitution; invalid/unaccounted Route rejection; internal/colon references; aggregate and empty-set rendering. | CMP-01, HTTP-01/03/04/07: planner output only, not runtime activation or traffic. |
| Six in [catalog_test.go](../registered-components/catalog/catalog_test.go) | Scoped action lookup; copied validator authority; digest sensitivity; sole registered-image authority; Tunnel image binding without action recipes; closed DNS observation input. Strengthened lookup to reject an action present only in another implementation and mutated constructor/getter inputs to test ownership. | CMP-04, DNS-01, HTTP-08: local catalog authority. The DNS input-copy assertion exposed the defect described below. |
| One in [container_preflight_test.go](../registered-components/catalog/container_preflight_test.go) | Require, copy and digest-bind the stdin preflight recipe rather than validating only the later reload command. | CMP-04, HTTP-05: local recipe, not native Caddy execution. |
| Three in [cloudflaretunnel_test.go](../registered-components/cloudflaretunnel/cloudflaretunnel_test.go) | Preserve opaque Secret and explicit Zone/gateway decisions; reject incomplete identity and invalid/internal-only placement. | CMP-01/04, NET-03, HTTP-08: plan only, not credential resolution or provider ingress. |
| Six in [coredns_test.go](../registered-components/coredns/coredns_test.go) | Deterministic bytes/digest; conflicting-host rejection; overridable validation port; exact serving image/observation action; exact marker expansion; invalid-template rejection. Renamed the image/action test after removing its duplicated argv assertion. | CMP-01/04, DNS-01/02: renderer/catalog output only, not serving DNS or restoration. |

The initial race run in `.tmp/qa-test-review-components.log` passes every package
except the new DNS observation-recipe copy assertion. The constructor validated
an `OCIImage` but stored its caller-owned `Platforms` slice. Changing that slice
after construction changed the recipe's supposedly immutable image authority;
the old test compared shared state to itself and missed the alias. The other
recipe constructors and the image getter already copied this slice.

The owner explicitly approved the scoped fix. The constructor now uses the same
defensive copy, without a new interface or changed catalog identity for unchanged
inputs. The exact failing regression and all catalog tests pass with the race
detector in `.tmp/qa-test-review-catalog-copy-fixed.log`; unchanged passing SDK and
planner proof is reused. The two existing affected app runtime-wiring tests pass
in `.tmp/qa-test-review-catalog-wiring.log`; focused vet and pinned Staticcheck
pass in `.tmp/qa-test-review-catalog-vet.log` and
`.tmp/qa-test-review-catalog-staticcheck.log`. This is an L0 ownership fix, not an
observed QA incident or proof of live DNS, upgrade recovery or publication races.
No deployment occurred. CMP-04 remains unqualified as a whole.

#### Verifier-helper safety tests

Reviewed and retained all 20 checks in the three verifier-helper test files:
11 named Python tests and nine separately documented shell scenarios. Each has
a delivery-safety reason; none counts as a GP product-case pass. No actual SSH
connection, host fault or GP process was used.

| Reviewed tests | Retention reason and proof limits |
| --- | --- |
| Eleven in [test_ssh_tunnel_supervisor.py](../.agents/skills/verify-groundplane/scripts/test_ssh_tunnel_supervisor.py) | Missing child startup, pre-existing stop, normal stop, control-proof gating, early child exit, exact control-check arguments, lifetime deadline, owner death, readiness-receipt failure, TERM-to-KILL escalation and bounded receipt writing are distinct failure modes. Prelaunch cancellation now checks that no child output/PID or control-check evidence appears. Normal stop/deadline now verify that the independently recorded child PID exits; the owner-death test already did so. The control check now requires the complete config-free argument vector. Receipt bounds use an independent 8192-byte expectation and rejection must leave no receipt. Escalation uses a fake child; readiness-write failure proves failure/teardown reporting, not independent child exit, and its test was renamed accordingly. |
| Five scenarios in [test_known_hosts_initialization.sh](../.agents/skills/verify-groundplane/scripts/test_known_hosts_initialization.sh) | Exact trusted-file copy with mode 0600; reject symlink, FIFO and over-2-MiB sources; remove temporary output after failed atomic replacement. These use actual temporary files but do not establish remote host identity or authorize trust enrollment. |
| Four scenarios in [test_supervisor_wait.sh](../.agents/skills/verify-groundplane/scripts/test_supervisor_wait.sh) | Bound waiting for a running process; accept zombie and absent-process states; reject mismatched PID evidence as ambiguous. Simulated procfs, not a real GP restart or lifecycle test. |

All 11 Python tests pass in `.tmp/qa-test-review-verifier-python.log`. Both shell
scripts exit zero under separate ten-second limits; evidence is
`.tmp/qa-test-review-verifier-known-hosts.log` and
`.tmp/qa-test-review-verifier-wait.log`. Expected refused-input diagnostics are
not failures; the wait log is empty on success. The tests clean their owned
temporary files/children; the run logs remain. No helper implementation or
verification workflow changed.

#### Controller HTTP lifecycle, admission and errors

Reviewed 46 tests in ten files. Retained 35 local behavioral checks and ten
delivery checks; removed one duplicate overflow-classification test. Local
socket tests use shortened deadlines and injected handlers. They do not qualify
normal GP restart, upgrade recovery, durable replay or application continuity.

| Reviewed tests | Disposition and reason | Matrix coverage or gap |
| --- | --- | --- |
| Twenty-one in [http_lifecycle_test.go](../internal/controller/http_lifecycle_test.go) | Retained 18 behavioral tests for header/idle limits, first-write and processing deadlines, known/unknown/chunked body bounds, SSE liveness/flush/disconnect/stall and graceful drain. Retained two delivery checks for pinned policy and the bodyless Backup route classification. Removed `TestHumaMaxBytesErrorMapsToCanonical413`; the same input is already checked for kind, code and status in `problems_test.go`. | UI-03/05, BP-03, TASK-06, LOG-02, REL-01/04: local transport only. No multipart semantic validation, durable event sequence, backend subscription cleanup or host restart proof. |
| Three in [server_test.go](../internal/controller/server_test.go) | Retained. Already-cancelled multi-listener startup returns cleanly; an occupied port reports failure; normal close and unexpected causal errors remain distinguishable. | HOST-02, REL-01, UI-05: local Serve/error boundary, not installed runtime ownership or partial multi-listener startup failure cleanup. |
| Four in [problems_adversarial_test.go](../internal/controller/problems_adversarial_test.go), three in [problems_test.go](../internal/controller/problems_test.go) | Retained. Real Huma serialization omits wrapped/opaque diagnostics and emits an exact closed error envelope; local classification distinguishes malformed 400, semantic 422, overflow 413 and fallback errors. The missing-query test now requires the independent 422/code/title tuple instead of deriving the expected title from whatever status happened to return. | UI-03, SEC-05: serialization/classification, not durable log sanitization or every endpoint's validation. |
| Five in [idempotency_test.go](../internal/controller/idempotency_test.go) | Retained four behavioral checks for exact read-only POST exemptions, missing/repeated/invalid mutation keys, accepted mutation keys and keyless safe methods. The non-API middleware boundary is delivery-only. | UI-01/03/04, BP-01, BAK-14: header/path admission with a recording handler, not durable key claims, request equality or replay. |
| Three in [mutation_admission_test.go](../internal/controller/mutation_admission_test.go) | Retained. Native trial blocks real Script-route dispatch and fake scheduled writes; failures stay private; only exact documented exceptions reach the recorder. Exception checks now require exactly zero or one downstream call. | UP-10, SCRIPT-01: guard and scheduler invocation only. Fake admission does not establish durable trial recovery, actual replay or Abort eligibility. |
| Four in [openapi_test.go](../internal/controller/openapi_test.go), one each in [environment_openapi_test.go](../internal/controller/environment_openapi_test.go), [project_openapi_test.go](../internal/controller/project_openapi_test.go), [tenant_openapi_test.go](../internal/controller/tenant_openapi_test.go) | Retained as seven delivery checks: deterministic generated metadata, served/generated byte identity, committed drift, exact Error schema and declared operation/body identities. | Generation/schema constraints, not working routes, cross-surface parity or product effects. |

Two timeout tests formerly accepted their own client read deadline as apparent
server closure; they now require EOF from the server. Oversized headers must
return 431. Deadline-writer assertions now check exact instants, including
delayed first output. The raw-body test formerly wrapped away the fixture's known
length; it now explicitly sets each length mode and independently counts reads
to prove early refusal and deferred streaming. Its name no longer claims full
Blueprint multipart proof. The grace-expiry test was renamed to describe actual
base-context cancellation and stopped serving, not unobserved forced socket closure.

`.tmp/qa-test-review-controller-http.log` records 31 passing tests and 14
sandbox-only listener failures. Those 14 pass with local loopback permission in
`.tmp/qa-test-review-controller-http-loopback.log`; together these verify all 45
retained tests with the race detector. Formatting passes. No production code,
generated artifact, dependency or deployed state changed.

#### Agent transport, workers and transient inputs

Reviewed and retained all 54 tests in eight files. Each now names its matrix
cases, local proof limit and concrete failure-prevention reason. None was a
confirmed duplicate or coverage-only test. These are local behavioral checks,
not live Agent, application or upgrade qualification.

| Reviewed tests | Retention reason | Matrix coverage or gap |
| --- | --- | --- |
| Sixteen in [client_test.go](../internal/agent/client_test.go) | Authentication-first ordering and token ownership; exact Task acknowledgement/event authority; copied DNS proof; configuration/epoch admission; cancellation; private-error handling; reconnect classification, replacement-pool execution and bounded backoff; exact gRPC envelope limits. | HOST-04/05/07, TASK-07/10, CMP-05, DNS-03, LOG-02. Fake streams and two local Unix-socket exchanges do not prove Controller authorization, persistence, live liveness expiry or actual interrupted effects. |
| Sixteen in [worker_test.go](../internal/agent/worker_test.go) | Typed DNS results, predecessor compensation ordering and failure refusal; one-use receipts and replacement ownership; duplicate assignment handling; queued Abort, capacity and worker joins; transient credential clearing; Blueprint executor selection. | DNS-02/03, CMP-05, SVC-09, TASK-02/03/04/06/09/10, HOST-07, CON-07, BP-04/10. Injected executors and observations do not establish filesystem/Docker effects, restored DNS, durable receipts or recovery. |
| One in [config_update_test.go](../internal/agent/config_update_test.go) | Drained configuration replaces capacity and advertises its exact new value. | HOST-07: local reconfiguration only; running-Task continuity and saved configuration remain unproved. |
| One in [task_ack_diagnostic_test.go](../internal/agent/task_ack_diagnostic_test.go) | Component failure diagnostics and complete Task authority survive acknowledgement construction and protobuf serialization. | TASK-07, CMP-05: not Controller classification, durable publication or runtime recovery. |
| Four in [service_observation_test.go](../internal/agent/service_observation_test.go) | Failed/malformed/unconfigured reads return unavailable without consuming Task capacity; success preserves counts; cancellation, deadlines and unknown envelopes cannot publish stale results or release another request. | OBS-01/02/03/04: fake observer/channel behavior, not Docker health, Controller freshness or source/generation fences. |
| Three in [logs_test.go](../internal/agent/logs_test.go) | Typed open-error classification, zero-source readiness/completion and bounded overflow cleanup. | LOG-01/02, TASK-07: injected sources and local queues, not Docker source selection, public SSE or client reconnect. |
| Three in [workload_images_test.go](../internal/agent/workload_images_test.go) | Unknown envelope rejection, exact image-result forwarding without Task capacity use, and one bounded image worker joined at teardown. | SVC-01, TASK-07, HOST-05: fake resolver/session behavior, not actual Docker inspection, Release publication or reconnect fencing. |
| Ten in [backup_secret_inbox_test.go](../internal/agent/backup_secret_inbox_test.go) | Exact one-use slot ownership, receive-buffer clearing, stale assignment refusal, closed purpose selection, terminal/abort/stop clearing, cancellation races, malformed sequences and fresh-pool redispatch. | CON-07, TASK-10, BAK-05/08/16: in-memory ownership only, not Controller credential resolution, S3 execution, durable claim recovery or process-wide erasure. |

Stronger assertions check complete Task identity, nested DNS proof ownership,
independent 32-byte token and 128-record queue expectations, bounded receive-pump
exit and exact observation/image values. Secret tests now prove the accepted copy
survives receive-buffer clearing, a stale frame leaves the current slot usable,
and duplicate headers clear previously accumulated bytes.

All 54 tests pass with the race detector in
`.tmp/qa-test-review-agent-transport.log`; the log also includes a passing focused
rerun of two final assertion changes. Two Unix-socket checks initially required
local socket permission; the complete successful run used that permission.
Formatting passes. No production defect was reproduced, and no production code
or live state changed.

#### Public API models and error taxonomy

Reviewed all 40 existing tests in `pkg/api` and `pkg/errs`. Removed two duplicate
error checks and split one existing source-limit assertion into a separate
delivery test. The final 39 tests comprise 37 local behavioral checks and two
delivery checks. Each behavioral test names its cases, reason and proof limit.

| Reviewed tests | Disposition and reason | Matrix coverage or gap |
| --- | --- | --- |
| Five original tests in [backup_policy_test.go](../pkg/api/backup_policy_test.go) | Retained disabled-policy JSON, strict replacement decoding, exact maximum integer fidelity, range boundaries and overflow rejection. The existing 12-source constant check is now a sixth, delivery-only test. Numeric expectations no longer derive from the implementation's maximum. | BAK-01/02, UI-03: DTO/predicate checks, not HTTP admission, persisted policy, source resolution, scheduling or retention effects. |
| Five in [component_config_test.go](../pkg/api/component_config_test.go) | Retained closed response/mutation unions, explicit-null and invalid-state refusal, empty managed-file arrays and exclusive Tunnel credential modes. Valid credentials now require every decoded field. | CMP-01/02, DNS-02, HTTP-08, UI-01/03: JSON only, not persisted configuration, Secret ownership, rendering, activation or ingress. |
| Three in [component_zone_config_test.go](../pkg/api/component_zone_config_test.go) | Retained distinct response/mutation roundtrips and invalid-selection rejection. Roundtrips now compare complete JSON instead of only the Zone-list substring. | CMP-01, HTTP-07/08, UI-03: public-model choices, not Zone lookup, network membership or runtime placement. |
| Five in [release_group_test.go](../pkg/api/release_group_test.go) | Retained canonical failure-policy encoding, ambiguous JSON refusal, omission/null/string tag decoding, duplicate PATCH refusal and presence-aware encoding. | GRP-01, UI-01/03/04: DTO behavior, not durable edits, idempotency or group execution. |
| Two in [script_execution_json_test.go](../pkg/api/script_execution_json_test.go), two in [script_json_test.go](../pkg/api/script_json_test.go) | Retained complete execution choices, explicit false grants, malformed choices, field/order presence and range checks. Create decoding now proves execution survives and re-encodes exactly, rather than merely succeeding. | SCRIPT-01/03/05/06, UI-03: JSON only, not image sealing, authorization, Task capture, hook order or runner/mount effects. |
| Five in [types_test.go](../pkg/api/types_test.go) | Retained four behavioral checks for runtime intent, exact redacted Connector metadata and explicit S3 addressing presence. Source-kind vocabulary remains a separate delivery check. Exact Connector JSON subsumes its old partial absence assertions. | SVC-01/02, CON-02/03/04, SEC-05, UI-01/03: model encoding/decoding, not lifecycle, Controller redaction, encryption, provider access or Backup dispatch. |
| Thirteen original tests in [errs_test.go](../pkg/errs/errs_test.go) | Retained eleven: full closed catalog, unknown/zero normalization, distinct malformed/semantic failures, wrapping/joining/classification, exact problem JSON, untrusted reconstruction, mutable-carrier defense and opaque-error secrecy. Removed `TestAcceptedPersistenceAndIdempotencyStatuses` and `TestPublicCodesUseCanonicalDotNamespaces`; the strengthened full catalog covers both subsets. | UI-03/05, TASK-07, SVC-12: error-package behavior, not HTTP dispatch, Controller logs, clients, actual recovery or live failures. |

The error catalog now uses independent literal code/class/status tuples rather
than implementation constants. Every tuple is checked through both the internal
descriptor and public constructor/accessors, preserving the removed status
test's protection. Reconstruction starts from independently authored public
problems. The joined-cause assertion now requires both diagnostics to be present
in order; two missing messages can no longer pass the ordering comparison.

All 39 tests pass with the race detector in
`.tmp/qa-test-review-public-models.log`. Three final primary assertion corrections
pass in `.tmp/qa-test-review-public-models-final.log`; unchanged proof is reused.
Formatting passes. Removed tests are recoverable from Git. No production defect
was reproduced and no production code or live state changed.

#### Controller Task, Activity and log boundaries

Reviewed all 26 tests in the six root Controller Task/log test files. Retained
22 local behavioral checks and four delivery checks; none is a confirmed duplicate
or coverage-only test. Each has a case/proof-limit or delivery annotation and a
concrete rationale. All 26 pass with the race detector in
`.tmp/qa-test-review-controller-task-logs.log`, with permitted local HTTP listeners.
Formatting and diff checks pass. No product defect was reproduced; no production
code, generated artifact or live state changed.

| Reviewed tests | Retention reason | Matrix coverage or gap |
| --- | --- | --- |
| Two in [task_routes_test.go](../internal/controller/task_routes_test.go) | Retry and Abort forward the exact Task id and replay key once, preserving accepted identity and JSON response. | TASK-02/03/05: HTTP dispatch with recording fakes, not durable eligibility, cancellation, replay or execution. |
| Five in [task_event_routes_test.go](../internal/controller/task_event_routes_test.go) | Canonical resume parsing, exact SSE shape/resume forwarding, pre-header problem mapping and safe post-frame failure. The operation-scoped OpenAPI check is delivery-only. | TASK-06/07, UI-03: local HTTP with an injected journal, not actual journal bounds, durable sequences, compaction, terminal drain or resumed delivery. |
| Eleven in [task_read_routes_test.go](../internal/controller/task_read_routes_test.go) | Closed actor/type/prune provenance, Controller-step lifecycle, exact Task/event read identity and revision, missing-Task problem, alias page/cursor forwarding and scope admission. Two OpenAPI identity/enum checks are delivery-only. | TASK-01/06, BAK-08, UI-03: projection and HTTP-to-query boundaries, not real fixed-revision storage, historical ownership, cursor binding or isolation. |
| Four in [log_routes_test.go](../internal/controller/log_routes_test.go) | Canonical query/media parsing, body/resume/Accept refusal and exact bounded Agent-event projection. | LOG-01/02, TASK-06, UI-03: parsers and direct handler/projection checks, not Docker source selection, actual truncation, subscriptions, follow streams or cleanup. |
| Two in [task_step_contract_test.go](../internal/controller/task_step_contract_test.go) | Invalid Script/operation identity fails as Internal. The paired-field/exclusion schema is a separate delivery check. | TASK-01, SCRIPT-03: pure projection, not durable validation, HTTP schema enforcement or hook execution. |
| Two in [task_wake_test.go](../internal/controller/task_wake_test.go) | Accepted 202 mutations hint both executor owners once; synchronous 204 does not. | TASK-01: callback invocation only, not actual Task publication, scheduler latency or executor progress. |

The fixed-revision test formerly used a fake that ignored read arguments; it now
requires the exact Task id and revision 17. Projection rejection starts from
valid baselines. Task/Activity identities, timestamps and step kinds are explicit;
conflicting scopes must make no query. Missing Task, rejected resume and invalid
log requests require exact public error codes, not status alone. The post-frame
failure must preserve its exact initial frame and emit nothing else. The 32 KiB
line bound is independent of the implementation constant. OpenAPI assertions are
scoped to the event operation and the step's actual forbidden fields. Two test
names now describe fake-page projection and injected resume-rejection mapping,
without claiming storage behavior. Local HTTP clients have a two-second bound.

#### Controller-Agent report containment and log ownership

Reviewed and retained all ten behavioral tests in two Agent-channel files; all
have concrete reasons and local case/proof-limit annotations. All ten pass with
the race detector in `.tmp/qa-test-review-controller-agent-report-logs.log`.
The logged quarantine warnings/errors are deliberately injected test inputs,
not failures from a deployed Agent. Formatting and diff checks pass. No product
defect was reproduced; no production code or live state changed.

| Reviewed tests | Retention reason | Matrix coverage or gap |
| --- | --- | --- |
| Five in [task_report_containment_test.go](../internal/controller/agentchannel/task_report_containment_test.go) | An applied exact-report conflict preserves its claim while unrelated work completes; known Component diagnostics validate and map correctly; capacity-one dispatch cannot overclaim; only exact delivered ownership can enter quarantine. | TASK-07/09/10, HOST-07, CMP-05: scripted channel, fake store and pure selection, not real publication races, Agent restart/recovery or live workload continuity. |
| Five in [logs_test.go](../internal/controller/agentchannel/logs_test.go) | Distinct pre-ready failures release slots; post-ready failure closes events; caller cleanup expiry and stalled subscribe sends retain ownership until cancel delivery; overflow reserves its slot until cancellation is confirmed. | LOG-01/02: controlled in-memory registry/channel tests, not actual gRPC send completion, Docker cleanup, public SSE or process memory measurements. |

Stronger report assertions require the exact failed step/epoch and independent
diagnostic spellings. A different assignment cannot be quarantined; refusal,
successful containment and repeated reports must preserve unrelated ownership.
Log limits now use independent eight-slot and 128-record expectations instead
of production constants. Confirmed cancellation must remove the old slot;
replacement must preserve all seven unrelated subscriptions. Sixteen sequential
cancel cycles test slot reuse. Command waits and the overflow admission probe
are bounded so a broken limit or missing handoff reports a local failure.

#### Controller-Agent assignment admission and session lifecycle

Reviewed and retained all 29 behavioral tests in these seven files. Each has a
case link, a concrete failure-prevention reason and a local proof limit. No test
in this batch was a confirmed tautology or duplicate. The verification target is
the exact 29 tests plus one unchanged consumer of the simplified fence helper.
All 30 pass with the race detector in
`.tmp/qa-test-review-agent-admission-registry.log`. That log also retains the
initial test-only compile error: the three new plan-hash assertions addressed
the assignment instead of its nested plan. The corrected assertions pass.

| Reviewed tests | Retention reason and corrections | Matrix coverage or gap |
| --- | --- | --- |
| Five in [assignment_admission_test.go](../internal/controller/agentchannel/assignment_admission_test.go) | Resume wakes dispatch; each pause owns only its hold; lifecycle fences survive resume; cancellation restores admission without terminating an admitted send; preparation drains before pause completes. Stronger checks require callback execution, resumed snapshot identity and completion of the released send. | HOST-04/07, UP-04, TASK-10: in-memory admission and controlled concurrency, not durable claims, live update or workload continuity. |
| One in [assignment_pause_dispatch_test.go](../internal/controller/agentchannel/assignment_pause_dispatch_test.go) | A pause during plan resolution leaves a valid assignment neither delivered nor quarantined; resume sends its exact Task, assignment and plan identity. | HOST-07, TASK-10: fake-store dispatch, not real claim persistence or execution. |
| One in [assignment_quarantine_test.go](../internal/controller/agentchannel/assignment_quarantine_test.go) | One unrenderable recovered assignment is isolated while the valid assignment is sent, with exact delivered/quarantined ownership. The rationale no longer claims timeout or repeated-Ready proof. | TASK-09/10, HOST-05: one fake-store dispatch, not reconnect recovery or bounded timeout. |
| One in [config_update_test.go](../internal/controller/agentchannel/config_update_test.go) | Initial and replacement interval/concurrency values are exact. Replacement must wait for a Ready showing full capacity under the old limit; premature lowering would reject that later Ready. | HOST-07: scripted sequencing, not real worker drain, saved labels or subsequent dispatch under the new limit. |
| One in [blueprint_closing_dispatch_test.go](../internal/controller/agentchannel/blueprint_closing_dispatch_test.go) | Injected Controller completion must not resolve the old plan, quarantine it or consume capacity; the next assignment has exact ownership and wire identity. Renamed to describe reconnect completion rather than claim durable Blueprint proof. | TASK-10, BP-10: fake completion only, not actual Blueprint terminal publication or desired-state races. |
| One in [native_predecessor_assignment_test.go](../internal/controller/agentchannel/native_predecessor_assignment_test.go) | Preserve complete restoration identity, candidate target and durable digest; native artifact bytes remain unchanged after the caller mutates its buffers. | SVC-09/12, TASK-10: wire conversion and copy ownership, not digest derivation, valid runtime artifacts or actual restoration. |
| Nineteen in [registry_test.go](../internal/controller/agentchannel/registry_test.go) | Preserve coalesced wake signals, replacement/revocation ownership, exact-generation fresh Ready, canceled subscription cleanup, lifecycle drain responsiveness and stale/invalid/canceled-open rejection. Stronger checks require actual subscription removal, a fresh report after reconnect and rejected send callbacks while stopped. | HOST-04/05/07, TASK-01/09/10, UP-04: local registry and controlled send races, not credentials, durable lifecycle state, network teardown or process recovery. |

The fence helper now calls the concrete registry method directly, removing stale
runtime interface discovery. Its unchanged consumer
`TestConnectDoesNotDeliverAssignmentClaimedDuringPriorGenerationFence` is included
in verification; the rest of `server_test.go` is not reviewed by this batch.
No production code or live state changed. These checks do not qualify a whole
product case or uninterrupted hosting. Formatting and diff checks pass.

#### Durable Task event-stream boundaries

The bounded review retains five tests in
[task_event_stream_test.go](../internal/infra/etcd/task_event_stream_test.go),
each linked to TASK-06: suffix replay and terminal closure, resume validation
before opening watches, sequence-based compaction recovery, gap refusal and
subscription cleanup on blocked-consumer cancellation. Their in-memory store
does not qualify real etcd compaction, HTTP SSE or Controller/Agent execution.
The first focused race run passed all five in
`.tmp/qa-test-review-durable-task-stream.log`, but subsequent failure invalidated
the compaction pass. Seven of 20 runs fail in
`.tmp/qa-test-review-durable-task-stream-compaction-repeat.log`. A final bounded
run with an early-return assertion fails on its first iteration in
`.tmp/qa-test-review-durable-task-stream-compaction-error.log` with
`internal: task event watch closed unexpectedly`.

The memory store injects the compaction error and closes both watch channels;
the production adapter uses the same error-then-close sequence. The old Task stream
could select the closed event channel instead of the queued compaction error,
returning before the required resnapshot. This reproduced a local stream failure,
not a live etcd incident or application outage. The owner approved the scoped
repair (D6), with no deployment or broader refactor.

Stronger retained assertions require exact watch selectors and snapshot-successor
revisions, no extra events after closure, zero watch starts for an invalid resume,
preserved terminal replay records and released subscriptions after gap/cancellation.
The blocked consumer is now actually delivering an event before cancellation.
Final-drain delivery order remains scheduler-selected in these fixtures; they do
not independently prove every notification ordering. No failure assertion was
weakened or converted into an expected-success disconnect.

Approved repair proof: `TestTaskEventStreamRecoversClosedWatchBeforeFollow` closes
each watch before following begins, then commits a new event. It requires both
watches to reopen, sequences 2 and 3 exactly once, terminal closure and no retained
subscriptions. Its pre-fix failure is `.tmp/task-stream-completion-red.log` and
its passing post-fix run is `.tmp/task-stream-completion-green.log`. Completion
is arranged before following, rather than racing the producer against the reader;
Go may still select either ready channel, and neither order may cause failure.
The existing live-compaction regression remains.

`TestTaskEventStreamClosedWatchPreservesFailureAndCancellation` exercises storage
failure, closure without an error and caller cancellation on both watches. It
requires the correct error, no invented progress, no watch restart and no retained
subscription. Together these are seven final Task-stream tests: five reviewed and
two added for the approved repair, all with reasons and local proof limits.

The implementation disables closed event channels and handles both watches'
terminal errors through one path. Compaction still resnapshots from the last
sequence; ordinary failures disconnect and cancellation remains cancellation.
All seven stream tests plus three existing watch-adapter tests pass with the race
detector in `.tmp/task-stream-completion-tests.log`. The live-compaction,
closed-before-follow and failure/cancellation tests each pass 20 bounded repeats
in `.tmp/task-stream-completion-repeat.log` (60 top-level executions).
Vet and pinned Staticcheck pass in `.tmp/task-stream-completion-vet.log` and
`.tmp/task-stream-completion-staticcheck-local.log`. The initial analyzer log,
`.tmp/task-stream-completion-staticcheck.log`, preserves a read-only external-cache
error; the successful rerun uses the ignored repository-local cache. Formatting
and diff checks pass. This closes the local failure, not whole-case TASK-06,
full CI, hosting continuity or live compaction qualification.

#### Controller-Agent image, observation and materialization exchanges

Reviewed and retained all 13 behavioral tests in the three files below. Each
has a concrete reason and local proof limit; none is a confirmed tautology or
duplicate. All 13 pass with the race detector in
`.tmp/qa-test-review-agent-read-transfer.log`. Formatting and diff checks pass.

| Reviewed tests | Retention reason and corrections | Matrix coverage or gap |
| --- | --- | --- |
| Five in [workload_images_test.go](../internal/controller/agentchannel/workload_images_test.go) | Unavailable correlation preserves Ready and queues no request; busy/stale responses cannot satisfy lookup; a replaced session returns unavailable; the Controller loop transports the exact selector/result; malformed/canceled lookups release their slot and retired responses cannot satisfy a later successful retry. Success now requires exact request, selector and local image identity, not just a nil error. | BP-08, SVC-05/10, HOST-05: local registry and in-memory stream, not Docker inspection, public preflight refusal, durable publication or network reconnect. |
| Six in [service_observation_test.go](../internal/controller/agentchannel/service_observation_test.go) | Observation is independent of Task/image capacity; fresh exact correlation is required; replaced sessions and malformed rows fail without evidence; cancellation precedes replacement; offline/invalid/canceled reads retain the correct cause. Success now checks the full returned row and identity. The deadline check uses a ten-second parent and an independent five-second expectation, so omitting the product timeout can no longer pass. | OBS-01/03/04, HOST-05: local exchange and ordered sends, not actual container counts, serving-source revision races, public freshness or Agent worker teardown. |
| Two in [materialization_sender_test.go](../internal/controller/agentchannel/materialization_sender_test.go) | Assignment precedes header/content/End; every record has exact execution identity; selected header fields and distinct chunk contents match the source at the independent 32 KiB boundary. Same-length corrupt bytes reach digest verification, return Internal and never send End. | ENT-02/04, BP-04, TASK-10: sender/recording-stream proof, not Agent/helper validation, file publication, ownership/mode effects or live secret safety. |

The materialization recorder copies messages during Send because the sender clears
its transient content afterward. Assertions observe bytes handed to transport,
not cleared caller-owned references. The fake source's closed/cleared state proves
the sender invoked its Close method; it does not qualify production-source memory
clearing. No production code, test framework or live state changed, and no new
product defect was reproduced. These results do not qualify a complete case.

The remaining root-module Go tests outside the reviewed files still require review.
Helpers and fixtures are not standalone test cases; inventory counts must not
classify them as behavioral coverage. No automatic blanket deletion based on
filenames, missing comments, mocks or small test size.

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
