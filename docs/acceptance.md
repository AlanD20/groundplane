# Historical acceptance evidence

The [QA matrix](qa-matrix.md) owns the case catalogue. This register records
what earlier runs established, failed to establish, or left blocked. It does
not change feature requirements and does not establish current live-runtime
qualification. Local delivery checks and operator-journey evidence are separate.

The detailed pre-consolidation reports remain available in Git at `08ccb78b4`.
That history is an archive, not current host state, an executable recipe, or
permission to repeat a mutation. Raw evidence included private ignored files;
this tracked register therefore retains only sanitized, independently useful
facts. A missing private artifact is a provenance limit, never a reason to
promote a historical result.

## Local migration delivery gate

On 2026-09-21, `make ci` passed for `eec4aa4ea` after retiring the temporary
architecture deferrals. All 40 relocated integration tests were retained. CI now
calls the architecture checker directly, which reported zero findings against
the unchanged baseline. Existing legacy baseline allowances remain; no live-host
qualification or deployment was performed.

On 2026-09-21, the unchanged `make ci` passed on Linux amd64 for code through
`4816c7cb3`, with documentation through `968c3c48d`. This includes generated-file
parity, formatting, static analysis, race-enabled behavior tests and Controller,
embedded Console and Agent-image artifact checks. The architecture release gate
uses the approved 0.0.1 deferrals; strict compliance is not established.

The retained proxy-cancellation regression also passed 20 race-enabled runs after
its fake attachment was corrected to match the Docker client's ownership contract.
No live host, upgrade, traffic or privileged Backup/Restore qualification ran.

## Minimum record for future runs

Before a qualification run, enumerate every selected matrix case and variant as
NOT RUN. Record:

- the case, requirement and test-definition revision;
- exact source/release, binary and image identities, including dirty-patch
  identity when applicable;
- supported architecture, configuration and sanitized fixture identity;
- initial desired, serving, data, ownership and permission state;
- the protected request, Task/attempt and actual fault point without secrets;
- independent expected and observed application, data, runtime and cleanup
  assertions with their time/size bounds;
- PASS, FAIL, BLOCKED, NOT RUN, NOT APPLICABLE, or historical PARTIAL; and
- remaining variants, missing probes and any later run that closes a finding.

PASS requires all assertions and required cleanup. An expected rejection passes
only when forbidden effects are also proved absent. Failure to inject the fault
is NOT RUN. A manual repair may restore a baseline, but does not turn failed
automatic recovery into a pass. Preserve failures instead of overwriting them.

## Historical evidence register

### Initial recovery and hosting records

| Ref | Cases and historical outcome | Evidence retained and limitation |
| --- | --- | --- |
| H1 | HOST-01 PASS on 2026-09-13. | API/CLI Host parity and owned tunnel/runtime cleanup passed on Controller `0.0.0-qa.recovery20260913.7` at `637990dd4`; the CLI came from a dirty checkout. No Console, bootstrap, application, upgrade, or clean-candidate claim. |
| H2 | UP-01, UP-07, UP-12, JOURNEY-05 PARTIAL. | A 600-second window recorded 2,948 HTTP 200s, no recorded request failures, 118 pongs on one WebSocket and 5.105-second worst latency; a failed native Controller candidate restored its predecessor and checked data. Candidate correlation, all phases, public routes and application-rollout recovery remained incomplete. |
| H3 | BP-04/05, OBS-01, HTTP-02/09, BACK-06/10, SCRIPT-03, JOURNEY-01 PARTIAL. | Earlier private-hosting runs exercised exact reapply, authentication, realtime clients and healthy observations on different builds. They are not a row-wide or every-surface pass. |
| H4 | SVC-14, ENT-08, ATT-08, SCRIPT-04, JOURNEY-03 PARTIAL. | A native cutover and forward Deploy on `637990dd4` preserved 11 healthy Services, checked data, unrelated Releases, current backing membership and two realtime clients. Automatic recovery and all variants were not exercised. |
| H5 | SVC-15, JOURNEY-02 FAIL. | Failed-candidate recovery restored an obsolete backing network after runtime configuration changed, producing HTTP 500 although recovery steps completed. A later normal Deploy repaired service but did not close the failed automatic-recovery result; the finding remains historical even though later clean journeys passed. |
| H6 | UI-03/04, TASK-02/03/04/05, SCRIPT-02/08, BACK-05, UP-03 PARTIAL. | Protected replay/rejection, Script Abort and unsafe-Retry rejection, authentication validation and rejected update input passed on older builds. Not all variants or current Console parity ran. |
| H7 | HTTP-08, UP-12 PARTIAL with failure. | Whole-build public Tunnel timeouts and continuity failures were observed. Private-path HTTP success cannot qualify the public path or establish the cause. |
| H8 | ATT-12 FAIL for one overlapping-name run. | New credentials worked at the new address while the bare backing name still resolved to the old instance. Old Detach restored resolution. A later clean overlap variant passed, but this failed run remains evidence. |
| H9 | ATT-08/09/10/11, SVC-13/16/17, BACK-05 PARTIAL, local only. | Selected-only Attach projection, shared-resource retention, source conflicts, atomic acknowledgement, replay and pruning independence passed in execution-plan, etcd and attach-planning tests after `e06a0d2bc`. No live application or recovery reader ran. |
| H10 | HOST-01 PASS rerun on 2026-09-14. | API/CLI canonical assertions and cleanup passed against the same `.7` Controller; the dirty-source client and running identities were recorded. This refreshed only the read-only baseline. |

### Local recovery building blocks

| Ref | Cases and historical outcome | Evidence retained and limitation |
| --- | --- | --- |
| H11 | BP-04, SVC-15, JOURNEY-02 PARTIAL, local. | Executed-runtime selection, atomic completion, no-effect rejection and replay passed in Blueprint runtime tests. No recovery reader or pinned-file execution ran. |
| H12 | ENT-08, SVC-13/15, JOURNEY-02 PARTIAL, local. | Entry mutation selected the acknowledged runtime, preserved proxy authority, rejected stale sources and committed Task/receipt atomically. Docker execution and source retention were not proved. |
| H13 | REL-01 PARTIAL, local. | Controller close preserved etcd runtime; restart verifier logic accepted Agent restart and rejected etcd restart or changed Agent identity. No live restart, traffic, data or deployment check ran. |
| H14 | SEC-07 PARTIAL, local. | Secret recovery pins blocked direct, retry and hierarchy deletion; malformed membership failed closed. Automatic pin lifecycle, restoration and live recovery were absent. |
| H15 | SEC-07 PARTIAL, local. | Task-secret-pin preparation, activation CAS, restart cleanup, explicit release, bounds and publication races passed against a controlled store. Production callers and deployment were unrun. |
| H16 | SVC-15, JOURNEY-02 PARTIAL, local. | Immutable runtime-configuration source storage and success-only acknowledgement passed, including replay and Retry preservation. Generated Component bytes, other writers, automatic pins and live recovery remained incomplete. |
| H17 | SVC-15, JOURNEY-02 PARTIAL, local. | Immutable retained Component content, bounded chunking, exact metadata and resolver use without rerendering passed. No deployment or selected-file recovery execution. |
| H18 | SEC-07, SVC-15 PARTIAL, local. | Actual desired publication, claim, acknowledgement, failed-attempt Retry, pin release and expiry races passed. No deployed file restoration or closing live recovery run. |
| H19 | SVC-15, JOURNEY-02 PARTIAL, local. | Filesystem verification rejected changed bytes, ownership, mode, links and replacement races; controlled Docker checks proved a read-only helper envelope. No real helper or live recovery ran. |
| H20 | SVC-15, JOURNEY-02 PARTIAL, local. | Acknowledged runtime readers preserved exact YAML, bindings, proxy and retained-slot bytes through a real two-pass store journey; a modeled maximum remained within limits. Full Attach/Entry failure recovery was unrun. |
| H21 | SVC-15, JOURNEY-02 PARTIAL, local. | Route and applied-Entry writers published configuration references and Secret pins; conflicting owners and heads rejected while compatible shared heads composed. Agent execution and deployment were unqualified. |
| H22 | SVC-15, JOURNEY-02 PARTIAL, local. | Sealed recovery plans and pinned sources rejected invented, foreign or changed files and retained exact predecessor bytes. No actual file recovery or deployment. |
| H23 | SVC-15, JOURNEY-02 PARTIAL, local. | A real Task journal reconstructed compensation for an issued write after repository restart and rejected reordered, foreign and non-running evidence. Full assignment transition and Agent I/O were modeled. |
| H24 | SVC-15, JOURNEY-02 PARTIAL, integrated local. | Controller delivery and Agent tests covered partial write, durable admission, restore then re-probe, reconnect and buffer retirement under existing limits. The helper was controlled; no live failed Task ran. |
| H25 | Candidate `6c89049fa`, BLOCKED. | Most runtime, Agent, Console, build and deployment prerequisites passed, but the durable-store run had 30 failures. VOL-07 also exceeded its 26-comparison terminal limit with 27 comparisons. No deployment, fault or live removal ran. |
| H26 | VOL-07 correction PARTIAL, local. | The extra configuration-head comparison was removed only from mutations that do not write files. Four removal/replay variants passed at 26 comparisons, 18 mutations and 13,250 bytes; eight H25 fixture mismatches remained. |
| H27 | BP-04, SVC-15/JOURNEY-02, VOL-07, BACK-01, HTTP-06 PARTIAL, local. | The eight fixtures were aligned with acknowledged runtime/configuration, final-publication fault injection and current-authority assertions. The complete durable-store race package passed; two unrelated fixtures were corrected. No live operation or full CI ran. |

### 2026-09-14 live production-operations sequence

| Ref | Cases and historical outcome | Evidence retained and limitation |
| --- | --- | --- |
| H28 | HOST-01 PASS; JOURNEY-01 PARTIAL. | Fresh clean source `76578c72f`, version `.qa.prodops20260914.1`, installed and hosted 11 healthy Services with authentication, realtime clients and password-only backings. The reset deleted disposable Volume contents; no public ingress, continuity or full background transaction claim. |
| H29 | BP-05 FAIL. | Exact Blueprint reapply recreated only the ingress router despite unchanged image, while serving Releases and profile survived. Subsequent cutover proof initially compared against the wrong pre-reapply snapshot; that attribution error did not close the recreation defect. |
| H30 | BP-05 repair PARTIAL, local. | Fixed-revision capture showed whole-map comparison included native proxy configs added by Deploys. Consumer-scoped comparison passed captured-artifact replay and affected race checks, but was not yet deployed. |
| H31 | JOURNEY-05 PARTIAL; SVC-06 FAIL. | Update to clean `2895989f2` held 45 HTTP 200s and eight pongs with 0.245-second max latency and preserved non-Agent runtimes. A profile-disabled canary's first Deploy failed because sealed Service authority was absent. |
| H32 | H31 repair PARTIAL, local. | Publisher tests preserved profile-disabled Service definitions and then restricted explicit Deploy to named members. Full affected Controller and Release suites passed; no live rerun yet. |
| H33 | JOURNEY-05 PARTIAL; BP-05 FAIL. | Update to signed `13d130929` passed 45 HTTP requests and nine pongs, max 1.858 seconds. Canary first Deploy passed, but exact reapply recreated its router and dropped the held HTTP connection; no outage duration was inferred. |
| H34 | H33 acknowledgement repair PARTIAL, local. | Configured Blueprint success now advances the sealed applied Component artifact while leaving native Service runtime unchanged; affected store, Entry and Blueprint checks passed. Existing history was not rewritten. |
| H35 | BP-05 PASS, tested ATT-12 overlap PASS, JOURNEY-05 PARTIAL. | Signed `18e936fa9` passed a 45-second update window. A fresh Apply/Deploy/reapply preserved router and workload identities while 90 same-connection requests passed; four-consumer Attach/Entry/Detach cutover passed with both realtime clients and 11 healthy Services. Sustained and failed-candidate proof remained. |
| H36 | SVC-15, JOURNEY-02 BLOCKED/FAIL. | A candidate-only fault left the original Task nonterminal after 600 seconds. Recovery probe alternated failure until the 1,000-event cap rejected progress and degraded the Agent. Application profile still served, but automatic recovery did not complete. |
| H37 | H36 Volume-scope repair PARTIAL, local. | Evidence showed five unrelated project Volumes were rejected as collisions. Scoping only named Volumes absent from the sealed artifact passed integration and preserved required/unknown-resource guards. Event saturation remained open. |
| H38 | TASK-06/07 and SVC-15 rolling history PASS, local. | Appending 1,003 events retained sequences 4–1,003, with atomic conflict, lost-response, restart, cursor-expiry and terminal-drain behavior. H36's live Task still needed repaired code. |
| H39 | SVC-15 recovery PASS only after approved one-time repair. | Version `.qa.prodops20260914.5` preserved Task identity/history while replacing the Agent selection. The original Task timed out with compensation proof; 45 HTTP 200s and nine pongs passed, and a later healthy Deploy cleaned the candidate. This was not unattended or normal-upgrade qualification. |
| H40 | REL-01 PASS. | Normal Controller restart changed PID while preserving runtime-directory inode, Agent identity/generation, etcd container, application runtimes and profile. A 45-second window passed 45 HTTP 200s and eight pongs; no host reboot or pressure case. |
| H41 | SVC-15, JOURNEY-02 FAIL on a clean rerun. | On `.5`, a candidate-only fault again failed predecessor restoration because the sealed proxy identity described newly applied config while the preserved proxy still had old labels. The Task and candidate were retained; ownership checks were not weakened. |
| H42 | Capacity cleanup PASS; later QA actions NOT RUN. | Two obsolete reset archives were hash-verified before remote removal, increasing free space from about 0.72 GB to 2.82 GB. Version `.6` staged but was not activated; the mixed-workload probe was only prepared. |
| H43 | H41 writer repair PARTIAL, local. | When forward work does not apply the proxy, receipts now combine candidate workload with sealed predecessor proxy ownership; Deploy/Rollback suites passed. No live recovery rerun. |
| H44 | Disposable rebuild/setup; HOST-01 PASS. | Signed `939fd039f` was installed as `.qa.proxy20260914.1` after an approved destructive reset and verified archive. Exact Controller and Agent digests were recorded; application setup had not yet qualified recovery or continuity. |
| H45 | Fresh private-hosting baseline PASS. | Eight Deploys completed; 11 Services, registration/OTP/profile, route isolation, TLS, two realtime clients, scanner/worker processing, reviewer approval and signed callback passed. No recovery, upgrade, public ingress or sustained claim. |
| H46 | SVC-15, JOURNEY-02 PASS. | Clean Attach/Entry/Detach cutover passed, then a candidate-only fault terminalized `timed_out` in about 352 seconds and automatically restored the application. All Services, backings, clients and profile passed; the Agent released its claim and later Deploy cleaned the candidate. H41 remains a separate failure. |
| H47 | Native update/recovery PARTIAL PASS. | Normal `.1→.2`, deliberately broken Controller recovery to `.2`, and normal `.2→.3` preserved non-Agent runtimes and application authority. These ran during mixed load but did not cover all update phases or ingress variants. |
| H48 | REL-03 Controller-loss and Agent-loss variants PASS. | Killing the exact process while a Script runner was active still produced one physical runner start, original Task completion/replay, owned cleanup and unchanged application. Reboot and other effect boundaries remained separate. |
| H49 | REL-04 and private-ingress JOURNEY-05 PASS. | A 1,800.035-second window with five authenticated clients recorded 9,000 HTTP 200s, zero unexpected failures/disconnections, 360 pongs per WebSocket and HTTP p95 0.194049 seconds, max 2.299403. It included H47–H48 operations, not public ingress, exhaustion or every activation phase. |
| H50 | Guest reboot FAIL. | The guest accepted reboot, but SSH did not return in 240 seconds; VMM logs showed exit without automatic VM restart. Later owner replacement destroyed this installation. H43–H49 remain valid only for that historical installation. |

### Replacement host, upgrade, reboot, and proxy records

| Ref | Cases and historical outcome | Evidence retained and limitation |
| --- | --- | --- |
| H51 | Fresh installation and HOST-01 PASS. | Signed `2676fbae6`, version `.qa.owner20260914.1`, installed on the owner-provided replacement; Agent enrollment and read-only Host journey passed. Image transport required correcting a config-ID versus manifest-ID comparison; application qualification was pending. |
| H52 | Fresh hosting baseline PASS. | New hierarchy, backings, credentials, six Attach stages, 43-step Blueprint and eight Deploys completed. Eleven Services, authentication, realtime, TLS, review and synthetic processing passed. Prior installation data and receipts were not restored. |
| H53 | UP-03 FAIL. | Selected Controller/Agent requests refused correctly, but an absent canonical release digest returned HTTP 500 instead of an input/resource refusal. No Task or runtime changed; concurrent 45-second traffic passed. |
| H54 | H53 repair PARTIAL, local. | Missing requested digest leaf became a validation error while missing parents, artifacts and closed store stayed Internal. Complete release-store and coordinator suites passed; live failure was not yet closed. |
| H55 | H53 live closure PASS; selected UP-04/05/11 preparation PASS. | Signed `64b2056dd` staged `.2`. Busy drain timed out without aborting its Script, preparation Abort settled safely, normal update preserved runtimes, and the missing digest now returned 422. A 360-second window passed 360 HTTP 200s and 65 pongs. |
| H56 | UP-08 and selected UP-06/10/11 trial guards PASS. | A deliberate unready Agent candidate reached `starting`; ordinary mutations refused during trial, automatic recovery restored H55's exact selection, and 240 HTTP 200s/43 pongs passed. Scheduled-writer and unknown-publication variants remained. |
| H57 | UP-09 `starting` interruption PASS. | Exact candidate PID/inode/journal proof authorized SIGKILL. The original Task failed/recovered, qualified selection and application survived, and 180 HTTP 200s/33 pongs passed. Other phases and reboot remained separate. |
| H58 | Remaining selected UP-03 refusal variants PASS. | Malformed, incompatible storage/channel and corrupt binary candidates all returned 422 without Tasks or runtime changes. A first traffic probe had stale-session 401 before the batch; the corrected 45-second confirmation passed. |
| H59 | UP-02 standalone Agent replacement PASS on a disposable host. | Downgrade and measured upgrade each produced one replacement, one generation increment, authenticated Ready, protected replay and preserved etcd/Service/sentinel data. Short held connections passed; native selection enrollment remained. |
| H60 | UP-02 qualified-selection enrollment PASS. | A native update qualified `.2`; normal Agent removal/join selected it over older bootstrap config and preserved application/etcd/sentinel state. Eighteen HTTP 200s and four pongs passed. |
| H61 | REL-02 automatic idle-host reboot FAIL. | Reboot returned zero, but the host did not return with a changed boot id within 240 seconds. No manual recovery was allowed during the probe; GP startup and data recovery were unverified. |
| H62 | Post-owner-start platform recovery PASS, application recovery FAIL. | After manual VM start, Controller, Agent, etcd, Task history and Volume sentinel survived. The canary and proxy stayed exited and router returned 502 because the fixture omitted authored restart policy. H61 remained failed. |
| H63 | Fixture correction attempt FAIL. | Adding `restart: unless-stopped` published desired state, but the following blue-green Deploy failed proxy activation because it tried to switch a stopped proxy. Automatic predecessor recovery restored HTTP/WebSocket/sentinel; runtime policy remained `no`. |
| H64 | Recreate and post-owner-start checks PASS; automatic host return FAIL. | Recreate applied `unless-stopped`. After another reboot and owner restart, platform, app, identities, Tasks, sentinel, HTTP and WebSocket passed after 220.84 seconds. External intervention was essential, so this is not automatic recovery or outage timing. |
| H65 | SVC-06, OBS-05 and operator-managed REL-02 variants PASS. | Clean `9e0732ed4`/`.qa.proxyboot20260914.2` passed stopped-proxy recovery, already-running preservation, 45 HTTP requests/nine pongs, degraded-to-healthy observation, and VM stop/start with exact live/restart routing and full application checks. Automatic VM return, in-flight boot recovery and HA were not qualified. |

### Later recovery and release packaging

| Ref | Cases and historical outcome | Evidence retained and limitation |
| --- | --- | --- |
| H66 | SVC-09 repair PARTIAL, local. | Integrated Agent/Moby observation reproduced recovery misclassifying its own absent-from-predecessor candidate as foreign; corrected classification passed while ownership drift still rejected. No live Task or deployment. |
| H67 | SVC-09 PASS live on 2026-09-19. | Source `30245a441`, version `.qa.recovery20260919.1`, survived native update traffic. A post-deploy Script `sleep 20; exit 23` failed only the new candidate; the Task closed failed in 26.463 seconds, predecessor and 11 Services survived, and 96 HTTP requests/18 pongs passed through recovery and cleanup. This covers that fault point, not every recovery variant. |
| H68 | PKG-01/02/03 PARTIAL, local. | Ten bundle tests and the 83-test deployment suite passed; native amd64 Controller/CLI builds matched the descriptor and Console audit was clean. The Agent scratch image measured 147,450,780 bytes versus 275,992,667 baseline and passed Docker/Compose/helper probes. No arm64, registry publication, host install or complete helper qualification. |
| H69 | PKG-04 PARTIAL, local. | Eight installer-idempotency and five distro-selection tests passed, as did the 99-test deployment gate and ShellCheck. Fake package/process evidence covered three allowed OS releases, drift and resume paths. All real OS/architecture fresh-install, rerun and native-update journeys remained unrun. |

### Blueprint and Entry intent

| Ref | Cases and historical outcome | Evidence retained and limitation |
| --- | --- | --- |
| H70 | BP-05/06/09, ENT-01 and UI-01 PARTIAL on 2026-09-23. | Signed `c246b837a`, version `0.0.1-qa.blueprint20260923.5`, passed local `make ci` and a normal update on one disposable Ubuntu 24.04 amd64 host. Isolated QA showed direct Entry adoption, an empty key-only secret, nonempty secret rotation, secret-safe export, omission of one Entry, refusal of persistent Volume omission, and a Console review with a new empty-secret badge; the review was cancelled before Apply. An earlier build returned 500 on secret edit, then corrected; later builds exposed and corrected editor refresh, HTTP-origin hashing and a generated Volume bind in canonical export. Exact reapply retained Entry IDs and selected synthetic secret values, but Validate reported updates and Apply created a Task. No running Service, managed-file pickup, runtime continuity, at-rest inspection, latest-wins handoff or resource cleanup was qualified. |
| H71 | BP-02 FAIL on 2026-09-23. | On the same `.5` QA host, a fresh isolated Environment and corrected BusyBox Blueprint returned a Validate diff creating a Service, Entry and implicit default Zone. Apply of the same bytes rejected the default Zone because it lacked an explicit IPv4 subnet. The fixture was incomplete, but Validate should have refused it. No Apply Task, Service, Entry, Zone or workload appeared; the Environment create Task and disposable fixture remain. Running-Service Entry removal was NOT RUN. |
| H72 | BP-02/06/09 selected variants PASS on 2026-09-23. | Signed `e59367f56` passed local `make ci`; a normal update to `.6` completed on the same disposable Ubuntu 24.04 amd64 host. Validate now rejects H71's exact implicit-Zone input. An explicit-Zone Blueprint created a healthy BusyBox Service and one plain Entry. Omitting that Entry removed its record and mode-600 managed file without restarting the container or changing its loaded value; the next Deploy created a healthy new container without the variable. The isolated Environment deletion Task completed, its container and owned root disappeared, and the test image was removed. The earlier smoke fixture and small private evidence bundles remain. Unchanged Service/Zone previewed as `update`; other omissions, recovery, latest-wins and full production workload continuity remain unqualified. |
| H73 | PKG-07 and BP-05 selected variants PASS; BP-02 diff remains inaccurate on 2026-09-23. | Public `install.sh --ref main` resolved pushed signed `739e4dd6f` and completed a guarded update on the same disposable Ubuntu 24.04 amd64 host. Controller and Agent selected that commit; etcd stayed healthy. No application ran during the update, so upgrade traffic continuity was not tested. In a separate HTTP-serving Environment, exact Blueprint reapply completed without changing Service id, serving Release, single Release-ledger item, healthy container id/start time or 150 successful host HTTP responses. Validate still called unchanged Compose, Service and Zone `update`. The test Environment, container, owned root and image were removed; the earlier smoke Environment remained. Hooks, proxy, WebSockets, real application traffic and failure/recovery were not tested. |
| H74 | PKG-02 successful-update and PKG-07 traffic-continuity variants PASS on 2026-09-23. | On freshly reset disposable Ubuntu 24.04 amd64 VM `.3`, the public installer installed pinned `739e4dd6f`, then upgraded under traffic to pushed signed `2e9a0319799`. The protected Controller Task completed and installed CLI, Controller and Agent all reported the new ref. The canary and etcd kept exact container IDs/start times, zero restarts and the same application data marker; background ticks advanced. A 5/s HTTP and held-stream probe recorded 2,807 successful requests, 563 stream updates, zero errors/disconnections and 3.9 ms p95; Console returned HTTP 200. The isolated fixture remains on disposable QA for inspection. Raw evidence is ignored locally under `.tmp/qa-installer-upgrade-repeat-20260923-882861/`. Failed/uncertain update branches, other platforms, public ingress/WebSockets and a sustained 30-minute run were not qualified. |

## Runner delivery

**H75 — selected RUN-01/04/09/10/12/13/14 variants PASS, 2026-09-28.**
On Ubuntu 24.04.4 amd64, Controller `0.0.1-qa-4f4c61ab6-r6` ran the repair source
committed as `8ca14df59`. Its complete local `make ci` passed. The Runner image
was pinned to `sha256:7f34cb2400f29be2eed3c6bd15e9327d806f1f9efc9f4e4c71a71299edc99b90`.
A disposable Project-owned GitHub Runner built an HTTP counter application with
rootless Docker, pushed through the private authenticated TLS registry, fetched
through GP and deployed through the ordinary CLI. Independent HTTP source output,
registry manifest/configuration digests, host image layers and serving Release
matched the workflow receipt. The persistent counter advanced across deployment.

Deliberate build exit 42, unauthorized push and missing-image Fetch each failed
before another Release or container change; the original source and advancing
counter remained served. After an idle listener stop, GP restarted the same
registered container within its maintenance interval. GitHub briefly reported a
session conflict, then reconnected; the next workflow ran without another token.
Normal GP removal deleted the Runner container, daemon unit, account/subordinate
ranges, network and owned directories. The registry retained the exact delivered
manifest; registry and application container identities/start times stayed intact.
The delivered canary remains for inspection; GitHub deregistration stays manual.

Earlier failed startup/TLS attempts are retained as failures, not overwritten.
Full token-failure, private-network denial, quota/concurrency, other ownership
scopes, host reboot, registry restart and arm64 variants were not run. Moved-tag
and replay behavior has local regression proof only. No new load, installation,
upgrade or general production-readiness claim follows from this run.

## Fresh installation, DNS and etcd activation

**H76 — selected installation and etcd variants PASS; DNS lifecycle BLOCKED,
2026-10-01.** Source `e51089e0ad99b7d1f96f366e30a9173ebba346b3` passed
local `make ci` and GitHub CI. The public Pages installer matched that source's
installer bytes. On a fresh Ubuntu 24.04.4 amd64 host, documented Docker build
prerequisites were installed first; `install.sh --ref main` then resolved that
exact commit, built and installed GP itself. CoreDNS bootstrap completed without
Retry, actual registry and upstream DNS answers passed, LAN Console returned
HTTP 200, and the existing API/CLI Host journey passed with owned tunnel cleanup.
Owned build containers, temporary tags and source directories were removed;
no host Go or Node toolchain remained. This is not other-platform qualification.

For HOST-12, Save remained pending through a Controller restart with unchanged
etcd container identity. Stale Save was rejected. Explicit Apply changed etcd
arguments; terminal exact replay returned the original Task without another
container replacement. An isolated HTTP application's process, held stream and
Volume sentinel survived, and its background writer progressed.

For HOST-13, killing only the new etcd candidate caused GP to restore its exact
predecessor arguments and data mount. Separately, killing the Controller during
prepared candidate activation caused automatic restart and receipt-owned rollback.
Both original Tasks failed, desired candidates remained pending, application
runtime/data survived, and subsequent store writes succeeded. The candidate-kill
probe timed out while polling during the expected outage; separate inspection
confirmed recovery. The corrected probe finished the independent interruption
case without repeating earlier passing checks. Explicit failed-request replay,
other crash points and sustained load were not run. Original etcd settings were
restored through Save and Apply, not by editing recovery files.

DNS-05 reached actual explicit-IP and stable Service-proxy answers. An invalid
reserved-name edit was rejected; a separately accepted invalid Corefile Task
failed without replacing the serving answer, and normal configuration restoration
completed. Redeployment stopped at HTTP 500, `release publication operation
budget is invalid`: proxy-reservation transaction fragments were added without
updating the exact shape accounting. Service removal returned HTTP 422,
`Service removal candidate projection changed`; Zone removal returned HTTP 422,
`Zone removal candidate desired Services changed`. Both stopped before the DNS
dependency guard, so neither guard nor successful resource cleanup is qualified.
These failures block release signoff; they are not passing safety checks.

Original DNS settings were restored. The small isolated canary is retained for
repair/retest, with its failed-run evidence; no manual unlock, history rewrite,
VM reset or tag was performed. Raw captures and host identities remain ignored
locally. Historical application, public ingress and Runner qualification was not
repeated; Backup/Restore, additional stress and alerts remain deferred.

## DNS lifecycle repair and installer update

**H77 — selected update and DNS variants PASS; post-Zone Service cleanup FAIL,
2026-10-01.** Source `0bea315a06b9582e7d37c890fac591014707dd3c` passed
full local `make ci` and fixed H76's Deploy accounting and removal-projection
admission defects. The public installer resolved that exact `main` commit and
completed its normal guarded Controller/Agent update on the same disposable
Ubuntu amd64 host. The activation probe recorded 114 successful HTTP requests
and no held-stream error. The application process, Volume sentinel and etcd
container identity remained unchanged; its background writer progressed.
Installed Controller, Agent and etcd were healthy, and owned build containers
were absent. This is a bounded private-HTTP check, not public ingress or load
qualification.

DNS-05 passed recreate and blue-green redeployment with the same actual proxy
address and DNS answer. Desired DNS references blocked Service/Zone removal
with `resource.in_use`. An accepted invalid Corefile failed without replacing
the old answer; last-applied references still blocked both removals. Reserved
name admission and subsequent valid restoration passed. Publication/removal
races were not injected.

After normal DNS-record removal, last-membership Zone Remove passed admission
and both Agent steps, but its acknowledgement repeatedly failed with
`environment mutation contains a duplicate compare key`. The terminal assembler
added the same Zone-removal fragment twice. Its scoped repair removes that
duplicate path; the Agent completion/replay regression and full local `make ci`
pass. GitHub CI also passed for `86e9467cf44e5048188049198f55a429d66222ba`.
An explicitly approved, Controller-only maintenance installation loaded that
repair. Agent and etcd container identities, CLI, configuration, keys and Volume
sentinel were unchanged. This was manual repair, not a normal protected upgrade;
the predecessor binary and settled native receipt were retained unchanged.

The original Zone Task settled as timed out after its deadline had expired.
Ordinary Retry completed with the same operation ID and plan hash. The Zone API
returned 404, its Docker network was absent, and Service membership was empty.
However, subsequent Service Remove failed with `Service removal has unexpected
or remaining runtime`. Independent Docker inspection found both the regular
workload created by Zone removal and the earlier blue-green workload still
running. The Zone procedure recreates the desired Compose member rather than
reconciling the acknowledged slot runtime; the extra regular member is outside
the Service removal plan's captured runtime. This also blocks SVC-04's selected
post-Zone-removal variant. Zone Task completion therefore does
not qualify correct blue-green runtime cleanup. Service removal and protected
Volume/hierarchy cleanup remain blocked. The Volume sentinel is intact and
Controller, Agent and etcd remain healthy; no force-removal was used.

Cleanup probes corrected their assumptions about host paths and omitted empty
membership fields. Raw failures and original Task history remain retained.
Original DNS settings are restored; the remaining isolated resources are retained.
No forced unlock, history rewrite, VM reset or tag was performed.
Historical application, Tunnel and Runner journeys were not repeated;
Backup/Restore, additional stress and alerts remain deferred.

## Zone runtime repair and cleanup blocker

**H78 — local repair PASS; deployment and fresh live retest BLOCKED,
2026-10-01.** Source `0bee842f77df596e44ba4bcb8d48755bd7111396` passed
full local `make ci` and was pushed on `main`, without a tag. Zone removal now
disconnects existing network members rather than reapplying desired Compose.
The execution/replay regression covers an empty network, a regular workload,
and a proxy with both blue-green slots; unexpected container mutations fail it.
The backing-cascade caller uses the same procedure. Neither its live variant nor
the fresh blue-green journey has run on this candidate.

The owner approved removal of only H77's verified extra regular QA workload.
Its labels and exact identity were checked before non-force stop/removal. The
blue-green workload, Volume sentinel and original Task records were unchanged.
Normal GP Service removal then completed; independent Docker inventory found no
remaining Service containers. The sentinel persisted in the Volume's bind source
after Docker unmounted it. Protected Volume removal subsequently completed,
with API absence and physical data-directory removal verified. That disposable
QA data was intentionally destroyed through the approved removal workflow.

Environment removal's Agent child completed, but its Controller parent remained
running with `environment retained live child authority`. Read-only inspection
identified an empty Zone subnet registry (`reservations: {}`); the finalizer
requires this key to be absent rather than retiring the empty registry. Normal
Abort returned `state.conflict`. Environment and parent hierarchy cleanup remain
blocked; no record was manually deleted or unlocked.

The public ref installer built and staged the exact candidate, but its normal
Controller update remained pending behind the cleanup Task. Only that unstarted
update was aborted through GP; the installer reported failure and retained its
installation files. The old Controller remains installed. This is not a passing
upgrade or live qualification of the repair. Failed evidence and Task history
remain retained locally. No VM reset, compatibility path or release tag was used.

## Environment finalization repair

**H79 — local repair PASS; live qualification NOT RUN, 2026-10-01.** Source
`b9f6e17f98d702d3e8348bbef8c655a2e17b1fcf` passes full local `make ci`.
Direct and aggregate Environment finalization now share one revision-checked
preparation: retire only empty Zone reservations and inert scheduling defaults,
while preserving active-child guards. Existing behavioral regressions cover
successful retirement, non-empty and corrupt records, concurrent reservation
insertion and terminal parent removal. The root-package allowance decreased;
no compatibility or manual-unlock path was added.

Publication is blocked by unavailable GitHub SSH authentication. The approved
Controller-only repair has not run; the original QA cleanup Task remains stuck
on the old Controller. Resumed cleanup, normal installer activation and the fresh
Zone/blue-green/Service removal journey remain unqualified. H78's failure
evidence and original Task records are retained.

## Installer activation and selected cleanup qualification

**H80 — normal Controller update and selected cleanup PASS; retained
blue-green slot FAIL, 2026-10-01.** The public installer built and activated
`main` at `3b22e9333676cb3808b066f7972f94c4d9a37430` using `--controller-only`.
This documentation-only successor has H79's CI-passing production code.
The protected update Task completed; the running Controller digest matched its
staged manifest, the bundled CLI matched the ref, and host health was healthy.
The Agent kept its container and image identities; its process restarted.
Etcd kept both its identity and start time. Prior failed Task records were unchanged.
No application workload was present during this update, so it adds no traffic
or held-connection continuity proof.

Before that normal update, the approved Controller-only maintenance installation
briefly loaded H79's repair. The original stuck Environment Task completed without
Retry, preserving its operation, plan and start identity; the Environment and its
containers were absent. The maintenance verifier incorrectly required an unchanged
Agent start time and therefore restored the old Controller. This was a verifier
failure, not a passing normal upgrade or product-owned recovery. The subsequent
normal installer update above activated the repair without another maintenance
replacement, reset, database edit, unlock or history rewrite.

A fresh isolated Environment then served an actual HTTP response with its Volume
sentinel and completed two blue-green Deploys. The second switch removed the prior
blue slot: the Release ledger and Docker lifecycle events agree, and
[the proxy executor](../internal/infra/docker/composehelper/service_proxy.go)
explicitly removes the previous workload after switching. This contradicts the
[retained-slot contract](features/services-and-releases.md#strategies-and-replicas).
The requested both-slots-present removal variant therefore failed its prerequisite;
it was not silently counted as a pass or repaired in this run.

The narrower serving-slot-plus-proxy variant passed. Zone removal retained both
actual container identities, left them running and disconnected, removed the
network and membership, preserved the Volume sentinel and replayed the same
terminal response. No extra regular workload appeared. Normal Service removal
then removed all of that Service's containers while preserving the physical Volume
data. Protected Volume removal deleted the confirmed source; Environment, Project
and Tenant removal completed through GP with API absence and no owned containers.
Both original and fresh QA hierarchies are now removed, with failure history kept.

Fixture/probe failures remain in ignored evidence separately from the successful
selected run; they were corrected without product changes or repeated creates.

This closes H78/H79's selected cleanup and publication blockers, not blanket release
qualification. Both-slot live removal, backing-consumer, concurrent-removal and
interruption variants remain unqualified. Historical application, Tunnel and Runner
journeys were not repeated; Backup/Restore, additional stress and alerts remain
deferred. No release tag was created.

## Blue-green retention and both-slot cleanup

**H81 — selected retention and cleanup variants PASS, 2026-10-01.**
`539922bffad57c941962b2441f62390e47f4655b` passes full local `make ci`,
including targeted runtime, lifecycle-authority, removal and recovery regressions.
The public `--ref main` installer completed a normal Controller/Agent update on
disposable QA. The Controller, CLI and Agent matched the exact staged manifest;
host health was healthy. Etcd retained its identity and start time, and prior
failed Task records were unchanged. No workload was present during the update,
so this adds no update traffic-continuity proof. An initial read-only probe used
an unsupported CLI flag; its corrected invocation passed without product changes.

A fresh isolated Environment applied a recreate singleton with a persistent
Volume, then completed three blue-green Deploys. The first switch retired the
singleton. The second retained the exact previous blue container and stable proxy
alongside the serving green slot. The third replaced only inactive blue; green
and the proxy kept their identities. An HTTP stream opened on green delivered
successive lines across that switch, while new HTTP requests reached the new
candidate. The Volume sentinel stayed intact.

Zone removal kept both slots and proxy running with unchanged identities, removed
their network memberships and the network, preserved data and replayed the same
terminal response. Service removal removed all three containers without deleting
the physical Volume sentinel. Protected Volume removal then deleted only the
confirmed test source. Environment, Project and Tenant removal completed through
GP with API absence and no owned containers. All isolated test resources were
removed; evidence and Task history remain.

This closes H80's predecessor-retention defect and its blocked both-slot removal
variant. Held WebSockets, failed switches, Start/Stop/Destroy, blue-green-to-recreate,
backing consumers and concurrent/interrupted removal were not exercised live.
Historical application, Tunnel and Runner journeys were not repeated. No reset,
manual repair, compatibility path or release tag was used.

## Bounded logs and blue-green failure check

**H82 — LOG-01/02 selected variants PASS; lifecycle and recovery FAIL, 2026-10-01.**
An isolated Python HTTP Service on candidate `539922bff` emitted a 300-line
stdout burst and continuing background ticks. Service and Environment follow
streams each delivered the requested tail and later ticks during two bounded
sessions, with client cancellation and successful reopening. A CLI tail sample
returned actual workload output. This is private API/CLI evidence, not browser,
public-ingress, memory-growth or source-cleanup qualification.

The Service had a stable proxy and two retained blue-green slots. Stop failed
before mutation because its reconstructed proxy owner differed from the actual
older proxy. A subsequent successful image-only Deploy served Python 3.14.
An Alpine candidate could not execute the Python command: its start failed after
replacing the inactive slot. The durable recovery record contained Running but
not Completed mutation evidence. Recovery incorrectly omitted compensation
eligibility, leaving the original Task running beyond the 180-second observation
window. Independent HTTP still reached the same serving process, its background
counter advanced and the persistent sentinel remained intact; the inactive slot
was not restored, so recovery did not pass.

Local repairs pin each acknowledged physical member's original bytes and owner,
compare the runtime revision at publication and retain compensation eligibility
for a durably started mutation that fails. Targeted race checks passed, including
untouched-member refusal. Full local CI passed on 2026-10-02. The owner approved
a 75,000-line root-etcd ceiling and QA repair/reset; deployment and live retesting
remain pending. During the 2026-10-01 run, no manual repair, reset,
Task-history change or tag was performed. Start, Destroy, historical rollback,
normal upgrade under traffic and owned-resource cleanup were blocked or unrun.
On 2026-10-02, repeated recovery had exhausted etcd's storage quota and prevented
Controller startup. The approved disposable host reset removed that runtime and
data after retaining incident evidence. This reset does not prove recovery of the
original failed Task; a fresh fault-injected run is required.

## Supplementary router and observation evidence

The 2026-09-10/12 Router work predates the numbered register and remains local or
disposable-QA evidence:

- Full-Caddyfile authoring, fixed-revision preview, stale-upload handling, API/
  CLI projection, 25 Console suites and a production build passed locally. A
  GET-only browser check covered invalid UTF-8, 32 KiB bounds and responsive
  editing. The architecture gate remained red and browser screenshots were not
  persisted.
- QA version `.qa.native20260910.8` replaced the retired marker and passed
  3,000 held WebSocket deliveries plus 1,200 verified-TLS HTTP requests, but
  Configure recreated Caddy and Tunnel; it was not file-only continuity proof.
- Version `.9` added native preflight. An invalid Caddy directive failed at the
  first step while preserving the prior file, Caddy/Tunnel identities and
  HTTP/TLS service. Its longer whole-build sample had two pre-activation 502s,
  so public-path continuity failed.
- Retained-runtime and directory-mount reload behavior passed local race checks
  but was not deployed before the storage incident. Full-policy file-only reload
  remained open.
- Route-summary UI tests/build and a read-only responsive browser fixture passed
  locally. Service observation later passed focused Go checks, API/client
  generation, 30 Console suites/build and a local expiry UI fixture. No live
  Docker observation, deployment, full CI, Gate A or Gate B ran.

These records do not authorize the old requested follow-up mutations. Current
Router and observation availability is owned by the feature and capability
documents.

## Supplementary safe-update evidence

The 2026-09-10 update work established local foundations before H47–H60:

- reversible Agent admission holds, uncertain-publication recovery, cold-start
  hold restoration and exact generation fencing passed focused race checks;
- the native coordinator sealed release/predecessor/Agent identity, bounded
  drain, activation/Abort winner, candidate qualification and watchdog recovery;
- staging rejected unsafe or changed paths, deployment receipts were fsynced and
  replayable, a 720-second client timeout did not SSH-rollback or Abort, and the
  operator API/CLI/Console vertical passed focused checks;
- strict-storage audit found ordinary writes could escape an unfinished trial;
  HTTP and scheduler writers were then gated locally, while reads and exact
  protected replay stayed available; and
- the broad architecture gate remained red, so none of this was full CI.

Disposable QA releases A–G supplied concrete but mixed continuity evidence.
Bootstrap A saw a public QUIC/WebSocket loss and invalid probe instrumentation.
Normal B activation passed 3,000 WebSocket deliveries and 1,200 HTTP requests.
Busy drain and preparation Abort preserved following work; non-executable and
unready candidates recovered their predecessor. C and E had pre-activation
traffic failures; D lacked an isolated HTTP window. A constrained diagnostic F
and checked-in bounded-build G each passed 3,000/1,200, but one passing bounded
sample did not prove the cause of earlier Tunnel failures. These are the source
of H7's unresolved public-path limitation; later private-path update passes do
not rewrite them.

## Supplementary Script execution evidence

The 2026-09-10 Script work was local evidence, not live explicit-context
qualification:

- numeric hook order `0..65535`, default zero, ordered before slug, passed
  parser, persistence, release, API/CLI/Console and fixed-revision checks;
- the explicit model bound a digest image, numeric uid/gid, closed grants and a
  networkless minimal runner, while inherited execution remained unchanged;
- immutable source, final-publication and transaction-bound checks passed,
  including a maximum Blueprint fixture at 192 comparisons and 176 success
  mutations under the unchanged 256-per-arm limit;
- resource preparation validated the entire grant set before secret reads,
  retained only declared sources, and cleared temporary plaintext;
- Blueprint/API/CLI/Console authoring and a memory-only responsive browser
  fixture passed; two unrelated broad CLI expectations and the architecture
  gate remained red; and
- Agent/Controller composition with fake Docker proved exact grants, cleanup
  before consumer start, failure/Abort barriers and lost-ack recovery without
  duplicate start. Real Docker enforcement and source-to-host behavior did not
  run.

Manual immutable-source tests covered publication/admission, terminal and Abort
fences, before-start timeout/Retry, bounded retention, startup abandonment,
protected replay and plain/secret Entry lifetimes. Five broader Blueprint/
recovery failure groups remained on the historical baseline. No current QA
pause or next action is inherited from that report.

## Storage-integrity incident on 2026-09-10

An attempted J deployment from `8e5f62586` stopped before publication when the
checkout filesystem reported no available bytes. No J update Task existed; QA
remained on `.qa.native20260910.9`. During the interval the kernel reported
virtual-disk write and extent-conversion errors with potential data loss, Valkey
reported AOF fsync failure/MISCONF, public login returned 500 and API health
remained 200. Later Valkey health and login recovery proved availability only,
not persistent-data integrity.

Only verified regenerable repository-local Go caches were removed. The later
approved cleanup reclaimed 17,769,828,352 bytes and left tracked source
unchanged; it did not delete evidence, credentials, Docker artifacts or
application data. Deployment subsequently gained fail-closed free-space checks:
10 GiB before target access, native compilation and each uncached image build,
then 2 GiB before image transfer/publication. Sixty deployment tests passed in
the real ownership namespace; a restricted sandbox still failed trusted-owner
fixtures without weakening checks.

The incident was a dated qualification blocker, not proof of current disk state
and not a current instruction to pause QA. No historical record established
whether every affected PostgreSQL, Valkey, Volume, application file and
Groundplane state byte remained intact.

## Interpretation limit

No row establishes current production readiness, current host health, full CI,
Gate A, Gate B, or blanket coverage beyond its named cases and variants. Later
passes do not erase earlier failures; they are separate source- and
environment-bound observations. Current capability, issue and qualification
documents decide what remains available or blocked now.

Private workload identities, domains, credentials, endpoints, topology and raw
host captures are not public artifacts. Future tracked evidence must remain
sanitized and independently understandable without an ignored private run
directory.
