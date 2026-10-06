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

## Complete retained recovery and installer continuity

**H83 — selected lifecycle, recovery, rollback and normal update PASS,
2026-10-02.** Disposable Ubuntu 24.04 amd64 was installed through the public
`install.sh --ref main`, without a manual source clone, at `a59acf782`.
An isolated Python HTTP Service used a persistent Volume, stable TCP proxy and
two blue-green slots. Stop stopped all three containers without replacing their
identities; Start returned both slots and the proxy to running with the same
data sentinel. Service and Environment tail300/follow, cancellation/reopening
and CLI output passed on this installation.

That initial candidate still failed complete restoration: an Alpine candidate
could not execute the Python command, and recovery closed the failed Task while
leaving the inactive retained slot missing. The serving process and data survived.
A separate normal good Deploy rebuilt the baseline; it did not repair or rewrite
the failed Task. This failure remains recorded, not converted into a pass.

The normal public installer then updated Controller and Agent to `39841e304`.
The update Task completed and installed bytes matched its staged manifest and
Controller checksum. During 342 seconds, 1,705 private HTTP requests succeeded
with p95 8.1 ms. A held HTTP stream delivered 685 lines and remained open through
completion. Both workload slots, proxy, serving process start time, data sentinel
and independent etcd identity/start time were unchanged; the background counter
advanced. This was normal updater activation, not a maintenance installation.

A fresh Alpine fault on the updated candidate failed Task
`task_01M3XJHNNY34S3V13ZZCS0C16B` with `reconciliation_required: false`.
Independent Docker inventory found both captured slots and the proxy running;
HTTP still reached the unchanged serving process and preserved sentinel. The
destroyed inactive slot was recreated from its sealed predecessor, not confused
with the failed candidate. An initial rollback check correctly rejected a tag
whose retained predecessor the fixture had already replaced. After explicitly
establishing a successful Python 3.13 then 3.14 pair, historical rollback Task
`task_01M3XJP6182W6R7MFS52264Z87` completed and HTTP reported Python 3.13.15.

Destroy removed all three containers while leaving the physical Volume sentinel.
Service removal, impact-confirmed Volume removal and Environment/Project/Tenant
removal completed through GP. API absence and removal of the Volume path were
checked. No owned test runtime remained. Failure and passing transcripts are
retained in ignored local evidence. Full local CI passed for `39841e304`; this
does not cover subsequent Backup protocol work.

These are selected singleton/private HTTP variants, not held WebSocket,
public-ingress, multi-Service/replica, every OS/architecture, concurrent mutation,
reboot or Backup/Restore qualification. Historical public application and Runner
checks were not repeated. No Task-history rewrite, forced unlock, manual runtime
repair during this run, compatibility path or release tag was used.

## Config backup and key-era recovery

**H84 — selected Config capture, Restore and key-era variants PASS;
Backup/Restore qualification PARTIAL, 2026-10-03.** On disposable Ubuntu amd64,
encrypted Config capture produced one verified Recovery Point. Restore on
`ee0c27e57` recovered the complete four-Entry set: changed plain and secret values
matched their independently seeded originals, and an Entry added after capture
was removed. Earlier rejected and aborted attempts remain failed evidence.

On `fa20d7eb0`, key rotation completed and an explicit export returned a different
identity. Restoring the old point without an identity returned 422; supplying
the new era's identity returned 409. Both requests preserved the complete Entry
set and values. Supplying the previously exported old identity completed Restore
and reproduced the captured values. Identities and raw transcripts remain in
private ignored evidence, not public documentation.

These results cover only the stated Config variants of BAK-06/12/14/15. Fact-derived
values, interrupted roll-forward, transient-key persistence and generic Retry
were not exercised. Volume capture and PostgreSQL capture remained rejected at
admission; neither source's Restore is qualified. The disposable resources remain
for the continuing QA run. This is not complete CI, release readiness or a tag.

## Source restore, retention and deletion boundary

**H85 — selected source Restore and retention variants PASS; Environment
deletion FAIL; Backup/Restore qualification PARTIAL, 2026-10-03.** Disposable
Ubuntu amd64 runs used immutable local Agent and PostgreSQL helper images.
Config restored the complete plain/secret Entry set on `ee0c27e57`.
An unmounted Volume restored exact files, bytes, uid/gid and modes on `d72c329bf`.
The corrected PostgreSQL helper on that Controller recovered independently seeded
rows with running consumers. On `a97ccf3e0`, actual Stop and Restore preserved the
same stopped containers; a separate database and the source Point stayed unchanged.
An object-store outage failed Restore before mutation. Ordinary Task Retry after
availability returned recovered the original pinned Point, not a new selection.

On `493fa60e0`, mounted-Volume capture and Restore reproduced original host and
container-visible files, ownership and modes. The same consumer containers
returned to their prior running state; the source Point was unchanged. Controller
and Agent activation used ordinary update Tasks, not a repair installation.
The deployed Controller release was
`sha256:90a678e9465f1acb098b4191f546229ed9f103d89f9bd64492a54e44beb76548`;
the Agent OCI manifest digest was
`sha256:4890d37ba94ed08ee7042cc15723113b508db478ec492ea9fed748e60dc01204`.

Lowering Volume retention to one kept the newest verified Point. Independent
authenticated object-store reads confirmed the former exact object absent and
the retained object present. Deletion is asynchronous: an immediate post-capture
check failed before the separate prune Task completed. A continuation-script
anchor error also repeated one harmless capture; both failed checks remain in
private evidence and are not product defects or independent qualification runs.

Deleting this isolated Environment then failed at Connector finalization with
retained Backup descendants. The deletion planner did not include remote Point,
orphan, configured-policy or key cleanup before dropping Connector authority.
The safety guard retained the Environment and credentials; this is a missing
cleanup integration, not successful parent deletion. No manual record deletion,
Task rewrite or VM reset was used to hide it.

Full local `make ci` passed on `493fa60e0`, including generation parity,
formatting, architecture, static analysis, race-enabled behavior checks and
packaged Controller/Console/Agent checks. No new tests were added. These results
cover selected BAK-06/08/11/12/13 variants and the failing BAK-17 path only.
Scheduling faults, interrupted mutation/cleanup, other provider/archive failures,
arm64 and complete Gate B qualification remain unproven. Earlier recovery-required
incident state remains retained separately. There is no release tag or blanket
production-readiness claim.

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

## Backup-owned Environment deletion

**H86 — retained Config/Volume cleanup through Task Retry PASS; BAK-17 PARTIAL,
2026-10-03.** On disposable Ubuntu amd64, `3947a0547` captured and verified one
Config and one Volume point in a new isolated Environment. Its deletion failed
before remote cleanup because the executor received a domain summary without
the durable action fence. `845260ee9` corrected that handoff to read the stored
operation. Both candidates activated through ordinary Controller-only update
Tasks; Agent, etcd and existing workloads were not replaced or reset.

Ordinary Task Retry resumed the same sealed deletion plan. Independent signed
object-store HEAD requests proved both exact objects absent. The owned Volume
path was absent; Environment and Connector reads returned 404. The capture Task
remained completed and the original deletion Task remained failed. Independently
seeded PostgreSQL rows and unrelated Recovery Points were unchanged.

Full local `make ci` passed on `845260ee9`; no new tests were added. The deployed
Controller digest was
`sha256:0972a48ae2a9f2759401992d34a8b28a73703beca7f0d6c629b37bbad0d45bea`.
The earlier H85 plan remains immutable and was not retrofitted. This run does not
qualify uncertain remote deletion, interruption, orphan variants, scheduling,
arm64 or complete Gate B. No release tag was created.

## Bounded Backup fault recovery

**H87 — selected failure and interruption variants, including uncertain Environment
deletion, PASS, 2026-10-03.** Disposable Ubuntu amd64 used separate, isolated Backup
Environments and an S3-compatible fault gateway restricted to their owned object
prefix. Expected results came from captured Entry values, host files, SQL rows,
independent authenticated object HEAD requests and retained original Task ids.

- A later Volume upload failure left the earlier verified Config Point intact.
  Ordinary Retry completed without uploading that Config again. A failed HEAD
  exposed no unverified Point and completed through ordinary Retry.
- Corrupted Config and Volume downloads failed before overwrite; independently
  changed target values and files remained unchanged. Interrupting the Agent
  during download resumed the original Restore Tasks without changing their
  source Points.
- Volume Restore resumed after actual tree exchange but before old-tree cleanup.
  Exact restored bytes, complete tree contents and absence of its temporary
  sibling were independently checked.
- Killing the Controller during Config publication first exposed a false Abort
  on Agent session teardown. `f318b31aa` preserves durable Backup work on that
  teardown. After ordinary Agent-only update, a separate Restore resumed the
  same Task from Entry ordinal 1 of 17 and restored the exact complete set.
  The original failed Restore remains recovery-required; it was not rewritten
  or declared recovered.
- Controller downtime caught up one scheduled occurrence; repeated live ticks
  created no duplicate, and an overlapping Restore recorded a skipped occurrence
  without creating a competing Backup. Existing calendar behavior checks cover
  backward intervals and multi-day catch-up; the host clock was not changed.
- PostgreSQL Restore admission exposed inconsistent derived runtime artifact
  handles between planning and storage validation. `2c3dc9fb7` moves that
  derivation into their shared reader, preserving exact artifact checks. An
  Agent interruption during observed `pg_restore` resumed the original Task
  and apply execution id/nonce. All 100,000 rows, their sum and ordered value
  hash matched capture; another database and its Points were unchanged.
- Interrupted retention first exposed a replay requirement for already retired
  Points. The same repair skips payloads with acknowledged exact deletion and
  validates each remaining object against its original plan. After an approved
  Controller-only repair, the original prune Task completed. Only one Config,
  one Volume and the unselected PostgreSQL Point remained; independent HEAD
  proved the retired exact objects absent. Application and etcd containers,
  protected installation files and immutable Task identity were preserved.
  The object retained by the earlier failed HEAD verification was subsequently
  adopted as a verified Point by ordinary orphan reconciliation. That preserved
  its original retention intent and left four verified Points before deletion;
  the earlier three-Point retention assertion describes its observation time.

Full local `make ci` passed for `f318b31aa` and the complete `2c3dc9fb7` batch.
No new tests were added. Earlier reconnect timeout and sandbox ownership failures
are retained; installer safety checks were not relaxed. That earlier Controller's
bytes were `sha256:c4c6d44dfef5a9033062875a9c43e86d1d4a1a8746b897f04afb96b7355f62b9`;
the Agent image was
`sha256:b62248912ceda1ec68d14a42d9f6355cc14fbcaf3eb0a7a9f5a603e7d6a9636f`.
The Controller repair is not normal-upgrade proof.

The initial Environment deletion stalled on its generated `attach.grant-revoke`
child before remote deletion. At that observation, authenticated HEAD requests
returned 200 for all four exact objects, and the Environment, Connector and Points
remained readable. Its invalid immutable child plan remains incident evidence;
no Task, fence or history was rewritten to make it executable.

`34fb59c06` replaces the incomplete Attach cascade with one sealed deprovision
child and exact Controller finalization. The next full Config/Volume/PostgreSQL
fixture reached remote deletion but exposed premature local Volume destruction
and rejection of the child's Detach plan by the generic Remove operation check.
`a4dfa9f54` orders Backup cleanup before destructive host cleanup and binds the
typed Attach child to its actual execution operation. After Controller-only
repair, that fixture's already accepted Retry and child completed without a plan
rewrite; the original failed Task stayed failed.

A fresh equivalent fixture captured three verified Points. The gateway deleted
one actual object, lost the DELETE response and rejected the subsequent HEAD.
Removal failed while retaining the Connector, exact Volume sentinel bytes and
PostgreSQL sentinel row. Ordinary Retry continued the same cleanup operation.
Killing the Controller while the gateway held another DELETE did not require a
reset or another Retry: the accepted Retry completed after restart. Independent
authenticated HEAD requests proved all three exact objects absent. The owned
Volume path, Environment, Connector, Service, Attach facts and workload containers
were absent. PostgreSQL catalog reads proved the credential role absent, with
the database retained under administrator ownership and no public CONNECT or
TEMPORARY privileges; its sentinel row was unchanged, as required by Detach.
Another database's rows and Recovery Points were unchanged. The original failed
Task remained failed.

Full local `make ci` passed on the complete follow-up batch; no new tests or
architecture allowances were added. The deployed Controller was
`0.0.1-qa-backup-order`, bytes
`sha256:eca53d993676bc461682de4b5f4aea42f6a5adbcf4a817b1acf5af9fdc24d4c8`,
release `sha256:9d9d47d204d230698d5036b825f105e6f7b1dfca2eb39a664b0e387be95187c3`.
The Agent image above was unchanged. Repair preserved application/etcd containers
and immutable Task identity; it is not normal-upgrade proof. The temporary fault
gateway was stopped and its listener independently proved closed. Private
receipts and failed observations remain retained. This qualifies only the selected
PostgreSQL-owner deletion variant, not live Custom hooks or shared/granted
credential variants.

This is not complete Gate B, arm64, all provider/archive/key failures or a release
qualification. No new tests or release tag were added; earlier failures remain
preserved.

## Upstream PostgreSQL patch and backup tooling

**H88 — upstream PostgreSQL backup tooling and patch authority, selected local
variants PASS on 2026-10-04. BACK-16 PARTIAL.**

On Linux amd64, the separate scratch toolbox built against pinned upstream
`postgres:16-alpine`. Its executable inventory verified, and its mounted helper
ran three client probes against an unchanged upstream database container.
A custom-format dump captured a known row; after changing that row, restore
returned its original value and the post-restore verification succeeded.
The isolated container and data Volume were removed afterward. No QA host or
existing application/database was changed.

The runtime transition regression accepts a new database image while preserving
the data Volume, tools and catalogue authority. It rejects changed data/tool
mounts both before publication and at acknowledgement. These are local behavior
checks, not proof of an operator-driven GP patch. The complete GP update journey,
failed-patch recovery, arm64 tooling, and remote Backup/Restore with this new
toolbox remain NOT RUN. Bounded raw evidence is ignored locally.

**H89 — upstream PostgreSQL patch followed by GP Backup/Restore, selected live
amd64 journey PASS on 2026-10-05. BACK-16 and BAK-18 PARTIAL.**

A fresh disposable Ubuntu 24.04 amd64 host ran committed source `92b2b24bf`
through the maintainer installer, version `0.0.1-qa-upstream-92b2b24bf`.
Controller bytes were
`sha256:e0f8eaca64fcc2ac4392d30f26a40ba0a431384c7b70de7eeb18818229b6ba47`;
the Agent image was
`sha256:16a8210b6b6ca7a4c45e4116302ce18dd54f379209f37f143ded4c4438e9ad94`.
The independently authenticated native toolbox was
`sha256:c15a427d9b9e18b1591a2bf88cce3f353c3e67c308e9c1b2ab7a84848259c91b`.
Only that host architecture was built; this is not a published dual-platform
release or a public `install.sh --ref` result.

GP provisioned an unchanged upstream PostgreSQL 16.12 instance and a new
credential-owning Attach. Two distinct known rows were inserted and read back.
GP Fetch selected the exact downloaded PostgreSQL 16.13 Alpine digest, followed
by Service Edit and protected recreate Deploy. The database container and image
changed; every data/tool mount's type, source, destination and writable flag
remained identical. PostgreSQL reported 16.13 and both rows were unchanged.
The database patch Task was `task_01M44FP0DDE2DA0N16A3ST7NTW`.

An isolated S3-compatible store received an age-encrypted GP Backup. Task
`task_01M44FPZ7YE4WFX87VFH7D5AJW` completed and exactly one verified PostgreSQL
Attach Recovery Point appeared; source rows were unchanged. After replacing
those rows with a different value, Restore Task
`task_01M44FQ1M01F3400FGZTCKEBZJ` completed and independent SQL reads returned
the original two rows. The patched database image and exact mounts remained;
the consumer containers retained their identities and returned to running.
Final Host/CLI and container reads showed healthy Controller, Agent, etcd and
PostgreSQL. These assertions check actual data, not only Task completion.

The first run had passed the patch but failed Backup capture before a verified
Point; its Agent subsequently rejected startup staging delivery. That failure's
Task, helper journal and logs remain retained, and Restore was not run there.
A separate early-exit reproduction found that the helper could not record its
owned, unreaped gate using live-process inspection. `92b2b24bf` records its
lifetime before readiness while retaining full identity checks before client
release. The original capture's precise launch cause was not established;
the later successful run does not erase or prove recovery from that incident.
The old disposable installation was reset with approval, not repaired in place.

Current isolated resources remain on the disposable QA host; private receipts
are retained locally. No push, tag, new tests or compatibility path was added.
Existing focused helper checks, vet and architecture pass. Full CI still has
the previously recorded verifier-helper timeouts. Failed-patch recovery,
connection interruption, injected Backup/Restore failures, arm64, public
installation and blanket Gate B/release qualification are not established here.

**H90 — actual Cloudflare R2 Connector Backup FAIL on 2026-10-05;
Restore NOT RUN.**

The H89 candidate and patched PostgreSQL instance were reused with a new
Connector targeting an operator-provided R2 test bucket and a unique prefix.
Authenticated read preflight passed. Backup Task
`task_01M46DNMKQW3MWKCSY6D1XEJCY` uploaded its encrypted artifact, then failed
HEAD verification with `storage.unavailable`. No verified Recovery Point was
published and no Restore or database mutation was attempted.

The retained run records the upload's returned VersionId. A bounded SDK probe
inside the same Agent independently reproduced successful PUT followed by
HTTP 501 `NotImplemented` for VersionId-selected HEAD. ETag-selected HEAD and
conditional DELETE succeeded on that probe's exact owned object. That candidate
preferred a returned VersionId; its successful MinIO journey did not qualify R2.
No provider-selection workaround or production-code change was made in H90.

The previous Backup Policy was restored through the API. The failed Task,
Connector and GP-owned uploaded orphan remain retained for normal product
reconciliation; they were not manually rewritten or deleted. The diagnostic
object and Agent probe executables were removed. Private credentials and raw
receipts remain ignored. This is a demonstrated provider-integration blocker,
not a failed credential check or a passing Backup/Restore result.

**H91 — actual Cloudflare R2 encrypted PostgreSQL Backup and Restore,
selected amd64 journey PASS on 2026-10-05.**

The scoped correction selects a returned ETag before sealing a new upload's
identity. HEAD, GET and protected deletion retain that exact identity; they do
not switch fields after an error. Explicit `NotImplemented` responses are
provider rejections rather than transient unavailability. The affected race
checks, vet, architecture and formatting passed, including an integrated
PUT/HEAD/GET/prune regression with both response fields present.

The disposable H89 host received the candidate through the protected maintainer
updater without a reset. Controller version was `0.0.1-qa-r2-identity`, with bytes
`sha256:91eedbe9e04ffe939942008a653f1336dffa681fecfb8cff9a70757bc8224191`;
the Agent image was
`sha256:8cebc3880b2b5afaa13e81ea953eb05107444aa1e11c568a45a08bc259461a09`.
The database remained upstream PostgreSQL 16.13 with the same independent
authenticated toolbox. The source changes were not yet committed at build time;
these artifact identities identify the tested candidate.

A new Connector used the operator-provided R2 test bucket and an initially empty,
unique prefix. Backup Task `task_01M46F30B6JC9E9X0WW97TACTJ` completed and published
one verified encrypted PostgreSQL Attach Recovery Point. Independent R2 listing,
HEAD and GET found exactly one object matching its stored size and age envelope;
the source database still contained both original rows.

After replacing those rows with a different value, Restore Task
`task_01M46F34AZMMQAMZ4M7P98AT2H` completed. Independent SQL reads returned both
original rows. Database image and exact mount properties were unchanged; consumer
container identities were retained and running. The R2 object's ciphertext hash
was unchanged after Restore. The original Backup Policy was restored through
the API, and final host observation reported healthy Controller, Agent and etcd.

The H90 failed Task and uploaded orphan remain retained with their original
VersionId authority. H91 does not rewrite that authority or prove reconciliation
of that old incident. Its diagnostic object was removed; the new verified Point
and isolated fixture remain available for inspection. This run does not qualify
live R2 retention/deletion, multipart, Config/Volume sources, injected failures,
arm64, public installation or a blanket Backup/Restore release gate. No push or
tag was made. Full CI retains the previously recorded verifier-helper timeouts.

**H92 — Backup Console key controls, captured storage and automatic refresh,
selected Ubuntu amd64 journey PASS on 2026-10-05. BAK-14/19 PARTIAL.**

The protected maintainer update loaded `0.0.1-qa-backup-console` without a reset.
Controller bytes were
`sha256:b950fd6412c59487c6eb8d05bbd0d0b3b4bb7765656414dcb4c7656185e529c1`;
the Agent image was
`sha256:a545d74abd43855e6075faa91abb25c474a9c7b51a77245340d03e4c690e82a9`.
The source edits were uncommitted at build time, awaiting signing-key unlock.
Console build, regenerated API clients, architecture, vet and the existing HTTP
private-data redaction check passed. No new rendering or static contract test
was added; the column-string-only CLI table test was removed.

Before the fix, the deployed Settings page disabled Rotate and claimed no key,
although API metadata reported era 1. After the fix, directly opening Settings
loaded that metadata and enabled the key controls. No existing key or policy was
changed during verification of that page.

In a separate disposable Environment, browser rotation Task
`task_01M46JH887DV7NEDR22DS9X40F` completed. Settings showed a different public
recipient and era 2 without reloading. Three browser-triggered Config Backups
created distinct verified Points, first using Connector A and then Connector B.
Each new row appeared automatically. With keep=2, the newest B Point and preceding
A Point remained; their displayed bucket/prefix and API snapshots matched their
respective original destinations rather than the current policy's Connector.
CLI JSON on the pre-existing fixture also returned each Point's captured storage.
The affected browser reported no console errors or warnings.

The fixture's first direct Entry setup returned HTTP 409 before mutation. Its
retained receipt was continued through the ordinary initial Blueprint Apply,
after which the Entry was accepted. The first refusal's precise cause was not
investigated; this run does not repair or qualify that separate path.
The complete isolated Environment,
Points and Connectors were subsequently removed through GP's normal protected
removal, preserving the pre-existing PostgreSQL data, policies and keys.
Final host observation reported healthy Controller, Agent and etcd.

Raw receipts and the original rejected setup remain ignored locally. More than
50 Points, hidden-tab resume, injected read failures and blanket release/CI
qualification were not established here. No push, tag or compatibility path.

**H93 — Manual Recovery Point deletion, selected Ubuntu amd64 R2 journey
PASS on 2026-10-06. BAK-20 PARTIAL.**

The tested Controller was `0.0.1-qa-point-delete-20261006-r2`, bytes
`sha256:3d7feae3487a4d357f38175d6b3b1678b9445770be022d02ec31e412346e66b5`.
The final `r3` delivery additionally hides unsupported Abort in the deletion
dialog, with bytes
`sha256:a604b76ee69eea0d9cb2534c6e3bbd67c0671cb191d876eb92038a4093eb8e0f`.
Both used the unchanged H92 Agent and GP's normal protected Controller updater.
These artifact identities identify uncommitted source builds.

One fresh encrypted Backup produced five verified test Points: Config, two
Volumes and two credential-owning PostgreSQL Attaches. Independent authenticated
R2 HEAD requests confirmed the exact captured object identities before deletion.
GP's bodyless API, CLI `backup remove-point` and Console confirmation then removed
only those test Points. Independent R2 HEAD returned 404 for all five archives;
the original five Points, Backup Policy, Volume IDs and running container IDs
were unchanged. The Console linked its Task and refreshed the list without a
page reload. No VM reset, direct provider deletion or source removal was used.

Replaying the completed API request returned its original Task after its Point
was gone. Selecting another Point under that key returned `idempotency.mismatch`.
A focused storage regression proved that a Restore admitted after selection
rejects deletion atomically, without hiding the Point or publishing a partial
Task. Existing prune lifecycle/failure checks and selected race checks passed.
API generation, Console build and affected vet checks passed. Architecture still
reports the pre-existing Runner file and application wiring total findings;
root-etcd remains below the approved 75,000-line cap. Full CI was not rerun.

The first live publication exposed a missing accepted response: its Task ran,
but the first HTTP reply was empty. The corrected publication returns its owned
Task response; fresh CLI and Console requests then linked and completed their
Tasks normally. The failed receipt is retained locally. Cleanup uses the existing
system-owned prune lifecycle, not a new operator Retry/Abort contract.

Live remote-deletion fault injection, exhausted retries, arm64 and blanket
Backup/Restore release qualification were not established by this run. Test
artifacts were removed through GP; unrelated archives and private credentials
were preserved. No push, tag or compatibility path was introduced.

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
