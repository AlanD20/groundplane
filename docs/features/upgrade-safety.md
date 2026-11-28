# Software updates

GP updates must preserve application containers, data, routing and durable Tasks.
The Console can briefly disconnect while the Controller restarts, but it must
recover the result of the same update operation. A successful software update is
not an application redeployment.

## Transient execution-storage outages

When execution storage temporarily becomes unavailable, the Controller pauses
new scheduling and Start grants, drains its execution loops, and retains its
process ownership and executor identity. After storage responds, it rechecks
that identity and reconciles already-started operations before admitting new
work. It does not repeat an accepted operation's Execute call to recover a lost
storage response. Reports and reconciliation remain available during the pause.

This keeps a transient execution-storage failure from automatically shutting
down the Controller and local Agent. Corrupt state, changed ownership, a failed
drain or another permanent loop failure still stops recovery. This behavior is
not an offline restore or a bypass for unresolved ownership.

## Choosing what to update

Agent, Controller and Console have independent releases. Fresh tagged installation uses
`vX.Y.Z`; subsequent updates use `controller/vX.Y.Z`, `agent/vX.Y.Z` and
`console/vX.Y.Z`.
Agent-only updates do not replace the Controller. Controller-only updates preserve
the enrolled Agent and requires a compatible channel format. Updating Both
qualifies the prepared Controller and Agent together. A failed pair qualification
restores the predecessor Controller and Agent before reporting the update failed.

Use [installation and updates](../deployment.md) for commands and artifact
selection. An Agent update pins its exact image digest; changing that digest with
the same idempotency key is a conflict.

## Prepare software, then Apply

Controller and Agent preparation supports Controller-only, Agent-only and Both, each from
a published release or an explicit branch, tag or commit. Source builds run in
Docker on the GP host for its architecture; host Go/Node toolchains are not
required. Both resolves one source commit, or the matching component release
tags. Published release inputs download verified artifacts instead of rebuilding.

Controller and Agent build descriptors retain the source kind, resolved commit,
binary hash and channel format. Preparation verifies the captured source and
immutable artifacts without running candidate executables; staging checks that
the selected pair agrees before activation.
Candidate details show that recorded commit for source builds. A candidate is not
the running Controller, even when their readable version labels match.

**Platform Settings → Software updates** retains preparation progress and
Task links. Prepare never activates software. Apply is a separate action using
the verified outputs in GP's registry, pinned by manifest digest. Both completes
both preparations before one coordinated activation. The candidate Controller
replaces the Agent and proves authenticated readiness before either is accepted.
Agent-only updates must match the running Controller's channel format. Select
Both when the channel format changes. Persistent-format changes still require
a separate supported cutover; selecting Both does not bypass storage checks.

Use `groundplane software prepare --component controller|agent|both|console|all
--source source_ref|release --ref REF`, follow its Task, then
`groundplane software apply PREPARATION_TASK`. Preparation and activation have
independent Task ids. Preserve the exact key and inputs when resolving an
uncertain request. [H97](../acceptance.md#source-software-preparation-and-activation)
records bounded source-build interruption and normal activation evidence.
Published release inputs retain their separate qualification requirements.
See [host-built development versions](../decisions/release-packaging.md#host-built-development-versions).

## Independent Console updates

Select **Console only** to prepare and Apply a published `console/vX.Y.Z` release.
The Controller serves the verified static bundle on its existing listener; no
separate daemon or Node runtime is installed. Console-only activation does not
restart Controller or Agent or redeploy applications. Reload to use the new UI.

**Both** still means Controller and Agent. **All** includes Console as well.
Console bundles record the exact API document hash. Incompatible Console
activation is rejected. Controller-only or Both updates must preserve compatibility
with the installed Console; choose All for a matching API/UI change. All stages
matching artifacts before Controller replacement and activates the matching Console
after the new API is available. If Console compatibility fails at boot, the API
remains available for recovery while UI requests fail closed.
If the running API differs from the prepared Console, the Console step records
a failure. Use the CLI or API to prepare and apply a matching Console release.

If a retained preparation lacks the Controller API fingerprint, prepare the
release again from a bundle that includes its OpenAPI document before applying it.

A first upgrade from an embedded-only installation initializes the verified Console
store from its trusted embedded assets. Later updates use its durable selection;
there is no arbitrary asset-directory override. Previously loaded fingerprinted
assets are retained for a bounded period so open tabs can load their old chunks.
Preparing a Console release can reclaim expired bundles when the store reaches
its byte limit. The active release and its immediate predecessor stay available.
Apply checks Console retention capacity during staging, before any component
is replaced. If the retention window is full, retry after the oldest retired
bundles expire.

All can build from a branch, tag, or commit. Groundplane resolves the ref once,
stages the Console built alongside the Controller, and verifies that their API
hashes match before Apply. Choose an exact commit for repeatable QA without
publishing tags. The selected source must support building the paired Console.
Console-only preparation still uses published Console releases.

## Admission and activation

The candidate must be staged, intact and compatible before activation. An unstaged
release is rejected before Task publication. Corrupt staging remains a storage
error, not an ordinary missing candidate.

GP pauses new Agent assignments and drains admitted sends before replacing it.
Busy work is not aborted to make an update proceed. The bounded drain can reject
an update with `resource.in_use`; let the existing work finish before trying again.
Application Scripts and migrations are not replayed during an update.

A same-channel native Controller or coordinated Controller/Agent update can also
proceed with retained Entry configuration recovery, provided its attempts have
finished and a fresh check of the authenticated Agent proves that no local work,
command-capable auxiliary sessions or transient execution helpers remain active.
The check is read-only. Resource claims, captured inputs, failed Tasks and Agent
journals remain available for explicit recovery afterward. A stopped cleanup
label alone does not supply this proof. A coordinated Agent replacement repeats
the fresh check after Controller handoff, before replacing the Agent.

If that candidate never becomes ready, the recovered predecessor Controller uses
the original native update journal and its continuously closed assignment hold
to authorize rollback. It does not wait for an offline candidate to answer a
probe. Recovery rejects work from outside the qualified executor-process generation and
preserves the original failed Entry operation for explicit Retry afterward.

Agent-only activation and other retained operation types still require full
settlement. To repair an Agent while Entry recovery is retained, prepare a
compatible coordinated update with a new Controller build.

An older Agent that cannot answer this check blocks the update before activation.
Installing a candidate does not change the predecessor's admission rules. An
already affected older installation can therefore still require a separately
reviewed repair; this path does not authorize deleting its recovery records.

A rejected preparation releases only its own temporary pause. Removal restrictions
and newer Agent generations remain protected. Reconnect cannot bypass the pause.
When publication may have committed, GP retains the hold until it resolves the
outcome instead of reopening the previous generation.

## Downgrade limits

Matching storage and channel versions do not establish arbitrary downgrade
compatibility. This release reads existing Entry records without migration and
keeps their legacy storage behavior. Once an operator writes explicit Entry
encryption settings, Controller builds predating independent Entry encryption
cannot read those definitions. Do not select such a build as a later update.
The automatic predecessor recovery window ends when the candidate qualifies;
ordinary configuration mutations remain blocked until that transition.

New opt-in listener and remote Agent settings live in companion configuration
files. Older binaries read the primary configuration without those settings.
Qualification of a software update on an existing host leaves the new features
disabled. Before enabling them, verify which new Task and runtime records the
predecessor can read. A successful rollback before activation does not qualify
a rollback after scoped Runners, public ingress or remote placement have
published state.

## Failure and interruption

The durable Task and private update journal retain the operation across Controller
or client interruption. Startup restores unfinished assignment holds before the
Agent channel opens. Follow the original Task or printed resume instructions;
do not delete the journal or replace binaries manually to force progress.

Native recovery uses a compatible predecessor even if the candidate cannot start.
An update that fails and recovers remains **failed**, not successful. Incompatible
persistent formats must be rejected before replacing the running binary.
Healthy process readiness alone does not authorize writes after ambiguous recovery.

Ordinary Controller restart may restart its Agent, but not etcd. A host reboot
necessarily interrupts the machine; the requirement is automatic return to
service without operator repair, not uninterrupted service while powered off.

## Design and qualification

[Release packaging and update recovery](../decisions/release-packaging.md) explains
staging, the independent predecessor guard and durable acceptance. The
[platform runtime](../decisions/platform-runtime.md) explains process ownership.

Historical update checks are in [acceptance](../acceptance.md#historical-evidence-register).
H97's Ubuntu amd64 HTTP/data continuity does not qualify held connections,
other platforms or every failure boundary in the [QA matrix](../qa-matrix.md).
Old incident pauses are not current host instructions.
