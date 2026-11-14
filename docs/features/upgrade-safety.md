# Controller and Agent updates

GP updates must preserve application containers, data, routing and durable Tasks.
The Console can briefly disconnect while the Controller restarts, but it must
recover the result of the same update operation. A successful software update is
not an application redeployment.

## Choosing what to update

Agent and Controller have independent releases. Fresh tagged installation uses
`vX.Y.Z`; subsequent updates use `controller/vX.Y.Z` and `agent/vX.Y.Z`.
Agent-only updates do not replace the Controller. Controller-only updates preserve
the enrolled Agent and requires a compatible channel format. Updating Both
qualifies the prepared Controller and Agent together. A failed pair qualification
restores the predecessor Controller and Agent before reporting the update failed.

Use [installation and updates](../deployment.md) for commands and artifact
selection. An Agent update pins its exact image digest; changing that digest with
the same idempotency key is a conflict.

## Prepare software, then Apply

Software preparation supports Controller-only, Agent-only and Both, each from
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

Use `groundplane software prepare --component controller|agent|both
--source source_ref|release --ref REF`, follow its Task, then
`groundplane software apply PREPARATION_TASK`. Preparation and activation have
independent Task ids. Preserve the exact key and inputs when resolving an
uncertain request. [H97](../acceptance.md#source-software-preparation-and-activation)
records bounded source-build interruption and normal activation evidence.
Published release inputs retain their separate qualification requirements.
See [host-built development versions](../decisions/release-packaging.md#host-built-development-versions).

## Admission and activation

The candidate must be staged, intact and compatible before activation. An unstaged
release is rejected before Task publication. Corrupt staging remains a storage
error, not an ordinary missing candidate.

GP pauses new Agent assignments and drains admitted sends before replacing it.
Busy work is not aborted to make an update proceed. The bounded drain can reject
an update with `resource.in_use`; let the existing work finish before trying again.
Application Scripts and migrations are not replayed during an update.

A rejected preparation releases only its own temporary pause. Removal restrictions
and newer Agent generations remain protected. Reconnect cannot bypass the pause.
When publication may have committed, GP retains the hold until it resolves the
outcome instead of reopening the previous generation.

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
