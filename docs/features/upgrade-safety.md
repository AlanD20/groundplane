# Controller and Agent updates

GP updates must preserve application containers, data, routing and durable Tasks.
The Console can briefly disconnect while the Controller restarts, but it must
recover the result of the same update operation. A successful software update is
not an application redeployment.

## Choosing what to update

Agent and Controller have independent releases. Fresh tagged installation uses
`vX.Y.Z`; subsequent updates use `controller/vX.Y.Z` and `agent/vX.Y.Z`.
Agent-only updates do not replace the Controller. Controller-only updates preserve
the enrolled Agent. Updating both performs the Controller operation first and then
the Agent operation. If the second fails, the completed Controller update is not
silently undone.

Use [installation and updates](../deployment.md) for commands and artifact
selection. An Agent update pins its exact image digest; changing that digest with
the same idempotency key is a conflict.

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

Historical update checks are in [acceptance](../acceptance.md#historical-evidence-register). Current source
still needs the candidate-specific upgrade and connection-continuity cases in the
[QA matrix](../qa-matrix.md); old incident pauses are not current host instructions.
