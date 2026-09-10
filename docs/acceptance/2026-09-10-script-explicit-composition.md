# Explicit setup composition — 2026-09-10

Task4 local implementation checkpoint, not live qualification. No QA host,
Docker daemon, network/provider setting or production state was changed.

## Evidence

- The real Controller Blueprint planner validates a mixed explicit/inherited
  two-Service plan. Managed Networks/Volumes precede both pre-hooks; both
  pre-hooks precede either consumer Apply. Setup and consumer image identities
  stay independent. Frozen reconstruction is protobuf-equal. This extends the
  existing global-phase test, not a new planner path.
- The real Agent release executor and Docker Script runtime, against a fake
  engine, preserve Volume preparation → durable Script checkpoints → cleanup
  acknowledgement → consumer Apply. Explicit failure and Abort stop before
  consumer Apply. The engine receives the exact setup projection and only the
  declared Entry generation; assignment values are cleared on completion.
- Lost outcome and cleanup acknowledgements resume from retained checkpoints
  without another create/start. Success, Abort and recovery-invariant failure
  retain their outcome; uncertain runtime evidence retains reconciliation.
- Two temporary Go overlays deliberately remove Entry filtering or successful
  cleanup. The new cases fail for the corresponding isolation/barrier violation.
  These are mutation-test negatives, not failures in the committed runtime.
  The first normal run instead found an assertion comparing against correctly
  cleared fixture data; only that assertion was corrected.

All evidence is under`.tmp/production-mvp-20260910/`:

| Check | Result | Log |
| --- | --- | --- |
| Agent affected Blueprint/explicit/Docker Script runtime race tests | Pass | `script-explicit-agent-final-race.log` |
| Controller Script and Blueprint release preparation race tests | Pass | `script-explicit-controller-final-race.log` |
| Complete Agent/Controller vet | Pass | `script-explicit-composition-vet.log` |
| Unfiltered Entry mutation | Expected failure | `script-explicit-filter-mutation.log` |
| Missing cleanup mutation | Expected failure | `script-explicit-cleanup-mutation.log` |

Pinned Go1.26.7, `-p2 -race -count=1`, repository-local caches and module-pinned
120-column formatting were used. There is no production-code or generated-client
delta in this checkpoint. Earlier source/CAS, image preparation, grammar,
Console/CLI/API and actual networkless Docker-runner evidence remain in the
named Script context reports.

## Remaining qualification

The Agent fixture starts at the received-assignment seam; it is not a full
source-to-host system test. Its engine and acknowledgement persistence are
in-memory doubles. Docker mount/network enforcement, real first Apply and
exact reapply, certificate setup/renewal, migration failure/Abort and reconnect
without duplicate starts remain live task6 checks. Live mutations remain paused
behind the recorded storage-integrity incident. Guarded native trial/write proof
and the known broad CI failures also remain outstanding. Independent task5
bundle and observation work continues; task4 is not fully accepted.
