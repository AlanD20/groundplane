# Entry retained-runtime mutations — 2026-09-09

Local implementation evidence only. Public HTTP/CLI, real Docker/filesystem
execution, and disposable-host acceptance remain required before floor handoff.

Create, edit and bulk-upsert share a reconstructible Entry procedure. Candidate
artifacts retain runtime ownership, proxy, Release and image metadata. The
Controller selects exposed logical Services; the shared machine/helper selector
runs only their workload instances, with `--no-deps`. Stable proxies are not
Entry consumers. The native-renderer regression proves the selected helper
command, and changed ownership metadata rejects. Acknowledgement records the
materialized Entry generations without replacing an existing applied snapshot's
unrelated workload decisions. This does not prove all pending/unapplied workload
interleavings.

DELETE uses the existing desired revision publisher, not a second publication
path. It seals the desired candidate while publishing the Task, protected
response, applied cleanup intent, tombstone and Environment ownership atomically.
The Entry remains in the visible desired head until successful cleanup. Applied
Entries use a 120-second materialization-only Agent plan; a never-applied Entry
uses a 30-second Controller finalizer with no host work. Success promotes the
candidate and deletes the Entry lookup and subordinate value generations.
Failure keeps desired/applied metadata unchanged and allows a pinned Retry.

The joined test uses the real desired claim/stage/publisher, Task assignment,
plan reconstruction, immutable materialization resolver, Agent worker, transfer
decoder, event journal and terminal acknowledgement. The helper filesystem
effect is faked, and initial applied-Entry setup supplies completion evidence.
Normal completion and retried completion both reach Entry/value absence; root
response and terminal acknowledgement replays remain stable.

Two integration regressions drove further corrections:

- After the root response was expired, staging GC removed the failed cleanup's
  candidate and Retry rejected. A retained original Task now pins its candidate;
  publication advances the sealed descriptor so its existing CAS fences a
  concurrent cleanup decision. The same Retry/Agent/terminal journey passes
  after response expiry. No GC transaction ceiling was raised.
- A never-applied Controller deletion allowed queued Agent materialization to
  acquire the Environment writer before finalization. It now reserves that
  existing writer during publication and protected Retry, verifies its exact
  owner at terminalization, and releases it atomically. The queued Task stays
  unclaimable during deletion and becomes claimable afterward. Successful
  promotion also fences applied state and rejects newly applied target Entries.

Passing focused commands use repository-local Go cache/temp directories,
`-race -count=1`, and the following coverage outputs:

- `.tmp/entry-removal-agent.cover`: real worker completion and Retry.
- `.tmp/entry-removal-retention.cover`: deferred visibility, Controller-only
  completion, failed Retry and response-expiry retention.
- `.tmp/entry-removal-controller-writer.cover`: queued-writer exclusion plus
  Agent completion and retained Retry after the correction.
- `.tmp/entry-retained-integration.cover`: Controller, etcd, desiredrevision,
  and app selection `^Test(EntryMutation|EntryRemoval|EntryDesiredProjection|
  EnvironmentEntryMutationAudit|RepositoryClaimsWinnerAndSealsTypedMutationRevision)`
  (without the displayed line break). This compiles app composition; it is not
  an executed public-handler journey.

`make proto` reproduces both generated Agent files exactly. Changed handwritten
Go files were formatted with module-pinned golines. `git diff --check` passes.
The tagged Controller build and Agent executable build pass. A read-only check
on disposable QA `10.25.0.2` found the Controller active and listed application
workloads healthy; these new binaries have not been installed there.
The architecture gate was run and remains red for deferred structure and
test-placement findings, including the three new external-test imports named
in `docs/issues/deferred-architecture-cleanup.md`. No architecture baseline,
record ceiling, ownership guard or publication limit was relaxed.

Manual Script cleanup, oversized Blueprint Apply, integrated live Volume/Entry
verification and required floor gates remain open. This is neither deployment
evidence nor a completed floor-MVP claim.

## Serving-slot correction after live QA

Live QA found that the desired baseline still selected blue after ordinary
Rollback had made green the serving workload. Task completion alone was not
acceptance. ADR 0073 records the correction: capture the per-Service immutable
runtime at a fixed revision, replace only Entry decorations, seal that runtime
in the existing candidate, and bind its Environment epoch to the ordinary
publication fence. No publication or record ceiling changed.

Three regressions failed before their corrections: missing serving green,
reconstruction against the stale desired baseline, and acceptance after the
capture epoch changed before publication. The actual publisher, immutable
reconstruction, both retained workload selection without proxy/dependencies,
commit-time race rejection, and read-only replay now pass. Existing Entry
projection and removal checks pass. Final formatted race evidence is
`.tmp/qa-floor-20260909/entry-runtime-final.cover`: etcd 3.513s/13.3%,
Controller 1.740s/11.8%; app composition compiles with no selected tests.
The two pre-existing oversized app files did not grow (931 and 625 lines).
No Agent, protobuf, Console, CLI, or public API contract changed.

The tagged Controller builds as `0.0.0-qa.floor20260909.5`, SHA-256
`1f19750ba5ac40b736343aaf6f8098d9e4e899e65c64a6c2552caae1c3002413`.
The live serving-container proof follows. The earlier live failure is retained
in the integrated QA report; this local correction is not yet a floor claim.
