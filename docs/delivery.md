# Delivery contract

## Change slices

Deliver one reviewable concern per commit. A useful slice leaves a coherent
contract and can be reverted without undoing neighboring work.

Use concise conventional messages:

```text
docs: define MVP implementation contract
refactor: simplify component registry
feat: port prototype console design
ci: enforce Go quality gates
```

Commits use the repository owner's configured identity. Do not add AI
co-author trailers or tool attribution.

### Canonical continuous lane pipeline

`docs/agents.md` defines the only lane state machine and role exit conditions:
`Ready -> Writing -> Stopped -> Frozen -> Reviewing -> Approved -> Landed`,
with one optional `Correcting -> Delta-verifying -> Approved` path. Every
independent feature gets one durable repository-local worktree and branch with
disjoint owned files; independent writers run in parallel. Repository worktrees
and all repository-managed temporary state use ignored repo-local `.tmp/` or
`.tmp-*` paths. This includes scratch, build caches, Go temporary/cache paths
(`GOTMPDIR` and `GOCACHE`), evidence, generated staging, scripts/tests, and
shell intermediates. Do not use system `/tmp` for repository workflow, and do
not use shell heredocs when the shell or runtime spills them to `/tmp`.
External tool-internal sandbox mounts outside repository control are not
repository paths and cannot be depended on. No writer, reviewer, or correction
agent edits a shared worktree or updates `main`.

Freeze only after implementation and the named focused proof. Only the
dedicated integration-owner agent creates one signed direct-child candidate
from the latest `main`; the head never performs candidate construction. Each
lane receives one full blocker-only review, with all findings returned in one
aggregate report. If blockers exist, the original writer receives one bounded
batch containing all of them; the correction reruns only exact tests covering
the correction delta. At most one delta-only verification follows, and it does
not become a new full review. Non-blockers are documented in `docs/issues/` and
do not hold the lane.

After approval or a passing delta verification, only the dedicated
integration-owner agent lands the signed direct child with `git merge --ff-only
<candidate>` after explicit land authorization from the head; the head never
performs ff-only landing. The integration owner records the evidence, confirms
the exact tree on `main`, and then removes the durable worktree and branch. The
head consumes the exact commit/tree/proof/blocker handoffs and decides approval;
the integration owner does not reinterpret contracts or decide approval. If a
writer, reviewer, correction, or delta invocation
reaches its recorded turn, wall-clock, or token budget, it stops. Same `HEAD`
plus same blocker twice, or two invocations without new authoritative evidence,
trips the root-owned circuit breaker. Root diagnoses and chooses a bounded fix,
a smaller lane, or a product-contract question; no replacement or identical
review loop is spawned. Broad gates remain integration-owner work at active
MVP-journey closure and final acceptance.

After compaction or restart, the head agent MUST perform the pipeline-restoration
sequence in `docs/agents.md` before repository work: read `docs/head.md` in full,
then read only exact authoritative contract sections needed for the current
decision, query actual agent status, reconcile durable lanes, advance every
stopped lane without waiting for unrelated work, and refill all eligible
contract-closed disjoint writer slots. Delegate prompts name their task-scoped
docs; no agent rereads broad project docs by default. Only running agents are active capacity.
Review starts per lane only after that lane's writer stops and its candidate is
frozen; after head approval and explicit land authorization, integration-owner
landing and head capacity refill happen immediately per approved lane.
Restoration is not complete until the occupancy target in `docs/agents.md` is
met. Every frozen candidate receives a reviewer subagent, ordinary writing and
correction remain delegated, and the head stays on scheduling, contract
decisions, adjudication, reviewer assignment, and explicit authorization. The
integration owner performs signed-candidate construction and ff-only landing.

### Landing blockers and deferred issues

Block a slice only for one of these findings:

- a compile or build failure intrinsic to the code;
- a violation of an authoritative product or API contract;
- a security failure or secret exposure;
- data loss or corruption;
- a destructive lifecycle error;
- a concurrency race;
- a broken primary operator journey.

Record every other finding in `docs/issues/` and continue the fast-forward
landing. This includes non-blocking defects, cleanup, ergonomics, architecture
polish, additional edge cases, and environment-only toolchain gaps covered by
equivalent evidence. Each issue records its owner, severity, evidence,
acceptance criteria, and whether the MVP requires it.

Do not defer a landing blocker through an issue record. A deferred issue does
not waive any requirement in `mvp.md`.

## Verification ladder

Testing is continuous delivery evidence, not the last delivery phase. Use the
smallest proof that can fail for the current change, then expand the proof as
dependencies become available:

1. Run a focused unit, component, contract, or package test while implementing
   the behavior.
2. Run the focused test again on the stopped writer worktree before constructing
   the signed candidate.
3. Run the affected integration tests on the signed candidate after the required
   judgment reviews approve it.
4. Exercise the real Console, CLI, API, Agent, or host path as soon as the
   required vertical slice can run.
5. Run broad architecture and full-repository gates only on the integration
   candidate that closes an active MVP journey and on the final MVP candidate;
   run the relevant operator verifier on the final candidate.

Use failures at the earliest rung to drive correction and bounded refactoring.
Do not postpone an executable claim until final acceptance. Pre-review checks
guide implementation and candidate construction. After review, rerun only the
exact proof and affected integration tests named by the candidate or correction
delta; do not repeat unrelated suites. Only post-review checks on the approved
candidate count as delivery evidence. Reviews inspect both the implementation
and the available evidence; they do not infer runtime correctness from source
inspection alone.

## Required gates

Run from the repository root unless a command says otherwise:

```sh
make ci
```

`make ci` is the local mirror of GitHub Actions. It verifies exact Node
`24.19.0` and npm `11.17.0`, runs `npm ci`, builds and verifies the Vite output,
then runs tidy, formatting, `make architecture-check`, repository-pinned
Staticcheck 2026.1, tagged vet and race tests, and the tagged production
Controller build. The architecture gate enforces ADR 0056's import direction,
module placement, interface and conversion rules, generated provenance, and
non-growing oversized-file baseline. Its final release smoke removes
`console/dist/` and proves
the compiled production asset wiring still serves the exact Vite bytes while
the API namespace remains API-owned. It then builds the digest-input-pinned
Agent OCI image and asserts its entrypoint, empty command, root identity,
Docker CLI 29.1.3, and Compose 2.40.3 runtime. `make generate` regenerates protobuf,
the committed OpenAPI document, and both generated clients before compilation;
CI fails on any drift. `make controller-dev` is the explicitly assetless
development build; release automation uses `make controller` only.

`make agent-image` builds `groundplane-agent:dev` by default. Release
automation supplies both `AGENT_VERSION` and `AGENT_IMAGE`, pushes that build,
and writes only the registry-reported RepoDigest into `controller.yaml`.
Neither a mutable tag nor the local image id is a valid runtime identity.

The Console lockfile must report no known vulnerabilities at delivery time.
Use supported dependency versions; do not resolve conflicts with `--force` or
`--legacy-peer-deps`.

## Review checklist

- Product vocabulary matches `mvp.md`.
- The Console/CLI/API 1:1 rule still holds for every operator-facing
  Controller capability; only the closed exceptions in `api-cli.md` are
  outside the manifest.
- Old contracts and compatibility layers are removed.
- Stable ids remain references; slugs remain renamable labels.
- Desired state contains decisions only.
- Console modules follow the seams in `agents.md` and remain responsive.
- Public or expensive architectural choices have an ADR.
- Interfaces are consumer-owned and justified by actual variation, a
  side-effect seam, a process port, or a standard-library contract.
- Implementations return concrete types; no local interface is followed by a
  concrete-recovery assertion or unsafe conversion.
- Capability behavior is not added to `internal/app`, the flat etcd mechanics
  package, the root Console store, or an oversized feature page.
- Registered Components import only `component-sdk` and approved stdlib,
  declare exact capability grants, return typed immutable intents, and add no
  implementation-named Controller/API/persistence/Agent path.
- Component code receives no backend aggregate, repository, CLI, host handle,
  secret plaintext, raw Task payload, or arbitrary execution authority.
- Existing oversized files do not grow; extractions reduce caller knowledge
  rather than creating pass-through modules.
- Generated artifacts were regenerated rather than hand-edited.
- Focused, integration, and real-surface evidence exists at the earliest rung
  supported by the changed behavior.
- Commits are focused, concise, and contain no co-author trailer.

## Delivery state

The MVP is not release-ready until the L2 acceptance scenario in `mvp.md`
deploys, rolls back, and backs up the reference topology without handwritten
operational scripts. Passing unit checks alone does not imply production
readiness. Resolve every open issue marked `MVP-required: yes` before declaring
the MVP goal complete. Early slice landing does not weaken this final audit.

## Disposable QA hosts

An owner-designated QA host may be reset or wiped without repeated approval
while executing the accepted Groundplane verification journey. The current
authorized host must be named in `docs/status.md`. This authority is limited to
that disposable host and its Groundplane test state; it never extends to source
worktrees, non-Groundplane data, or a production host. Prefer a bounded repair
when it is faster and preserves useful evidence, otherwise reprovision cleanly
and record the destructive action in the acceptance evidence.
