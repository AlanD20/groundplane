# Agent workflow

## Workstation tools

Use only the tools needed for the change. Documentation-only work needs no Go,
Node, Docker or browser setup.

- Go changes use the version and tool dependencies in `go.mod`.
- Console changes use the Node/npm versions checked by the repository manifests.
- Protobuf changes also need `protoc`; `make proto` selects the Go generators
  pinned by `go.mod`. Do not install separate global generator versions.
- Runtime and Agent checks need Docker and Compose.
- Changes to rendered Console behavior need an isolated browser check when that
  behavior can run. Use an available browser tool; configure one only if needed.
  Keep personal browser profiles and unrelated authenticated sessions out of tests.

The Makefile and package scripts own setup and verification commands. Follow
[delivery.md](delivery.md#verification-ladder) for the scope of proof. Do not repeat
tool installation or passing checks whose inputs have not changed.
Never place deployment credentials in the repository.

## Start from the contract

1. Read the task-specific document named in `AGENTS.md`.
2. Locate the owning Console feature/action, CLI command, and REST endpoint.
3. State the contract change before editing implementation files.
4. Change the authoritative document and every mirror in the same slice.
5. Name the earliest executable proof for the changed behavior.
6. Run that proof as soon as the behavior can execute, then keep it green while
   the slice evolves.
7. Finish with the delivery gates in `delivery.md`.

A slice is complete when every changed operator-facing Controller behavior has
one Console action, one CLI command, and one API endpoint, or when all three
are absent by design. Local tooling and machine bootstrap qualify only when
they are one of the closed exceptions listed in `api-cli.md`.

## Root-cause repair

Fix the cause and its affected workflows, not just the reported symptom. Before
changing implementation:

1. Preserve the failure and the smallest useful reproduction or diagnostic
   evidence. Separate observed facts from hypotheses; name any missing proof.
2. Trace the failing decision through its producer, stored state, consumer and
   verifier, where applicable. Identify the broken requirement or assumption and
   why existing tests did not catch it. Distinguish requested configuration,
   successfully applied state and live observation; they are not interchangeable.
3. Check the actual callers and workflows that share that assumption. Include
   relevant sequences of operations, not only each operation in isolation. Keep
   this investigation bounded to the affected mechanism. If successive fixes
   expose related failures in other layers, revisit the shared model before
   adding another local patch; follow the existing progress limits.

Implement the smallest coherent correction in the module that owns the decision,
covering every confirmed affected path in scope. Remove superseded logic rather
than adding caller-specific exceptions. A required contract change must be stated
and resolved through the user-decision rule; passing existing code or tests is not
evidence that a conflicting design is correct. Do not weaken validation, discard
failure evidence or rewrite immutable history to obtain a passing result.

Add a regression that fails on the cause and passes with the correction. Exercise
the relevant operation sequence through the owning interface, including prior
successful changes and failure/replay when they affect the result. Then verify
the original operator-visible outcome at the appropriate verification rung.
Two checks derived from the same wrong state can agree; include an independent
expected outcome. A completed Task, healthy process or passing unit test alone
does not prove application recovery.

An authorized restart, redeploy, reset or configuration workaround may restore
service or unblock diagnosis. Label it as mitigation, record what it bypasses
and what remains to fix, and preserve the original failed result. Mitigation
does not close the defect or qualify product-owned recovery. Resume dependent
fault tests only after the required recovery outcome is proved.

Record the cause, affected paths, regression evidence and remaining limits in
the handoff or commit. When a requirement or design assumption was incomplete,
correct its owning feature document or ADR with the approved change so the next
implementation uses the lesson. Keep unresolved findings in the existing issue
record and current checkpoint; do not create a separate document for every fix.

## Repository-local temporary state

Repository worktrees and all repository-managed temporary state use ignored
repo-local `.tmp/` or `.tmp-*` paths. This includes scratch, build caches, Go
temporary/cache paths (`GOTMPDIR` and `GOCACHE`), evidence, generated staging,
scripts/tests, and shell intermediates. Do not use system `/tmp` for repository
workflow, and do not use shell heredocs when the shell or runtime spills them
to `/tmp`. External tool-internal sandbox mounts outside repository control are
not repository paths and cannot be depended on. Resolve exact cleanup targets
and preserve the existing safety boundaries around destructive actions.

### Supported tooling invocation

Make recipes and verification shell entrypoints initialize paths through
[`scripts/repo-env.sh`](../scripts/repo-env.sh). For other local commands, use
`bash scripts/repo-env.sh COMMAND [ARG...]` from the repository root.

Unset `TMPDIR`, `GOTMPDIR` and `GOCACHE` default to `.tmp/tmp`, `.tmp/go-tmp`
and `.tmp/go-cache`. Existing caches are reused, not erased. The privileged
host-test subprocess uses `.tmp/go-cache-root` so it cannot populate the normal
cache with root-owned files. Make coverage goes to `.tmp/coverage.out`; release
smoke binaries use a unique temporary directory.

Overrides must be canonical paths below this checkout's `.tmp/` or `.tmp-*`.
Relative paths are resolved from the repository root. Outside paths, dot segments,
symlink components and non-directory ancestors are rejected before the command
runs; unset a conflicting ambient override to use the defaults. Verifier evidence
and `GROUNDPLANE_VERIFY_RUNTIME_DIR` overrides follow the same rule. Verifiers
retain evidence and remove only their own disposable runtime when cleanup is safe.
`make tooling-check` proves path setup and helper safety without live host actions.

## Execution policy

Current user instructions select the task and workflow, subject to higher-priority
instructions and permission boundaries. They override repository workflow defaults
and skill guidelines. Product-document authority below answers product questions;
it does not authorize extra work or override the user's chosen execution mode.

### Primary-owned delivery

The primary agent owns implementation decisions, integration, verification and
delivery to `main`. It may implement or correct work itself. It alone performs
Git mutations, stages exact completed slices and creates signed commits using the
owner's identity. Delegates make scoped file edits, not branches or commits.

Before starting more delegated work, land any ready, coherent slice whose required
checks pass. A delegate handoff is not delivery: record the commit on `main`,
qualification still required, and deployment status separately. A genuine blocker
keeps its affected slice pending, not unrelated verified work. Preserve unrelated
and unfinished changes; no blanket staging or cleanup.

### User decisions

When progress requires user input, the primary asks a concise question stating
the decision, relevant facts and a recommendation or tradeoff. This includes
unresolved requirements or preferences, material scope choices and new authority.
Resolve factual questions from available sources first; delegates escalate user
decisions to the primary. Pause dependent work until the user answers, preserve
pending changes, and continue only independent, already-authorized work. Silence
is not approval; token limits do not justify guessing a required user decision.

### Optional parallel implementation

Use delegation only when a bounded independent task will save total effort or
elapsed time enough to justify its context and integration cost. Small changes
stay local. Default to `gpt-5.6-sol` with `xhigh` reasoning and at most two active
delegates; this is a ceiling, not an occupancy target. A different model or wider
parallelism requires an explicit user decision. Delegates do not spawn agents.

Before dispatch, close the module boundary, concrete inputs/results, exact file
ownership, relevant contract sections and smallest useful proof. ADR0056 applies;
registered Component work also follows ADR0061's SDK-only boundary. A delegate
returns an unresolved contract question instead of inventing a new seam.

Use the existing checkout with disjoint writer ownership. The primary coordinates
shared contracts and generated outputs; writers format only their own files.
Pause affected writers before integration checks so proof uses consistent inputs.
A committed slice must not depend on another delegate's unfinished changes.

An additional worktree is an exception for a stated isolation need. The primary
records its owner, exact path and landing/cleanup condition before creating it.
Use repo-local temporary paths; do not duplicate dependency installations or
allocate per-agent build caches by default. Share compatible read-only dependencies
and safe caches; serialize tools that would mutate shared dependency state.
After verifying that its changes are on `main`, remove the task-owned worktree
and branch. Preserve or explicitly archive unlanded work, never delete it to
satisfy a cleanup target.

### Token and progress discipline

Each delegate prompt contains only the outcome, owned files, exact required
context, proof, a small effort bound and stop conditions. Use a token bound when
usage is observable; otherwise use an attempt or elapsed-time bound. Avoid full
conversation forks, broad repository maps and repeated history reads. Reuse
verified context and proof while their inputs remain unchanged.

Optimize for sufficient task context, not the shortest prompt. Include authoritative
contract sections, relevant input/output types and code locations, invariants,
dependencies, applicable standards, and prior evidence or failed approaches that
would affect this task. Preserve the rationale behind non-obvious constraints;
a summary is not a substitute for the source that defines an invariant.

Before editing, the delegate checks that it can identify the affected boundary,
required behavior and proof. It may read linked or discovered task-relevant source
to fill a context gap within its effort bound. If the missing context leaves
authority or scope unresolved, report the exact gap to the primary rather than
guessing. The primary supplies the missing source or narrows the task. Expand
context for a named dependency or uncertainty, not by loading the entire history.

Stop after the same unexpected failure recurs twice without materially new
diagnostic evidence, or when the effort bound is reached. Cosmetic edits,
rephrased plans and repeated passing tests are not progress. Report changed files,
proof command/result, and the exact blocker or remaining step; then stop.
The primary monitors progress and interrupts a stalled delegate rather than
relying only on the delegate to notice its own loop.

The primary inspects the change and proof once, and may send one bounded correction
batch for concrete defects. A stalled correction returns to the primary for a
targeted fix, smaller scope or an actual blocker report. Do not restart the same
attempt under a fresh agent or cycle through reviewers. An independent review
requires a specific high-risk uncertainty or explicit requirement that justifies
its cost, with the same bounds.

These limits apply to the primary too. Additional effort is justified only by a
named requirement or critical correctness, security, data-integrity, concurrency
or operator-journey blocker and a concrete next hypothesis. State why the extra
work is necessary and set a new bound; it is not an automatic budget reset.
Use the user-decision rule above when further work requires their input.

Enforce the applicable repository standards; cost control does not waive them.
Separate actual violations from optional polish or speculative edge cases.
Record actionable out-of-scope improvements without investigating or implementing
them as part of the current slice. Verification scope is owned by
[delivery.md](delivery.md#verification-ladder).

### Recovery

After recovery, follow the context instructions in `AGENTS.md`. Check actual
delegate status only if delegated work is recorded or present, reconcile it with
Git and preserve pending changes. Address ready integration and stalled work
before starting new tasks; do not refill slots merely because they are free.
Keep the ignored local checkpoint current under `AGENTS.md`'s recovery policy;
it is not part of a landing commit or a source of operational permission.

## Source-of-truth order

1. `docs/mvp.md` owns product behavior and vocabulary.
2. `docs/blueprint.md` owns desired-state grammar.
3. `docs/api-cli.md` owns the human API and CLI mapping.
4. `docs/architecture.md` owns module seams and runtime placement.
5. `docs/standards.md` owns enforceable Go rules.

Resolve conflicts upward in that order. Never preserve a lower-level shape
with a compatibility adapter when its source contract has changed.
Implementation gaps do not narrow these requirements. Wire sources and derived
OpenAPI/client generation follow [architecture.md](architecture.md#api-contracts-locked);
neither the root store nor a handwritten frontend type defines the API.

Within those shared boundaries, [feature documents](README.md#features) own
feature-specific requirements, design and acceptance. Their linked technical
contracts own exact protocols and schemas not yet implemented in source.
Accepted ADRs record decisions, not a competing product hierarchy. A Proposed
or Deferred section does not authorize implementation; later accepted decisions
may adopt a clearly named subset. Resolve remaining product choices with the user.

## Clean replacement rule

Groundplane has no backward-compatibility requirement before its first public
release. When a contract changes, replace it cleanly:

- remove the old noun, route, command, type, fixture, and file;
- migrate all internal callers in the same change;
- keep one implementation and one documented term;
- record expensive-to-reverse choices in `docs/decisions/`.

Do not add aliases, fallback parsing, dual routes, deprecated wrappers, or
framework-compatibility shims unless a future public migration policy
explicitly requires them.

## Console module seams

- `components/ui/`: reusable interaction primitives; no domain knowledge.
- `components/common/`: shared Groundplane presentation modules.
- `components/shell/`: responsive navigation and workspace chrome.
- `features/<capability>/`: feature request state, actions, projections and
  presentation behind route-level interfaces.
- `routes/`: shallow route composition and parameter resolution only.
- `lib/store.tsx`: cross-feature workspace selection, shared navigation identity
  and feature-provider composition only.
- `lib/api.generated.ts`: derived OpenAPI transport types; regenerate, never edit.
- Feature-owned types describe presentation state, not parallel wire contracts.

These boundaries follow [ADR0056](decisions/0056-mvp-modular-monolith-and-code-quality-architecture.md).
Remaining feature logic in the root store and handwritten wire models are
migration debt, not a template for new work. This authority alignment does not
authorize a broad extraction; keep any later migration scoped to its approved task.

Route modules compose; they do not recreate primitives, contain product-area
implementations, or own cross-route state. Feature modules may contain local
compositions and split independently adjustable sections into focused files.
Extract shared modules when behavior is reused across features. Prefer
composition over option-heavy components.

The production Console uses React Router directly. Next.js imports and
compatibility adapters do not belong in `console/`.

## Documentation discipline

Follow [the documentation rules](README.md) when creating, changing or retiring
documents. Keep feature requirements and design together, link to shared rules,
and use plain language. Preserve valid constraints and useful rationale before
removing superseded material; Git retains its history.

The instruction realignment favors task-relevant context, explicit authority and
observable completion over accumulated workflow rules. Background reading used
in the September 2026 audit: [OpenAI's skills and prompts article](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra),
[model guidance](https://developers.openai.com/api/docs/guides/latest-model#instruction-following)
and [instruction discovery](https://learn.chatgpt.com/docs/agent-configuration/agents-md).
These sources explain the rationale; they do not replace this repository's rules.
