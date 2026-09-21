# Agent workflow

## Task authority

Current user instructions select the work and execution mode. Product documents
answer product questions; they do not authorize extra work, live mutations or
a different validation scope.

Read `AGENTS.md`, the current ignored checkpoint if present, and only the
contracts needed for the task. Compare the checkpoint with Git and current
instructions. A checkpoint or old QA report cannot grant operational permission.

When a decision needs user input, ask with the facts, recommendation and tradeoff.
Pause dependent work; continue only independent authorized work. Resolve factual
questions from source before asking. Silence is not approval.

## Source-of-truth order

1. [Product scope](mvp.md): shared behavior and vocabulary.
2. [Feature guides](README.md#features): feature-specific requirements and limits.
3. [Blueprint](blueprint.md): authored syntax.
4. [CLI and API](api-cli.md): human-surface conventions.
5. [Architecture](architecture.md) and [standards](standards.md): module and coding rules.

[Technical decisions](decisions/README.md) explain these choices, not a competing
product hierarchy. An accepted but unfinished design does not authorize enabling
it. Generated clients and frontend state do not define the wire contract.

## Root-cause repair

Preserve the failure and distinguish facts from hypotheses. Trace the broken
assumption through its producer, stored state and consumers. Include confirmed
affected workflows in the authorized scope, not speculative improvements.

Implement the smallest coherent correction in the owning module. Remove the
superseded path instead of adding caller-specific workarounds. Ask before changing
a contract that needs a product decision. Never weaken checks, rewrite immutable
history or discard failure evidence to manufacture success.

A reset or manual repair may restore service, but does not prove product-owned
recovery. Record the mitigation and remaining limitation. When testing is warranted,
use an independent expected outcome; two readers of the same wrong state can agree.
Follow [testing policy](delivery.md#testing-policy), not a test-per-fix rule.

## Execution policy

### Primary-owned delivery

Work on `main`. The primary owns integration and Git mutations. Stage exact
coherent changes and use signed commits with the owner's identity, without AI
co-author trailers. Preserve unrelated and unfinished work. Report implementation,
verification and deployment separately.

Land a ready slice before starting more delegated work. A delegate handoff is not
delivery. A blocker holds its affected work, not unrelated completed changes.

### Optional parallel implementation

Delegate only when a bounded independent task saves enough total effort to justify
context and integration costs. Default to `gpt-5.6-sol` / `xhigh`, with at most two
active delegates. Another model or wider parallelism requires a user decision.
Delegates neither spawn agents nor create branches or commits.

Use disjoint file ownership in the same checkout. Supply the outcome, exact files,
relevant contracts, input/output boundaries, prior failures, smallest appropriate
proof and an effort bound. Sufficient context matters more than an arbitrary
short prompt. Retrieve missing sources for a named uncertainty, not the entire
repository history.

An additional worktree requires a stated isolation need, owner, exact path and
landing/cleanup condition. Share safe caches; do not duplicate dependencies per
agent. Remove a task-owned worktree only after its changes are integrated or
explicitly preserved.

### Progress limits

Stop repeating an approach after the same unexpected failure occurs twice
without new evidence, or when its effort bound is reached. Return the exact
blocker and pending changes. The primary takes over, narrows the task or asks
for the missing decision; do not reset the loop under a new agent.

Inspect a handoff once and send at most one bounded correction batch. Extra review
needs a specific high-risk uncertainty. These limits apply to the primary too:
extra effort needs a concrete requirement or correctness/security/data/reliability
blocker and a new hypothesis. Cosmetic churn and repeated passing checks are not
progress. Do not expand scope to finish optional polish.

### Recovery

After compaction or restart, reconcile the checkpoint, Git and actual delegate
state before resuming. Keep `docs/head.md` current and ignored. Replace its current
summary rather than appending a journal. Resolve ready integration and stalled work
before starting new tasks.

## Repository-local temporary state

Use ignored repo-local `.tmp/` or `.tmp-*` for scratch, worktrees, evidence,
generated staging, Go temporary/cache paths and shell intermediates. Do not use
system `/tmp` or shell heredocs that spill there. External tool-internal sandbox
paths are not repository storage.

### Supported tooling invocation

Make recipes initialize paths through [repo-env.sh](../scripts/repo-env.sh).
For direct commands use `bash scripts/repo-env.sh COMMAND [ARG...]`.
Keep overrides inside the checkout's validated temporary roots. Reuse existing
caches and remove only exact task-owned targets when safe.

Documentation work requires no Go, Node, Docker or browser setup. Executable
commands and version pins belong to manifests and scripts, not copied instructions.

## Console module seams

Follow [architecture](architecture.md#console): primitives, shared presentation,
feature modules, shallow routes and a composition-only workspace store.
Use the existing design system and React Router directly, with no parallel
prototype or framework compatibility layer.

## Documentation discipline

Follow [documentation rules](README.md#documentation-rules). Keep operator
instructions and engineering rationale purposeful and separate. Link to code
rather than narrating it. Retain current limitations, not stale work plans.
Private deployment identities, transcripts and agent checkpoints stay ignored.
