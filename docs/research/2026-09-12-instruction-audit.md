# Groundplane instruction audit

Date: 2026-09-12. Baseline: `ed25190e0` on `main`.
Status: baseline findings and proposals, with the dated resolutions below.
Evidence line numbers and original proposals refer to the baseline, not current
policy. This report is not replacement instructions.

## Alignment 1 — 2026-09-12

The owner subsequently approved [primary-owned delivery](../agents.md#execution-policy)
with optional bounded Sol/xhigh implementation, replacing both the mandatory
delegation pipeline and the blanket no-subagent rule in finding 1. The alignment
also clarifies [scoped verification](../delivery.md#verification-ladder), effort
and no-progress limits, and sufficient task context with targeted retrieval.
Applicable standards and qualification gates remain required. Paused feature WIP
remains separate and uncommitted.

## Alignment 2 — 2026-09-12

The owner approved resolving finding 2 in documentation only. `mvp.md` owns
required behavior; [typed Controller sources generate the wire contract](../architecture.md#api-contracts-locked)
and derived clients. [Console feature ownership](../agents.md#console-module-seams)
follows ADR0056; remaining root-store feature logic is migration debt, not authority
or the pattern for new work. Console/CLI/API parity and Controller-owned product/
runtime decisions remain required. No code extraction or runtime change is included.
The remaining findings are proposals except for the cleanup recorded below.

## Documentation migration — 2026-09-12

The owner approved [living feature documents and deletion rules](../README.md),
with plain language and preservation of valid requirements and evidence.
The operational checkpoint now contains current work and blockers, not its
historical journal. Two superseded Volume-removal task snapshots were removed;
the completed floor handoff remains. Four root specs were migrated to the
[feature index](../README.md#features). Historical status, duplicated ledgers,
superseded ADRs and report consolidation still need review. This resolves the
checkpoint part of finding 3, not every stale-document or setup finding.

## Summary

The repository has useful product and safety boundaries, but accumulated workflow
rules conflict with the current owner-approved execution mode and the implemented
Console architecture. The highest-value cleanup is to resolve those conflicts,
shorten recovery context, and give verification one scope-aware contract. Merely
adding more instructions would preserve the contradictions.

The production initiative is not at final cleanup: tasks1–2 have implementation
and recorded QA evidence, tasks3–4 retain deferred live checks, task5 still needs
Service visibility and the portable hosting bundle, and task6 requires CI and
actual-source backup/restore qualification. Task7 is separately approved cutover.
See [the current checklist](../../tasks/todo.md) and
[the operational checkpoint](../head.md). These are recorded results, not a new
live-host verification.

## OpenAI sources

The matching article is [Rethinking skills and prompts for GPT-6 Astra](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra),
retrieved in full from its Markdown representation. It recommends revisiting
accumulated guidance, narrowing skill triggers, using task-relevant document
discovery, and defining completion without unnecessary review stops. This is
guidance to calibrate instructions, not permission to weaken real safety rules.

[Astra model guidance](https://developers.openai.com/api/docs/guides/latest-model#instruction-following)
also recommends explicit precedence for user requests over skill guidelines and
visibility when a skill changes or blocks the task. Its testing section favors
appropriate checks and further reruns when new evidence justifies them.

[Codex instruction discovery](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
explains global/project layering and the default32KiB automatic instruction-file
limit. The root `AGENTS.md` is only5,387bytes; this audit did not demonstrate
automatic truncation. The cost here is the additional reading explicitly required
by the repository, not evidence that this discovery limit has been exceeded.

## Findings

### 1. High — incompatible execution modes

Evidence: [current scope](../../CAPABILITY_MAP.md), lines25–29;
[`docs/head.md`](../head.md), lines9–15; [`tasks/plan.md`](../../tasks/plan.md),
line32, require direct work on main with no subagents/worktrees/review loops.
[`docs/agents.md`](../agents.md), lines97–180 and195–258, instead require writers,
an integration owner, isolated worktrees, reviews and delegated capacity refill.
[`docs/delivery.md`](../delivery.md), lines22–75, repeats that mandatory pipeline.

Impact: recovery can restart a workflow the owner explicitly stopped. Under
single-agent execution, required reviewer/integration roles do not exist, and
the post-review-only evidence rule can make valid local proof appear inadmissible.
The product source-of-truth order at agents.md333–343 does not resolve workflow
override precedence because it does not include the current task/operational mode.

Proposal: one default execution policy in `docs/agents.md`, honoring explicit
task instructions. For the current initiative, one agent works on main and
continues through authorized implementation and verification. Delegation and its
worktree/review mechanics apply only when that mode is explicitly selected.
Make `docs/delivery.md` link to that policy instead of repeating it. Distinguish
instruction precedence from product-document authority.

### 2. High — Console placement and API authority point in opposite directions

Evidence: [`AGENTS.md`](../../AGENTS.md), lines57–62, calls the Console fixture
data and its store the Controller stand-in. [`docs/agents.md`](../agents.md),
line366, says behavior lives in the root store.
[ADR0056](../decisions/0056-mvp-modular-monolith-and-code-quality-architecture.md),
lines257–281, instead puts transport authority in generated OpenAPI types and
request/actions/projection ownership in feature modules. The delivery checklist
explicitly prohibits adding capability behavior to the root store at176–177.
The [actual store](../../console/src/lib/store.tsx) imports generated operation
types at8 and makes real `/api/v1` requests at520. The architecture testing
section still repeats the fixture-store description.

Impact: an agent can faithfully follow the entrypoint and violate the accepted
module contract, or treat an incomplete UI projection as the product definition.

Proposal: describe the current production SPA and remaining migration debt.
Keep `mvp.md` as product authority, HTTP/schema sources as wire authority,
generated clients as derived transport types, and feature modules as behavior
owners. The existing root store is not a template for new feature placement.

### 3. Medium — mandatory recovery reads a historical journal with stale actions

Evidence: [`AGENTS.md`](../../AGENTS.md), lines45–55, requires full `head.md`
reading after recovery. [`head.md`](../head.md) is368lines/25,534bytes. Its
lines164–169 still call explicit execution unconnected and the next increment;
lines233–242 say it is connected and task5 is next. The previous-floor section
also uses present-tense host-health statements at309–316 after a newer storage
incident and mutation pause have been recorded earlier.

Impact: every recovery pays for historical evidence and must reconstruct which
apparently active next action superseded which. The dated previous-floor heading
helps, but does not make these statements a good current checkpoint.

Proposal: retain a short current checkpoint with scope, execution mode, checkout
and deployed identities, blockers, next action and links to proof. Move historical
narrative into existing acceptance records or an explicitly historical archive.
Preserve every incident and deferred check; do not erase evidence for brevity.

### 4. Medium — completion and verification scope are ambiguous

Evidence: [`standards.md`](../standards.md), lines263–286, makes the complete
local gate a prerequisite for any completed task. [`delivery.md`](../delivery.md),
lines104–120, schedules broad gates at journey closure/final acceptance and
discourages unrelated reruns. Its adjacent required-gates section only says
`make ci`. The current initiative separately defers accumulated gates to task6.

Impact: a small documentation/UI change can trigger the full release/image/host
gate; alternatively, an agent can use the narrower rule to understate an actual
release blocker. This is a scope ambiguity, not a recommendation to waive CI.

Proposal: define completion separately for a bounded change, integrated journey,
and qualified release. Keep full CI mandatory for integration/release acceptance.
For small changes, name relevant checks and rerun after changed inputs or failures.
Put the executable gate in Makefile and keep a single explanatory owner in
delivery.md; standards.md links there. Record failing/deferred checks explicitly.

### 5. Medium — supplied verification paths violate temporary-state policy

Evidence: [`AGENTS.md`](../../AGENTS.md), lines83–92, requires repo-local temporary
state. The [C11 guide](../../.agents/skills/verify-groundplane/features/c11-attach-l2.md)
line22 documents `/tmp` as fallback; its [script](../../.agents/skills/verify-groundplane/scripts/c11-attach-l2.sh)
uses it at50. The [network verifier](../../.agents/skills/verify-groundplane/scripts/network-zone-route-etcd.sh)
does so at27, and the [Volume verifier](../../.agents/skills/verify-groundplane/scripts/volume-lifecycle-ssh.sh)
at211. `make ci` also leaves Go-cache/temp placement to the caller and writes
`coverage.out` at the repository root; its documented invocation supplies no
repo-local environment wrapper.

Impact: following the prescribed command with an ordinary environment violates
the written policy. Repeated ad-hoc environment overrides become hidden setup
requirements.

Proposal: align helper defaults and documentation around validated repo-local
runtime paths, preserving existing symlink, scope and cleanup protections. Provide
one supported invocation that configures required cache/temp paths. Decide whether
named build/coverage outputs are explicit exceptions or move them under `.tmp`.
This needs script tests, not a prose-only rule addition. No verifier was executed
and no temporary directory was deleted for this audit.

### 6. Medium — copied contracts increase reading cost and drift

Evidence: [`architecture.md`](../architecture.md), lines443–479 and490–526,
repeat the desired-revision staging/publication mechanics. The multi-agent
pipeline is independently restated across agents.md and delivery.md.
[ADR0056](../decisions/0056-mvp-modular-monolith-and-code-quality-architecture.md),
lines301–320, already calls for one document owner and concise linked mirrors.

Impact: copies can diverge and create competing authority. The root instructions,
workflow, delivery and recovery files alone total63,718bytes, before task-specific
product contracts. This is file size, not a measured token/latency estimate.

Proposal: remove exact duplicate passages and give each rule one canonical owner.
Keep concise product/API/Blueprint mirrors where their audiences need the
decision, with links for implementation mechanics. Do not weaken the 1:1 rule.

### 7. Medium — setup and delegation guidance is insufficiently conditional

Evidence: [`docs/agents.md`](../agents.md), lines5–18, asks for all Go, Node,
protoc, Docker and Chrome prerequisites before repository changes. Its default
delegated model/reasoning is hardcoded at296–299. The root's task-based document
router is otherwise useful and should be preserved.

Impact: a documentation-only task can appear to require runtime/browser setup;
model-specific scheduling is presented alongside stable engineering rules even
when the active workflow forbids delegation.

Proposal: route prerequisites by operation and verify installed pinned versions
before installing anything. Place model selection in the optional delegated
workflow, honoring the user's chosen model and available tools. Keep durable
project invariants model-neutral.

### 8. External — the loaded research skill independently mandates delegation

Evidence: `/home/www/.agents/skills/research/SKILL.md:6` unconditionally directs
creation of a background agent. Its research trigger applies to this very audit,
while the current project mode says no subagents. I performed this audit locally
to preserve the current owner direction.

Proposal: make delegation conditional on task authorization and available useful
parallel work, with a local research path. This file is outside the repository;
a repository-only cleanup cannot fully solve this conflict. Do not copy a
competing local skill merely to mask it. A separate requested global-skill edit
would need to respect that location's write permissions.

## Keep these boundaries

Retain product/Blueprint/API authority, stable-id references, generated-artifact
provenance, Component SDK isolation, real secret-handling rules, preservation of
unrelated work and failed evidence, exact cleanup targets, explicit production/
provider/network approval, and truthful implementation/QA/deployment reporting.
The storage-integrity pause remains valid. The short, task-routed root document
is a useful structure; the problem is conflicting/stale downstream guidance.

## Suggested cleanup sequence

1. Resolve workflow precedence and Console ownership in AGENTS/agents/delivery;
   retain the current main-only initiative mode. Clarify scoped verification.
2. Compact head.md into current state and proof links; remove duplicated prose;
   mark preserved history unambiguously historical.
3. Align verifier/cache defaults with the existing temporary-state rule and run
   their focused tests. Handle the global research skill as a separate change.

Acceptance: after a fresh session, a docs fix should not require Docker or full
CI; a Console change should route to its feature module; an ordinary initiative
task should not spawn a delegation pipeline; a real release must still require
CI/QA; a paused QA mutation must remain paused; verifier runtime defaults must
stay inside the repository. Check these scenarios against the resulting files
and use executable helper tests where behavior changes.

## Audit limits and paused feature work

Inspected the root instructions, workflow/delivery/standards/head, current plan
and checklist, applicable architecture/ADR sections, the project verification
skill and relevant helper defaults, plus the external research skill used here.
The repository inventory found one root AGENTS.md and one project skill, without
a nested AGENTS/CLAUDE override. The local Codex-home AGENTS.md was empty. This
is not an exhaustive audit of every installed global/plugin skill, every product
paragraph, or runtime security. No production/QA state or global configuration
was changed; no repository policy was rewritten by the audit.

On the user's mid-task request, Service observation implementation was paused.
Its draft ADR0077, contract mirrors, protobuf/generated output, pure validation
and state tests, and Docker observer test scaffold remain uncommitted. The pure
package race tests pass; the new Docker observer tests are deliberately still
red because `NewWithEngine`/the implementation do not exist yet. This is unfinished
feature work, not a passing build or an audit finding against the prior commit.
At the original audit checkpoint, no commit, push, deployment, full CI run or
clean-worktree claim accompanied the report. The subsequent documentation-only
alignment preserves that unfinished WIP without including it in the docs commit.
