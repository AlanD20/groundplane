# Groundplane

Self-hosted control plane: run many projects on one machine. One backend
(Controller), three frontends (Console, CLI, API), one Agent, one host in
the MVP.

The repository contains the product definition (`docs/`), the production
Console (`console/`), and the Go Controller, Agent and CLI.

## Context docs — read the one that matches your task

- **docs/README.md** — documentation owners, feature format and migration rules.
  Read it before creating, moving or deleting documentation.
- **docs/features/** — operator capabilities, actions, constraints and limits.
  Use the feature index in `docs/README.md`; each guide links its design owner.
- **docs/mvp.md** — the authoritative product contract. Reach for it on
  any product, model, or locked-decision question, and BEFORE changing
  product or Console behavior. Missing implementation does not narrow this contract.
- **docs/api-cli.md** — API/CLI conventions and authoritative source locations.
  Reach for it on command or endpoint work. The 1:1 rule: every operator-facing
  Controller capability has exactly one Console action, CLI command, and API
  endpoint; local tooling and machine bootstrap are explicitly exempt.
- **docs/blueprint.md** — the spec-file grammar (desired state). Reach
  for it on any spec/desired-state/YAML work. The one law: specs are pure
  inputs of decisions — nothing derived, nothing non-reproducible.
- **docs/architecture.md** — the implementation contract: Go layout,
  adapter and closed Component Capability boundaries, OpenAPI, frontend stack.
  Reach for it when planning implementation.
- **docs/standards.md** — Go rules (import matrix, one
  error type, the subprocess Runner and banned patterns). Reach
  for it when writing Go code.
- **docs/agents.md** — primary-owned delivery, bounded delegation, task-context
  requirements and the source-of-truth map. Read its root-cause repair policy before
  diagnosing or fixing a defect, and its execution policy before changing multiple
  layers, moving a contract, or delegating repository work to subagents.
- **docs/delivery.md** — the delivery and commit contract. Reach for it
  before committing, opening a PR, or declaring a change complete.
- **docs/capabilities.md** — the current implementation and qualification gaps.
  Reach for it before selecting or declaring an MVP slice complete.
- **docs/decisions/** — grouped technical decisions and rationale, with code
  pointers. Read the relevant topic before changing its seam; accepted design
  does not imply implementation. Deferred decisions are explicitly labeled.

## Context recovery (MUST)

After context compaction, a session restart, or head-agent replacement, every
agent MUST read the ignored local `docs/head.md` if present, then verify its
checkpoint against Git and the current user instructions. If absent, recover
from those sources; a fresh clone does not require a checkpoint. Read only the exact
authoritative contract sections needed for the current decision. A delegate
starts with the task-scoped docs named in its prompt and retrieves additional
relevant sources when needed; it does not reread broad project docs by default.
Keep local checkpoints current when work, blockers or next actions change;
never stage them. `docs/head.md` is an operational checkpoint only; the named
authoritative contracts remain the source of truth. Read dated acceptance
evidence only when it is relevant to the current task.

## Execution

**Testing scope:** do not write tests for straightforward implementation. Tests
are for complex logic, interacting actions or substantial edge cases. If uncertain,
ask the user before writing or running tests. Follow `docs/delivery.md`'s testing
policy and its existing release-gate boundary.
**No static contract tests:** do not test source text, declaration shapes or
generated schema/type inventories. Justified tests must exercise behavior.

Follow `docs/agents.md`'s execution policy. The primary owns verified delivery
to `main`; delegation is optional and bounded. Current user task instructions
override workflow defaults and skill guidelines, subject to higher-priority
instructions and permission boundaries. Use sufficient task context, not arbitrary
context cuts; apply the policy's progress limits to primary and delegated work.

## Console and API ownership

`console/` is the production React 19 + Vite SPA consuming the Controller API.
Product authority stays in `docs/mvp.md`; wire sources and client generation
follow `docs/architecture.md`'s API contracts. For Console feature placement,
follow `docs/agents.md`'s Console module seams. The root store is implementation,
not product or wire authority.

## Non-negotiables

- Shared product rules belong to `docs/mvp.md`, feature behavior to its operator
  guide, syntax to `docs/blueprint.md`, and design rationale to its decision topic.
  Keep them consistent; do not duplicate contracts, code inventories or task
  journals. Record implementation discrepancies without weakening requirements.
- The 1:1 rule always holds for operator-facing Controller capabilities — no
  Console action without a CLI command and API endpoint, and vice versa. Local
  process/tooling commands and machine bootstrap surfaces are exempt exactly
  as listed in `docs/api-cli.md`.
- Replace superseded contracts cleanly. Remove old nouns, routes, files,
  and adapters in the same change; do not add compatibility layers.
- Registered Components compile only against the public Component SDK. They
  consume generic typed Groundplane Capabilities, never `internal/**`, CLI,
  repositories, etcd, Docker, filesystem, secret plaintext, or arbitrary
  execution. The MVP has no runtime/custom plugin loading.
- Slugs are renamable labels (scoped uniqueness); ids are the stable
  references. CLI takes slugs, `--id` opts into ids.
- Never edit generated artifacts: `proto/` Go code and OpenAPI-derived
  clients are regenerated, and the Console's `dist/` output is untracked.

## Repository-local temporary state

Repository worktrees and all repository-managed temporary state use ignored
repo-local `.tmp/` or `.tmp-*` paths. This includes scratch, build caches, Go
temporary/cache paths (`GOTMPDIR` and `GOCACHE`), evidence, generated staging,
scripts/tests, and shell intermediates. Do not use system `/tmp` for repository
workflow, and do not use shell heredocs when the shell or runtime spills them
to `/tmp`. External tool-internal sandbox mounts outside repository control are
not repository paths and cannot be depended on. Resolve exact cleanup targets
and preserve the existing safety boundaries around destructive actions.

## Commands

- Install Console dependencies: `bash scripts/repo-env.sh npm --prefix console ci`.
- Typecheck and build the Console: `bash scripts/repo-env.sh npm --prefix console run build`.
- Commands above run from the repository root with validated local temporary paths.
