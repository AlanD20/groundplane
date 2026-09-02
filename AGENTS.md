# Groundplane

Self-hosted control plane: run many projects on one machine. One backend
(Controller), three frontends (Console, CLI, API), one Agent, one host in
the MVP.

The repository contains the product definition (`docs/`), the production
Console (`console/`), and the Go implementation scaffold.

## Context docs — read the one that matches your task

- **docs/mvp.md** — the authoritative product contract. Reach for it on
  any product, model, or locked-decision question, and BEFORE changing
  product or Console behavior. "If it isn't in the Console store, it
  doesn't exist in the product."
- **docs/api-cli.md** — the CLI command tree and the REST API. Reach for
  it on any command or endpoint work. The 1:1 rule: every operator-facing
  Controller capability has exactly one Console action, CLI command, and API
  endpoint; local tooling and machine bootstrap are explicitly exempt.
- **docs/blueprint.md** — the spec-file grammar (desired state). Reach
  for it on any spec/desired-state/YAML work. The one law: specs are pure
  inputs of decisions — nothing derived, nothing non-reproducible.
- **docs/architecture.md** — the implementation contract: Go layout,
  adapter and closed Component Capability boundaries, OpenAPI, frontend stack.
  Reach for it when planning implementation.
- **docs/standards.md** — the enforceable Go rules (import matrix, one
  error type, the subprocess Runner, banned patterns, CI gate). Reach
  for it when writing Go code.
- **docs/agents.md** — the repository workflow, source-of-truth map, and
  multi-agent delegation policy. Reach for it before changing multiple layers,
  moving a contract, or delegating repository work to subagents.
- **docs/delivery.md** — the delivery and commit contract. Reach for it
  before committing, opening a PR, or declaring a change complete.
- **docs/capabilities.md** — the implementation ledger and vertical delivery
  order. Reach for it before selecting or declaring an MVP slice complete.
- **docs/status.md** — the compact implementation checkpoint and recovery
  handoff. Reach for it after context compaction or session restart.
- **docs/decisions/** — accepted architectural decisions. Reach for the
  relevant ADR before changing a recorded seam or dependency choice.

## Context recovery (MUST)

After context compaction or a session restart, before any repository work, every
agent MUST read `docs/agents.md`, `docs/delivery.md`, `docs/capabilities.md`, and
`docs/status.md` in full. The integration owner MUST update `docs/status.md` in
the same commit whenever a lane lands or the remaining set changes. `docs/status.md`
is implementation evidence only; `docs/mvp.md` remains the authoritative product
contract.

## The Console store is the API contract

`console/` is a React 19 + Vite static SPA on fixture data. Its store
(`console/src/lib/store.tsx`) is the stand-in for the Controller API, and
the UI mirrors the product exactly. Changing Console behavior changes the
API contract. UI questions are answered here; product questions in mvp.md.

## Non-negotiables

- Docs stay in sync: mvp.md is authoritative; the other docs and Console
  mirror it.
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

- Install Console dependencies: `npm ci` (from `console/`).
- Typecheck and build the Console: `npm run build` (from `console/`).
