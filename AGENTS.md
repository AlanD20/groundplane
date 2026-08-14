# Groundplane

Self-hosted control plane: run many projects on one machine. One backend
(Controller), three frontends (Console, CLI, API), one Agent, one host in
the MVP.

There is **no production implementation yet** — this repo is the product
definition (docs/) and the Console prototype (prototype/).

## Context docs — read the one that matches your task

- **docs/mvp.md** — the authoritative product contract. Reach for it on
  any product, model, or locked-decision question, and BEFORE changing
  product behavior or prototype behavior. "If it isn't in the prototype's
  store, it doesn't exist in the product."
- **docs/api-cli.md** — the CLI command tree and the REST API. Reach for
  it on any command or endpoint work. The 1:1 rule: every Console action
  = exactly one CLI command = exactly one API endpoint.
- **docs/blueprint.md** — the spec-file grammar (desired state). Reach
  for it on any spec/desired-state/YAML work. The one law: specs are pure
  inputs of decisions — nothing derived, nothing non-reproducible.
- **docs/architecture.md** — the implementation contract: Go layout,
  extension model (adapters, core components), OpenAPI, frontend stack.
  Reach for it when planning implementation.
- **docs/standards.md** — the enforceable Go rules (import matrix, one
  error type, the subprocess Runner, banned patterns, CI gate). Reach
  for it when writing Go code.

## The prototype is the API contract

`prototype/` is a Next.js console on fixture data. Its store
(`lib/store.tsx`) is the stand-in for the Controller API, and the UI
mirrors the product exactly — changing prototype behavior changes the API
contract. UI questions are answered here; product questions in mvp.md.

## Non-negotiables

- Docs stay in sync: mvp.md is authoritative; the other docs and the
  prototype mirror it.
- The 1:1 rule always holds — no Console action without a CLI command
  and API endpoint, and vice versa.
- Slugs are renamable labels (scoped uniqueness); ids are the stable
  references. CLI takes slugs, `--id` opts into ids.
- Never edit generated artifacts: `proto/` Go code and OpenAPI-derived
  clients are regenerated, and the prototype's `.next/` build output is
  untracked.

## Commands

- Typecheck the prototype: `pnpm exec tsc --noEmit` (from `prototype/`).
  Note: the `lint` script exists but eslint is not an installed
  dependency — do not rely on it.
