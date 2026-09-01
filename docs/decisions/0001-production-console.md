# ADR-001: Maintain one production Console

## Status

Accepted

## Date

2026-08-19

## Context

The full Console design began as a Next.js prototype, while the implementation
contract requires a React 19 + Vite static SPA embedded in the Controller.
Maintaining both trees would duplicate routes, fixtures, store actions, and UI
primitives. The two copies would drift and make the 1:1 rule ambiguous.

Groundplane has no public compatibility obligation yet. The clean replacement
rule therefore favors one shipped implementation over a prototype adapter.

## Decision

Promote the designed interface into `console/` and remove `prototype/`.

The Console uses React Router directly. Route modules compose checked-in UI
primitives and the fixture store at `console/src/lib/store.tsx`. No Next.js
runtime, Next navigation shim, or duplicate prototype store remains.

The Controller will embed the Vite `dist/` output and serve `index.html` for
unknown non-API paths so direct SPA navigation works.

## Alternatives considered

### Keep both frontends

Rejected because every behavior change would require synchronized edits to two
stores and two route trees without adding product value.

### Keep Next-compatible navigation adapters

Rejected because the adapter would preserve a discarded framework interface
and make new Console code depend on obsolete conventions such as `href` and
`useRouter`.

### Ship Next.js

Rejected because it requires a Node runtime and conflicts with the locked
static-SPA embedding model.

## Consequences

- `console/` is the only maintained UI implementation and interaction contract.
- Design changes happen once and are visible to future generated API clients.
- Route files use React Router conventions directly.
- The Controller must implement SPA fallback and asset embedding before the
  Console can be served by the production binary.
