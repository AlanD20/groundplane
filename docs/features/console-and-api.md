# Console and operator API

## Purpose and scope

Expose one Controller product through Console, CLI and REST. `console/` is the
only maintained UI: a React/Vite static SPA using React Router directly. A
second prototype, Next.js runtime or navigation shim would duplicate behavior
and undermine parity, so none is retained.

## Functional requirements

- Every operator-facing Controller capability has one Console action, CLI
  command and API endpoint, subject only to the closed exemptions in
  [api-cli.md](../api-cli.md). A generated type does not by itself prove parity.
- The Controller owns validation, decisions and sequencing. UI presentation and
  request state belong to focused feature modules; route files compose them.
  The root store is shared workspace/navigation composition, not product or
  wire authority. Existing oversized feature logic is debt, not a template.
- Typed Controller handlers/schema emit OpenAPI and derived CLI/Console clients.
  Regenerate artifacts rather than editing generated output or defining a second
  handwritten wire model. [mvp.md](../mvp.md) owns required product behavior.
- Production serves the exact Vite build from the Controller binary without
  Node or an adjacent asset directory. Canonical SPA navigation must work, while
  API and operational paths never fall through to HTML.
- Loading, failure, unavailable evidence and action progress are explicit.
  Fixtures cannot make an unavailable Controller or unobserved workload look
  healthy. Desktop and mobile interactions must represent the same capability.

## Non-functional requirements

Preserve accessibility, responsive layouts, deterministic API errors, protected
mutation replay and safe secret handling. Reject noncanonical static paths before
router normalization; never serve arbitrary disk content. Fingerprinted assets
and non-fingerprinted HTML have distinct cache policies. Build from pinned
toolchains and the lockfile, with generated-output drift rejected.

## Technical design

| Concern | Current technical contract |
| --- | --- |
| Typed REST boundary and generators | [API generation decision](../decisions/0002-api-generation.md) and [wire authority](../architecture.md#api-contracts-locked) |
| Concrete generated artifacts and operation identity | [Human API artifacts](../decisions/0027-generated-human-api-artifacts.md) |
| Closed parity exceptions | [Surface parity](../decisions/0006-surface-parity-boundary.md) |
| Protected request canonicalization | [Request identity](../decisions/0019-idempotent-request-canonicalization.md) |
| Embedded assets, exact routing, MIME and cache behavior | [Static Console delivery](../decisions/0017-static-console-packaging.md) |
| HTTP startup/shutdown ownership | [Server lifecycle](../decisions/0014-http-server-lifecycle.md) |
| UI module placement | [Console seams](../agents.md#console-module-seams) |

### Shared presentation

The Console uses one plum/lavender palette with dark and light themes. Semantic
tokens live in `console/src/index.css`; feature pages do not define another
palette. Shared controls, tables, dialogs and drawers live in `components/ui`.
Shell navigation and breadcrumbs preserve workspace and project context.
Focus outlines are thin, transitions respect reduced motion, and clicking
outside a dialog or drawer dismisses it.

CodeMirror provides the shared document editor and read-only preview. YAML,
JSON and shell have syntax highlighting. YAML/JSON formatting is an explicit,
local action using lazily loaded Prettier parsers; it never saves or applies a
document. Unsupported languages remain plain text. Controller validation and
protected apply requests remain authoritative.

The zone map repeats each Service in its actual memberships, always includes
No zone, and highlights shared membership on selection. Selecting it again clears
the highlight. A zone index supports wide topologies; the list view filters and
sorts loaded Services without changing Controller state. Runtime badges retain
their observation expiry rules. Service and Environment logs retain their live
Controller stream, bounded buffer and explicit stop controls.

### Verification design

The intended automated stack is Vitest for unit checks, React Testing Library
for component/interaction tests, and Playwright for committed production-build
browser scenarios. This is not the current implementation; see status below.
Interactive diagnosis
uses an available isolated browser; it does not require configuring a specific
agent tool for a docs change. Browser acceptance runs against the built SPA,
not only Vite development mode.

Typechecking alone cannot prove action wiring, accessibility or responsive
navigation. Test request/projection behavior, validation and critical mutation
journeys. Cover route shapes at desktop and mobile widths. Select checks through
[delivery.md](../delivery.md#verification-ladder); unchanged passing evidence
does not require an automatic new review cycle.

## Acceptance

Prove generated parity without drift; one action per operator capability; safe
errors and replay; accessible interaction and responsive routing; and a tagged
production binary serving exact assets after the build directory is unavailable.
Test canonical paths, API precedence, GET/HEAD, HTML negotiation, security/cache
headers and development-only assetless failures against the static contract.

## Current status

The production SPA and API-first embedded packaging have recorded qualification.
That does not qualify every feature action. Remaining root-store extraction,
feature parity, generated/full checks and unfinished runtime observations remain
in [capabilities.md](../capabilities.md) and the scoped issue/task lists.

Committed frontend tests currently use Node's test runner and compile-time
TypeScript assertions. The [existing-test review](../qa-matrix.md#console-review)
retains meaningful local behavior and static contract checks; no committed
Console browser/interaction suite was found. Source-text matches were removed
where they did not prove the claimed behavior. Past isolated browser checks
remain bounded evidence, not a substitute for the missing interaction suite.
