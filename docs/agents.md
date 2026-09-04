# Agent workflow

## Workstation and agent setup

Install these prerequisites before changing the repository:

- Go at the version declared by `go.mod` (currently Go 1.26);
- Node.js with npm for the Console toolchain;
- `protoc` for Agent protocol changes;
- Docker Engine with Docker Compose v2 for runtime and Agent work;
- current stable Chrome for Console interaction and browser verification.

Bootstrap a checkout from the repository root:

```sh
go mod download
go install github.com/segmentio/golines@v0.12.2
cd console && npm ci
```

Keep `$(go env GOPATH)/bin` on `PATH` so `make ci` can invoke the pinned
`golines` binary. GitHub Actions installs the same version before running the
identical gate.

Do not install `protoc-gen-go` or `protoc-gen-go-grpc` globally. Their exact
versions are Go 1.26 tool dependencies in `go.mod`, and `make proto` resolves
those repository tools explicitly instead of selecting generators from
`PATH`. The `protoc` compiler itself is not repository-pinned yet.

The fixture-backed Console and the current Go scaffold require no local
secrets or `.env` file. Never place deployment credentials in the repository.

### Chrome DevTools MCP for Codex

Console work requires browser evidence, not source inspection alone. Register
Chrome DevTools MCP in Codex with an isolated headless browser so it works in
agent environments without an X server and never reuses a collaborator's
normal browser profile:

```sh
codex mcp add chrome-devtools -- \
  npx -y chrome-devtools-mcp@latest --headless --isolated
```

If `chrome-devtools` already exists with different arguments, replace it
cleanly rather than keeping a second server definition:

```sh
codex mcp remove chrome-devtools
codex mcp add chrome-devtools -- \
  npx -y chrome-devtools-mcp@latest --headless --isolated
```

Start a new Codex session after changing MCP configuration; MCP tools are
negotiated when a session starts. Check registration with:

```sh
codex mcp get chrome-devtools
```

Registration alone is not a functional check. In the new session, have the
agent call the Chrome DevTools `list_pages` tool. Setup is complete only when
that call returns an open page. `Missing X server` means `--headless` is
absent; an interactive `npx` prompt means `-y` is absent.

Do not use `--browser-url` against a personal Chrome profile for routine
project work. A remotely debuggable browser can expose every open page and its
authenticated state to local processes.

## Start from the contract

1. Read the task-specific document named in `AGENTS.md`.
2. Locate the existing Console store action, CLI command, and REST endpoint.
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

## Repository-local temporary state

Repository worktrees and all repository-managed temporary state use ignored
repo-local `.tmp/` or `.tmp-*` paths. This includes scratch, build caches, Go
temporary/cache paths (`GOTMPDIR` and `GOCACHE`), evidence, generated staging,
scripts/tests, and shell intermediates. Do not use system `/tmp` for repository
workflow, and do not use shell heredocs when the shell or runtime spills them
to `/tmp`. External tool-internal sandbox mounts outside repository control are
not repository paths and cannot be depended on. Resolve exact cleanup targets
and preserve the existing safety boundaries around destructive actions.

## Multi-agent delegation

The head agent (normally root) owns minimum context, contract decisions,
lane/model selection, reviewer assignment, blocker adjudication, and explicit
land authorization. It consumes the exact `Commit`, `Tree`, `Proof`, and
`Blockers` handoffs. It MUST NOT perform broad source or diff reading when a
bounded delegate can supply the exact evidence.

The head assigns one dedicated integration owner. The integration owner works
in an isolated integration worktree and owns candidate construction, focused
proof mechanics, signed direct-child creation, `git merge --ff-only` onto
`main` after explicit authorization, and landed cleanup. It MUST NOT decide
approval or reinterpret contracts. Writers, correction agents, and reviewers
MUST NOT update `main`, rebase or merge `main`, or edit a shared worktree.
The integration owner updates `docs/head.md` in the same landing commit when
`main`, active lanes, blockers, or next actions change.

### Context-recovery pipeline restoration

After every context compaction, session restart, or head-agent replacement, the
head agent MUST restore the live pipeline before selecting or implementing
repository work:

1. Read `docs/head.md` in full, then read only the exact authoritative
   contract sections needed for the current decision. Delegate prompts name
   the task-scoped docs each delegate must read; no agent rereads broad
   project docs by default.
2. Query the agent runtime and classify every delegated agent by its actual
   status. Only running agents count as active; completed, failed, interrupted,
   or blocked agents do not count as working capacity.
3. Reconcile the runtime result with every durable repository-local worktree,
   branch, frozen commit/tree, proof, and lane state recorded in the handoff.
   Unlanded source-bearing work is preserved.
4. Advance each stopped lane immediately: freeze it, start its one independent
   review, send the one aggregate correction batch, or land it, according to
   its current state. A lane never waits for unrelated writers or a human status
   request.
5. Refill every freed slot with the next contract-closed, disjoint `Ready`
   writer lane. Run all eligible independent writers concurrently, up to the
   available capacity; never manufacture overlap merely to increase the count.
6. Dispatch every eligible review, correction, and bounded contract-scoping
   action to a subagent. The head agent MUST NOT remain the only running worker
   while any such action or disjoint `Ready` writer lane exists.
7. Report the actual running occupancy by role: writers, reviewers,
   corrections, and contract scopers. Completed or stopped agents are reported
   separately and never included in that count.

Pipeline restoration is complete only when the number of running delegated
agents equals `min(free delegated slots, eligible delegated actions)`, or when
the handoff records the exact dispatch failure preventing that equality. An
eligible delegated action is one of: writing a contract-closed disjoint lane,
reviewing one frozen candidate, applying its one correction batch, or closing
one bounded contract question required to make a blocked MVP lane writable.
Speculative audits, duplicate reviews, and overlapping writers are not eligible
actions.

The head agent's steady-state job is minimum-context coordination: close
contracts and file ownership, select models, dispatch writers, assign reviewers,
freeze candidates, adjudicate blocker reports, authorize landing, and refill
capacity. It does not leave completed agents idle while contract-closed work
remains, and it does not report completed agents as active progress. Reviewers
start only after their corresponding writer has stopped and the exact candidate
is frozen.

The head delegates ordinary implementation, independent review, correction
batches, bounded contract analysis, and all integration-owner mechanics. A
feature defect returns to its writer; the head does not silently become a
replacement writer, reviewer, or integration owner. The head decides approval
and land authorization from the bounded handoffs but does not repeat a review.

Capacity is scheduled continuously, not in waves. When a writer stops, start
that lane's reviewer immediately and, when another disjoint `Ready` lane
exists, start its writer without waiting for the review. When a reviewer
approves, the head adjudicates the report and explicitly authorizes landing;
the integration owner then performs the landing mechanics. When it rejects,
the head sends the single aggregate batch to the original writer while
unrelated writers and reviewers continue. Use all
available slots that have eligible work; an arbitrary agent-count target never
justifies overlapping ownership or invented work.

Every independent feature has exactly one durable repository-local worktree and
one branch, with one writer and an explicit disjoint file ownership list. The
worktree is created inside the repository area and remains available through
review and landing. Independent lanes run continuously in parallel.
After explicit head authorization, the integration owner lands each approved
lane immediately, then the head assigns the freed writer capacity to the next
`Ready` lane.

This is the one continuous lane pipeline for delegated work:

```text
Ready -> Writing -> Stopped -> Frozen -> Reviewing -> Approved -> Landed
                                      |                  ^
                                      +-> Correcting -> Delta-verifying -+
```

### Lane states and exit conditions

1. **Ready.** The head selects one disjoint landing-blocker or MVP slice and
   records its authoritative contract, exact owned files, earliest focused
   proof, and exit condition. Resolve questions in the source-of-truth order
   below. A conflict or missing decision keeps the lane `Ready` and returns the
   next action to the head; agents MUST NOT guess, widen scope, or invent a
   compatibility path.
2. **Writing.** Assign exactly one writer to the lane's durable worktree and
   branch. The writer changes only its owned files, implements the closed
   contract, runs the named focused proof, makes a focused commit, and stops.
   Writers for disjoint lanes may run concurrently. Its handoff contains only
   `Commit`, `Proof`, `Blockers`, `Remaining`, and `Progress`; `Progress`
   records the exact `HEAD`/tree, proof result or delta, blocker fingerprint or
   delta, correction-batch count, and turn/time/token usage. A writer does not
   review its own lane.
3. **Stopped.** Start integration only after the writer stops. The integration
   owner preserves the writer worktree, runs hygiene and the exact named proof,
   then transplants the lane onto the latest `main` in the isolated integration
   worktree. A failing landing proof is included in the one correction batch; it
   does not start a second review loop.
4. **Frozen.** Freeze only after implementation and the focused proof have
   completed. The integration owner constructs one signed feature commit that
   is a direct child of the current `main`, records its exact commit and tree,
   and makes that candidate immutable. No writer or reviewer edits a frozen
   candidate.
5. **Reviewing.** Review exactly once per lane, after the candidate is frozen.
   One independent, fresh-context reviewer is normal. Use two independent blind
   reviewers only for security, secrets or credentials, concurrency, destructive
   lifecycle, or irreversible behavior. The reviewer performs one full
   blocker-only review and returns one aggregate report naming every finding,
   its blocker classification, and the exact candidate commit/tree. Reviewers
   MUST NOT delegate, edit the candidate, or reinterpret an unresolved contract.
   A report may mention non-blockers, but only the blocker list can hold the
   lane.
6. **Correcting.** If the aggregate report contains landing blockers, the
   head sends one bounded correction batch containing all blockers to the
   original writer. The writer resumes its isolated worktree once,
   applies the batch, reruns only the exact focused proof affected by the
   corrections, commits, and stops. The integration owner builds the replacement
   signed direct-child candidate. There is no fresh full review after a
   correction; the candidate goes to `Delta-verifying` at most once.
7. **Delta-verifying.** The integration owner verifies only the correction
   delta and the exact tests named by the failed proof or changed behavior.
   Unchanged review evidence is retained. A passing delta verifies the lane;
   a new blocker or failed exact test returns to the head, which owns the
   circuit-breaker diagnosis and next action rather than starting another
   review or correction batch.
8. **Approved.** A lane with no blockers, or one that passes its single delta
   verification, is eligible for head approval. The head grants approval from
   the exact review and proof handoffs. The integration owner records the exact
   post-review proof on the exact candidate. Broad architecture and
   full-repository gates are integration-owner work: run them when a candidate
   closes an active MVP journey and on the final MVP candidate, never as a
   writer's per-lane gate.
9. **Landed.** After explicit head land authorization, the integration owner
   lands the approved signed direct child immediately with `git merge --ff-only
   <candidate>`, records the evidence, confirms that `main` contains the
   candidate tree, and only then removes the lane's worktree and branch. The
   next ready disjoint lane uses the freed capacity. Cleanup never removes an
   unlanded WIP.

### Progress circuit breaker

Before each writer, reviewer, correction batch, and delta verification, the
owner records a bounded turn, wall-clock, and token budget. An invocation that
reaches any bound stops; it receives no identical continuation. Keep prompts
tight: exact contract, owned files, proof command, and handoff fields only.
Progress exists only when the handoff records at least one of:

- a changed candidate tree or `HEAD`;
- a new exact focused-proof result;
- materially new landing-blocker evidence.

Each handoff records `HEAD`, tree, exact-proof result or delta, blocker
fingerprint or delta, correction-batch count, and turn/time/token usage. Token
consumption and narrative effort are not progress. The same `HEAD` with the
same blocker twice, or two invocations without an authoritative state or
evidence change, MUST interrupt the lane. A lane has at most one correction
batch and one delta-only verification. At the circuit-breaker cap, the head
owns the diagnosis and next action; it MUST NOT spawn a replacement or restart
identical instructions. The original writer owns correction, while the
integration owner executes only the bounded mechanics the head authorizes. The
head may define a smaller disjoint lane or ask the human only for genuine
product-contract ambiguity. A broad gate failure outside the lane's owned
scope routes to its owning lane or issue.

This efficiency guard never waives a landing blocker, contract requirement,
focused proof, review, or final gate. Run only exact tests required by the
current proof or correction delta; do not spend a correction cycle on
unrelated test reruns.

If `main` moves before landing, the integration owner reconstructs the signed
direct-child candidate from the new `main` and refreshes head-sensitive exact
proof evidence before the immediate landing. Retain the review judgment only
when the candidate tree is unchanged; otherwise the root agent decides whether
the changed candidate is a material contract change or can use the one allowed
delta verification. Never create a merge commit or rewrite existing `main`
history.

Non-blocking findings go to `docs/issues/` with an owner, severity, evidence,
acceptance criteria, and MVP requirement; they do not hold the lane. Do not
defer a landing blocker through an issue record.

Use `gpt-5.6-luna` with `xhigh` reasoning by default for bounded tasks. Supply
the required repository paths and contract context explicitly rather than
relying on an unbounded conversation fork. Use the fallback below when its
quality threshold has been crossed.

ADR 0056 applies before delegation. The primary owner closes the capability
module, concrete input/output types, consumer-owned side-effect seams, file
ownership, and acceptance evidence before dispatch. Subagents may implement or
review that contract; they do not introduce interfaces, move behavior across
layers, add compatibility paths, or reinterpret the architecture. Overlapping
writers are forbidden, and work from an unclosed seam returns to architecture
instead of being integrated through assertions or adapters.

ADR 0061 additionally governs every registered Component change. A writer may
touch a registered integration only after the head agent closes its provided
capabilities, grants, typed config, immutable planning inputs/intents,
management ownership, generic Agent procedure, and earliest proof. Registered
code imports only `component-sdk` and approved stdlib; it never receives a
backend package, CLI, repository, etcd/Docker/filesystem handle, secret value,
raw Task step, or arbitrary command. A new implementation cannot introduce a
technology-named Controller/API/persistence/Agent path.

Treat model selection as an observed quality decision. Switch subsequent
delegated work to `gpt-5.6-sol` with `high` reasoning after either one critical
miss or two material misses across three consecutively reviewed Luna tasks. A
material miss is a documented invariant missed after it was included in the
task context, incorrect transaction or replay reasoning, a cross-layer
inconsistency, substantial review rework, or a test claim unsupported by the
test. A critical miss is a material miss in a wire or public contract,
persistence/state-machine invariant, authentication or secret boundary,
destructive lifecycle, or ADR-level architectural seam.

For critical work in those same areas, the second blind reviewer has no
implementation context, regardless of the implementation model. The integration
owner owns all post-review evidence; a writer's test report never substitutes
for that evidence.

## Source-of-truth order

1. `docs/mvp.md` owns product behavior and vocabulary.
2. `docs/blueprint.md` owns desired-state grammar.
3. `docs/api-cli.md` owns the human API and CLI mapping.
4. `docs/architecture.md` owns module seams and runtime placement.
5. `docs/standards.md` owns enforceable Go rules.
6. `console/src/lib/store.tsx` owns the current frontend/API interaction surface.

Resolve conflicts upward in that order. Never preserve a lower-level shape
with a compatibility adapter when its source contract has changed.

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
- `features/`: product-area implementations behind route-level interfaces.
- `routes/`: shallow route composition and parameter resolution only.
- `lib/store.tsx`: the Controller-interface stand-in; behavior lives here.
- `lib/types.ts`: contract types until generated OpenAPI types replace them.

Route modules compose; they do not recreate primitives, contain product-area
implementations, or own cross-route state. Feature modules may contain local
compositions and split independently adjustable sections into focused files.
Extract shared modules when behavior is reused across features. Prefer
composition over option-heavy components.

The production Console uses React Router directly. Next.js imports and
compatibility adapters do not belong in `console/`.

## Documentation discipline

Document rationale and invariants. Let code and manifests describe mechanics.
Add an ADR when changing a framework, process boundary, persistence model,
public interface, or extension seam. Supersede old ADRs; never delete them.
