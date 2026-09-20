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

### Integration ownership

Follow [the execution policy](agents.md#execution-policy) for primary-owned
integration, optional bounded delegation, shared-checkout coordination and
recovery. The primary lands each verified, coherent slice on `main` before
starting more delegated work. No separate integration agent, mandatory reviewer
chain or repeated approval cycle is required.

### Landing blockers and deferred issues

Block a slice only for one of these findings:

- a compile or build failure intrinsic to the code;
- a violation of an authoritative product or API contract;
- an applicable repository-standard violation introduced by the slice;
- a security failure or secret exposure;
- data loss or corruption;
- a destructive lifecycle error;
- a concurrency race;
- a broken primary operator journey.

Applicable repository-standard violations caused by the slice must be corrected
before landing; optional preferences are not standard violations. Record actionable
out-of-scope findings in `docs/issues/` with owner, evidence and acceptance criteria,
then continue delivery. Do not expand the slice for speculative polish or reopen
unchanged passing evidence. Existing debt and deferred qualification stay explicit.

Do not defer a landing blocker through an issue record. A deferred issue does
not waive any requirement in `mvp.md`.

## Testing policy

Straightforward implementation does not require testing. Do not write tests or
add test gates for basic rendering, static pages, simple wiring or other direct
implementation without complex behavior. Do not run dedicated test suites or
create QA-matrix cases merely because such code changed. Ordinary code inspection
and a user-requested visual preview are not reasons to build a test suite.

Reserve tests for convoluted logic, several interacting actions, or substantial
edge cases. Name the concrete behavior and failure cases that justify each test;
coverage percentages and a blanket test-per-change rule are not justification.
Static source-text and contract-shape tests are prohibited: do not assert source
strings, regex matches, declaration structure or generated schema/type inventories.
Exercise actual behavior when a test is justified. Compiler, generation-parity
and architecture gates remain separate from these tests.
When uncertain whether testing is needed, ask the user before writing or running
tests. This policy applies to fixes as well as new implementation and takes
precedence over generic skill defaults and the verification ladder below.

Test removals must stay within the requested cleanup scope. Keep tests with a
concrete complex-behavior or edge-case rationale; ask about uncertain removals.
Existing release/integration gates still run at their established milestones,
not after every straightforward change. Explicit user-requested product QA
remains in scope.

## Verification ladder

First apply the testing policy above. When tests are justified, select proof
for the changed behavior before implementation. Run the smallest
meaningful check first, then affected package/integration or operator-surface
checks as the change becomes executable. Documentation-only changes use consistency,
link and diff checks unless they alter executable behavior or a required gate.

[The product QA matrix](qa-matrix.md) records each behavioral case and its
independently observable result. Add or update the affected case before executing
a new product test, including a regression or fault probe. Map automated behavioral
tests and manual journeys to case IDs; mechanical build/lint checks remain separate.
Existing unmapped tests are unverified coverage, not implicit passes. Qualification
runs use [per-case records](acceptance.md#case-run-record), with explicit unrun,
blocked and failed cases. A green build, matching internal projections or completed
Task alone cannot qualify a product outcome.

The primary inspects each completed slice and its proof. Reuse passing results
when the tested inputs and relevant dependencies are unchanged; a commit id change
alone does not require a rerun. Pause affected writers for consistent integration
checks. A relevant code change, failing check, dependency change or unresolved
concrete concern justifies rerunning the affected proof, not an automatic full
suite or new review round. Follow the execution policy's progress/effort bounds.

A bounded slice is implemented when it is coherent, satisfies its applicable
contracts and standards, passes its required focused checks and is committed on
`main`. That is not full qualification or deployment. Run broad architecture and
full-repository gates at active-journey integration and final release acceptance;
exercise the relevant real operator paths as soon as safe resources are available.
Record the exact unavailable-resource or pre-existing blocker and deferred proof.
A known safety or correctness failure still blocks dependent work.

## Required gates

For integrated-journey and release qualification, the required root target is:

```sh
make ci
```

Make applies the [repository environment](agents.md#supported-tooling-invocation)
to every recipe and recursive invocation. The CI Go-cache path matches it.
`make tooling-check` is the focused, host-free path and helper-safety gate;
full CI runs those checks once through deployment and verifier-helper targets.

Default `make ci` does not invoke sudo or execute privileged host tests. It runs
`make backupstage-host-acceptance-compile` to compile the mount acceptance test
without executing it. The real mount and root-ownership checks remain available
through the explicit `make backupstage-host-acceptance` command, for a separately
authorized disposable, root-capable Linux test machine. These checks are not
silently skipped or reported as passing. Privileged Backup qualification remains
deferred with Backup/Restore and does not block the current release.

`make ci` is the local mirror of GitHub Actions. It includes `make deployment-check`
for the private bootstrap/staging/protected-update client and verifies exact Node
`24.19.0` and npm `11.17.0`, runs `npm ci`, builds and verifies the Vite output,
then runs tidy, `make format-check` across repository Go source roots (excluding
ignored scratch and caches), using `gofmt` and module-pinned `golines` with an
explicit `gofmt` base formatter rather than an ambient editor's `goimports`,
`make architecture-release-check`, module-pinned Staticcheck 2026.1, tagged vet and
race tests, and the tagged production
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

The Protobuf compiler is pinned by `.protoc-version`, independently of the Go
plugins pinned in `go.mod`. `make proto` rejects a different compiler before
generation. CI downloads that exact official compiler archive and verifies its
pinned SHA256 instead of installing a distribution's `protobuf-compiler` package.
Update the archive checksum with any compiler pin change and regenerate normally;
never hand-edit generated version headers or waive generated-file parity.

### Approved architecture debt for 0.0.1

The owner deferred structural cleanup on 2026-09-19. Release CI runs the strict
checker, prints every finding, and permits only the 105 existing findings in
`architecture-deferred.json`: 55 file-size findings, two frozen-total findings,
and 48 test-only import findings. Matching uses path, rule, subject and exact
message, not source line positions. New or changed findings, extra occurrences,
invalid reports and checker failures block CI. Resolved findings need not remain.
The snapshot must not be regenerated to accept new debt without owner approval.

`make architecture-check` remains the unmodified strict check; its original
baseline and architecture requirements are unchanged. A passing release gate
does not mean this debt is fixed or architecture compliance is qualified.
This exception is for the 0.0.1 release; revisit it before qualifying a later
release. Build, tests, race detection, vet, Staticcheck, security, generated-file
parity and artifact checks remain mandatory. See the
[deferred cleanup record](issues/deferred-architecture-cleanup.md).

`make agent-image` builds `groundplane-agent:dev` by default. Release
automation supplies both `AGENT_VERSION` and `AGENT_IMAGE`, pushes that build,
and uses only the registry-reported RepoDigest. Initial bootstrap records it in
`controller.yaml`; routine native deployment pins it in the release manifest.
Neither a mutable tag nor the local image id is a valid runtime identity.

The public packaging entrypoints are `scripts/release-agent.sh`,
`scripts/release_bundle.py` and root `install.sh`; their exact build/install
commands and trust boundary are in [deployment.md](deployment.md#prebuilt-releases).
Host-free archive and branch-routing checks run in `make deployment-check`.
Publishing artifacts and executing an installer require their own selected target
and authority. Local script proof does not qualify fresh-host installation,
native-update traffic continuity or the second supported architecture.

The Console lockfile must report no known vulnerabilities at delivery time.
Use supported dependency versions; do not resolve conflicts with `--force` or
`--legacy-peer-deps`.

## Review checklist

- Bug fixes satisfy [root-cause repair](agents.md#root-cause-repair): the causal
  explanation, affected workflows and any required regression proof agree; mitigation is not
  reported as resolution.
- Product vocabulary matches `mvp.md`.
- The Console/CLI/API 1:1 rule still holds for every operator-facing
  Controller capability; only the closed exceptions in `api-cli.md` are
  outside the manifest.
- Old contracts and compatibility layers are removed.
- Stable ids remain references; slugs remain renamable labels.
- Desired state contains decisions only.
- Console modules follow the seams in `agents.md` and remain responsive.
- Significant shared or expensive-to-reverse choices have an ADR; ordinary
  feature design is kept in its living feature document.
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
- Changed behavioral tests map to documented QA cases; execution records identify
  the tested build, required variants, independent assertions, outcome and cleanup.
- Each test has a concrete rationale; removals follow the matrix's test-review
  rule and leave any lost or missing behavioral proof explicit. Coverage metrics
  are not a delivery target.
- Commits are focused, concise, and contain no co-author trailer.

## Delivery state

[mvp.md](mvp.md#acceptance-gates) owns Gate A hosting acceptance and Gate B recovery
acceptance. The [capability index](capabilities.md) records implementation and
qualification separately for each feature.

A landed slice, a passed gate and complete MVP qualification are different
outcomes. Report only the scope actually proved. Required CI, Console/CLI/API
parity, generated-artifact, security and failure-recovery checks remain mandatory.
Missing capabilities retain their own accepted requirements; early slice landing
does not waive them or authorize deployment.

## Disposable QA hosts

An owner-designated QA host may be reset or wiped without repeated approval
while executing the accepted Groundplane verification journey. The current
authorized host and any active pause must be explicit in the current user
instructions; an optional local checkpoint can record them but cannot grant
authority. If the target or permission is unclear, ask before mutating it.
This authority is limited to
that disposable host and its Groundplane test state; it never extends to source
worktrees, non-Groundplane data, or a production host. Prefer a bounded repair
when it is faster and preserves useful evidence, otherwise reprovision cleanly
and record the destructive action in the acceptance evidence.
