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

## Verification ladder

Select proof for the changed behavior before implementation. Run the smallest
meaningful check first, then affected package/integration or operator-surface
checks as the change becomes executable. Documentation-only changes use consistency,
link and diff checks unless they alter executable behavior or a required gate.

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

`make ci` is the local mirror of GitHub Actions. It includes `make deployment-check`
for the private bootstrap/staging/protected-update client and verifies exact Node
`24.19.0` and npm `11.17.0`, runs `npm ci`, builds and verifies the Vite output,
then runs tidy, formatting through module-pinned `golines`,
`make architecture-check`, module-pinned Staticcheck 2026.1, tagged vet and
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

`make agent-image` builds `groundplane-agent:dev` by default. Release
automation supplies both `AGENT_VERSION` and `AGENT_IMAGE`, pushes that build,
and uses only the registry-reported RepoDigest. Initial bootstrap records it in
`controller.yaml`; routine native deployment pins it in the release manifest.
Neither a mutable tag nor the local image id is a valid runtime identity.

The Console lockfile must report no known vulnerabilities at delivery time.
Use supported dependency versions; do not resolve conflicts with `--force` or
`--legacy-peer-deps`.

## Review checklist

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
- Commits are focused, concise, and contain no co-author trailer.

## Delivery state

Gate A is disposable-host acceptance of the minimum hosting floor in `mvp.md`.
It proves the named generic resource, networking, TLS, routing, replicated
WebSocket/Valkey, group migration, lifecycle, logs, and Task paths with
host-local workload images. It is not first production cutover, does not claim
all MVP capabilities, and does not require every capability-ledger row to be
Accepted. Under the working assumption that an existing external builder
supplies the host-local images, GP-managed Runner provisioning is not a Gate A
or Gate B prerequisite. Owner confirmation remains pending, and the detailed
Runner contract remains subsequent work rather than being silently narrowed.

Gate B is Gate A plus production MVP operations: complete backup, verified
restore to each original surviving target, and retention for every actual persistent
source, including Valkey once its safe source contract exists. The full
relevant CI, Console/CLI/API parity, generated-artifact, security, build, and
operator-surface gates remain mandatory. Full empty-host DR and a second
selected private-workload acceptance are post-MVP, not Gate B prerequisites.
Passing Gate A or unit checks alone does not imply production readiness.
Triage issues marked `MVP-required: yes` for floor relevance before Gate B;
resolve every floor safety, data, parity, security, and exercised destructive-
path issue. Unrelated roadmap issues and unexercised capabilities do not become
floor prerequisites. Early slice landing does not weaken this final audit.

## Disposable QA hosts

An owner-designated QA host may be reset or wiped without repeated approval
while executing the accepted Groundplane verification journey. The current
authorized host and any active pause must be named in `docs/head.md`; follow
those current limits. This authority is limited to
that disposable host and its Groundplane test state; it never extends to source
worktrees, non-Groundplane data, or a production host. Prefer a bounded repair
when it is faster and preserves useful evidence, otherwise reprovision cleanly
and record the destructive action in the acceptance evidence.
