# Temporary-output tooling alignment

- Owner: primary agent for the next approved repository-tooling change
- Severity: medium
- MVP-required: yes, before the affected verification or release gate is run
- Evidence: source inspection on 2026-09-12; no verifier or host action executed

## Remaining mismatch

The [repository policy](../agents.md#repository-local-temporary-state) requires
ignored repo-local temporary state. These current executable defaults conflict:

- [C11 Attach verifier](../../.agents/skills/verify-groundplane/scripts/c11-attach-l2.sh)
  uses `GROUNDPLANE_VERIFY_RUNTIME_DIR`, then `TMPDIR`, then system `/tmp`.
- [Network verifier](../../.agents/skills/verify-groundplane/scripts/network-zone-route-etcd.sh)
  and [Volume verifier](../../.agents/skills/verify-groundplane/scripts/volume-lifecycle-ssh.sh)
  use `TMPDIR` with a system `/tmp` fallback.
- [Makefile](../../Makefile) leaves Go temporary/cache paths to the caller and
  writes `coverage.out` at the root for test and CI targets.

Documentation migration does not change these scripts. Set explicit validated
repo-local paths for any independently safe supported invocation; do not treat
an unsafe fallback as an exception. The full gate needs coverage-output alignment
as well. Preserve current symlink checks, exact cleanup ownership, evidence and
pre-existing caches while implementing the fix.

## Acceptance

One supported invocation configures repository-local scratch, Go temp/cache,
verification runtime, staging and coverage output. Default and unset-environment
tests prove that no repository-managed state spills outside ignored local paths.
Existing ownership, symlink, bounded evidence and cleanup tests still pass.
Update the affected verifier guides with their implementation in the same slice.
Do not run host-mutating journeys while the [integrity pause](../head.md) applies.

## External guidance boundary

The September instruction audit also found unconditional delegation in the
external `/home/www/.agents/skills/research/SKILL.md`. Repository execution policy
honors current user instructions over such skill defaults. Changing that global
skill is a separately requested task outside this repository migration; do not
create a competing local copy to hide it.
