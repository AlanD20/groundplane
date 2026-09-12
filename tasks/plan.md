# Plan: production-usable Groundplane for Kobwnewe

Owner approved 2026-09-10 after reviewing the scope, build order, Script
extension, full Caddyfile templating and existing IPAM decision. Continue until
all authorized work is handled, not just one slice. The index is
[CAPABILITY_MAP.md](../CAPABILITY_MAP.md).

## Outcome

Deploy Kobwnewe through normal GP operations, recover its required persistent
state and upgrade Controller/Agent without interrupting application workloads.
Upgrade safety exists before production relies on GP. A bounded automatic
Console/API reconnect is acceptable; routine SSH recovery is not.

## Decisions and boundaries

- Native Controller owns Agent replacement. Application containers, volumes
  and routing are not redeployed merely because GP updates.
- Stage immutable update inputs, drain active work, retain durable operation
  identity, health-check and recover a failed candidate. Refuse incompatible
  persistent-state/protocol changes before activation.
- Full Caddyfile templates use reserved GP substitutions referencing Routes
  and stable Service upstreams. Validate the complete file before reload.
- Preserve primary-Zone IPAM. LAN reachability is operator-owned networking;
  public Tunnel success is not proof of direct LAN HTTPS.
- Separate a Script's real Service hook association from its execution context.
  Explicit contexts receive only declared resources. Preserve immutable source
  authority, failure visibility and safe replay.
- Keep external image building and intentional Identity isolation proxies.
  No generic Jobs, PKI automation or architecture-cleanup campaign.
- Relevant contracts, Console, CLI, API and regenerated clients move together.
- Deliver small verified commits to main under `docs/agents.md`'s execution policy.
  The primary owns integration; bounded delegation is optional. Preserve unrelated work.
- Autonomous remote work is only on disposable QA `10.25.0.2`. Production,
  upstream RPi, BIP, provider ingress and host firewall changes are not implied.

## Verification and persistence

Use a failing regression first, affected package/integration checks next, and
real operator journeys as soon as available. Required full CI and production
qualification supersede the former floor-only pause. All repository-managed
temporary state stays under `.tmp/`.

If a QA resource is unavailable, record the attempted check, resource blocker
and remaining acceptance condition; continue independent tasks and revisit the
queue at qualification. Never call unexecuted QA passing or waive a known safety
failure. Production cutover requires evidence and explicit target/cutover approval.

## Tasks, risks and open operational inputs

See [todo.md](todo.md). The completed Volume-removal work is recorded in the
[floor handoff](../docs/acceptance/2026-09-09-integrated-floor-qa.md).

The first risk is a rejected update retaining its dispatch fence. Releasing a
pause must not reopen removal/revocation, a newer generation or an uncertain
durable replacement. Native recovery must work when the candidate cannot start;
an old binary cannot safely roll back across incompatible persistent writes.
Scripts own atomic output: application rollback does not undo arbitrary data
changes. Production migration must prevent dual writers/schedulers; switching
ingress back does not reconcile post-cutover writes.

Production host/network scope, data-transfer authority and the final cutover
window remain operational inputs. They do not block local work or disposable QA.
