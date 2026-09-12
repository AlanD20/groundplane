# Private acceptance evidence

## Evidence index

These are dated working records, not current host health, executable next actions
or permission to repeat a mutation. Read the feature document for requirements
and [head.md](head.md) for current authority and pauses. Keep related future proof
in the same evidence document rather than creating a report for each commit.

| Evidence | Scope |
| --- | --- |
| [Repository consolidation](acceptance/repository-consolidation.md) | Landed source, baseline failures, removed worktrees and recovery archive |
| [Hosting floor](acceptance/hosting-floor.md) | Manual management, Volume/Entry/Script journeys and bounded Blueprint publication |
| [Safe updates](acceptance/safe-updates.md) | Native update implementation, QA candidates, continuity failures and recovery limits |
| [Router and visibility](acceptance/router-and-visibility.md) | Full Caddyfile, native validation, retained reload and Route summary |
| [Script execution](acceptance/script-execution.md) | Ordered hooks, explicit contexts, immutable sources and remaining qualification |
| [Storage integrity incident](acceptance/storage-integrity-incident.md) | Disk/AOF failures, bounded cleanup and the unresolved integrity pause |

No record establishes full CI, Gate A, Gate B or production readiness beyond the
scope it explicitly proves. Failed and unavailable checks remain visible.

These tracked working records retain existing private verification context.
They are not approved public artifacts; sanitize that context before any public
publication. Consolidation does not authorize publishing private topology.

## Private data

Groundplane is exercised against a private representative workload, but its
identities, domains, credentials, source paths, network topology, service
inventory, deployment driver, and captured host state are not public product
artifacts. Detailed operator-journey evidence remains in the repository-local
ignored workspace.

The public repository records reproducible contracts, generic fixtures, and CI
proofs. Public documentation must use generic names and documentation-only
addresses and must never embed a private deployment topology.
