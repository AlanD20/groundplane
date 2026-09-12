# Shared-builder capacity and QA storage-integrity incident

This is the standalone incident record for 2026-09-10. It remains an active
qualification blocker. Capacity recovery and process health did not prove
persistent-data integrity.

## Incident

Attempted J deployment from source `8e5f62586` stopped before remote
publication. At 07:51 UTC the checkout filesystem reported zero available
bytes; deployment and both continuity logs ended mid-record with process exits
120/255. No J update Task was created. QA therefore remained on I,
`0.0.0-qa.native20260910.9`, with its application and ingress identities
unchanged. The partial logs are failed/incomplete evidence, never a passing
continuity sample.

During that interval, the QA kernel reported virtual-disk write errors and
failed extent conversion with potential data loss. Valkey reported AOF fsync
I/O failure at 07:49:03 and its health check returned MISCONF. Public login
returned 500 while the API health path stayed 200 in the retained record. The
guest filesystem still reported 1.9 GiB free. This incident is distinct from
the earlier Tunnel timeouts and does not establish their cause.

Valkey health recovered by 07:52:25 without restart; login and `/up` returned
200 with certificate verification. No filesystem repair, persistence rewrite,
Controller update or Caddy mutation followed. Recovery proves availability,
not persisted integrity.

## Capacity recovery

Only verified regenerable repository-local Go caches were cleared. The first
cleanup covered `.tmp/production-mvp-20260910/go-cache` and the unused
`.tmp/volume-selection-cache`, restoring 14 GiB locally.

At the owner's later explicit cleanup request, seven additional ignored caches
were verified by signature, canonical path, untracked status and absence of
active builds, then cleared with Go's cache-clean operation:

- `gocache`;
- `go-cache`;
- `go-cache-deploy`;
- `attach-effective-cache`;
- `consolidation-20260908/gocache`;
- `cache/go-build`; and
- `hosting-review-20260910/go-cache`.

All paths were under repository-local `.tmp/`. This reclaimed 17,769,828,352
bytes (16.55 GiB); checkout use became 17 GiB and filesystem availability 29
GiB. Git status matched byte-for-byte before and after. No source, Git history,
5.1 GiB recovery archive, evidence, credential, Docker artifact or application
data was deleted. Evidence is
`space-cleanup-{before,after,source-before,source-after}.txt` under the incident
evidence directory.

## Capacity guard

Deployment now requires 10 GiB checkout-filesystem headroom before target
access, native compilation and each uncached image build, then 2 GiB before
image transfer or target publication. Unreadable capacity fails closed. The
guard never deletes data automatically and does not reserve capacity or cover
other Docker, cache or VM filesystems.

Failing-first boundary, inspection-error and between-stage depletion checks
passed after correction. All 60 deployment tests passed in the real host
filesystem namespace. A restricted sandbox run still failed 14 trusted-ancestor
tests because synthetic `/home` ownership was uid 65534; ownership checks were
not weakened.

Evidence under `.tmp/production-mvp-20260910/`:

- `qa-native-update-j-deploy.log`;
- `qa-native-update-j-disk-incident.txt`;
- `caddy-reload-upgrade-*-continuity.*`;
- `deploy-capacity-red.log`;
- `deploy-capacity-green.log`; and
- `deploy-capacity-green-host.log`.

## Unresolved integrity and authority

Live mutation QA remains paused. Before any restore/rebuild decision or resumed
J/Caddy/Script mutation, qualify the virtual disk and every affected persistent
source: PostgreSQL, Valkey AOF state, application files/Volumes
and Groundplane state. Preserve the evidence and verify actual current QA state.

Production remains out of scope. This incident does not authorize production,
provider, firewall, host-network, BIP or other-host changes. Independent local
work may continue under its own authority.
