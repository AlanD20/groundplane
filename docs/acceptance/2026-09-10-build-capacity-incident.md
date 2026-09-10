# Shared-builder capacity incident

The attempted J deployment of8e5f62586 stopped before remote publication.
At07:51UTC the checkout filesystem reported zero available bytes; deployment
and both continuity logs ended mid-record. The processes exited120/255 and
no J update Task was created. QA still executes I, version
`0.0.0-qa.native20260910.9`, with unchanged application and ingress identities.
This is a failed, incomplete sample, not passing continuity evidence.

During the same interval, QA's kernel reported virtual-disk write errors and
failed extent conversion with potential data loss; Valkey reported AOF fsync
I/O failure at07:49:03 and its health check returned MISCONF. The public login
probe recorded500; the API health path remained200 in the retained records.
The guest filesystem itself still had1.9GiB available. This distinguishes the
incident from the earlier Tunnel timeouts; it does not prove their root cause.

Only verified regenerable Go build caches were cleared: this initiative's
`production-mvp-20260910/go-cache` and the unused `volume-selection-cache`, both
under repository-local`.tmp`. No source, evidence, archive, Docker artifact,
credential or application data was deleted. Caches are reconstructed by later
builds rather than restored from an archive. Local available capacity became
14GiB. Valkey health recovered by07:52:25 without restart; login/up returned200
with certificate verification. That is availability evidence, not proof of
persisted-data integrity. No filesystem repair, forced persistence rewrite,
Controller update or Caddy mutation was performed after the incident.

At the owner's subsequent space-cleanup request, the checkout measured33GiB,
including31GiB of ignored`.tmp`state. Seven additional stale Go caches were
verified by cache signature, ignored/untracked membership, exact canonical path
and absence of active builds, then cleared with Go's cache-clean command:
`gocache`, `go-cache`, `go-cache-deploy`, `attach-effective-cache`,
`consolidation-20260908/gocache`, `cache/go-build`and
`hosting-review-20260910/go-cache`, all under repository-local`.tmp`.
This reclaimed17,769,828,352bytes (16.55GiB); the checkout became17GiB and
available filesystem space reached29GiB. The current initiative cache was kept.
Git status matched byte-for-byte before and after cleanup. No source, history,
5.1GiB recovery archive, evidence, credential, Docker artifact or application
data was removed. These caches regenerate on later builds. Evidence:
`space-cleanup-{before,after,source-before,source-after}.txt`in the same evidence
directory. This cleanup is capacity recovery, not guest-storage qualification.

## Correction and proof

Deployment now requires10GiB checkout-filesystem headroom before target access,
native compilation and each uncached image build, then2GiB before image transfer
or target publication. It fails closed on unreadable capacity and never cleans
data automatically. These admission checks do not reserve capacity or cover
other Docker/cache/VM filesystems; the runbook records that limitation.

Both initial regressions failed against the old implementation. Boundary,
inspection-failure and between-build-stage depletion checks now pass. All60
deployment tests pass on the real host filesystem. The sandbox-only run failed
14 existing trusted-ancestor tests because its synthetic`/home`owner is65534;
the implementation's ownership checks were not weakened to make those pass.

Evidence is under`.tmp/production-mvp-20260910/`:
`qa-native-update-j-deploy.log`, `qa-native-update-j-disk-incident.txt`,
`caddy-reload-upgrade-*-continuity.*`, `deploy-capacity-red.log`,
`deploy-capacity-green.log`and`deploy-capacity-green-host.log`.

## Remaining qualification

Live mutation QA is paused while the virtual-disk and persistent-source integrity
checks remain unresolved. Preserve and check PostgreSQL, Valkey, application
files, Identity and GP state before a restore/rebuild decision; do not infer
integrity from recovered health. Resume J and the full-policy/file-only Caddy
reload proofs only on a qualified QA baseline with adequate backing capacity.
Independent local Script/context and hosting work continues under the owner's
explicit deferred-QA instruction. Production remains out of scope.
