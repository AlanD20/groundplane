# Bounded running-Service Blueprint publication

- Scope: the oversized full-Blueprint floor blocker; no limit increases
- Contract: ADR 0072, extending the existing ADR 0070 storage seam
- State: local integration; no new runtime deployed by this evidence

## Reproduction and correction

`TestBlueprintRunningUpdateBoundedPublication` performs two real preparations
and publications for two Services, each with eight 3,000-character non-secret
fixture environment values. The initial Apply completes and installs actual
serving Release projections. The second changes replicas from one to two.
Before correction, its aggregate marker was 412,657 bytes and rejected against
the 262,144-byte ceiling. This is a reproduced expansion, not a reconstructed
breakdown of the older QA failure of 692,408 bytes.

Candidate-owned immutable render inputs now retain the exact current/optional
inactive artifacts. The marker contains compact digest-bound references, not
duplicated historical snapshots. Original Release source comparisons and
candidate-input comparisons remain part of publication. Existing limits and
retention ownership are unchanged.

## Measured complete shapes

| Shape | Initial Apply | Running update |
| --- | ---: | ---: |
| Aggregate marker bytes | 71,034 | 71,808 |
| Largest candidate render record bytes | 135,113 | 170,794 |
| Assignment native artifact bytes | 0 | 53,064 |
| Assignment durable record bytes | 1,208 | 141,511 |
| Publication comparisons / mutations | 26 / 19 | 37 / 19 |
| Publication protobuf transaction bytes | 82,352 | 84,892 |
| Claim comparisons / mutations | 13 / 7 | 17 / 7 |
| Claim protobuf transaction bytes | 8,577 | 430,154 |
| Plan protobuf bytes | 54,171 | 107,395 |
| Assignment wire protobuf bytes | 400 | 105,511 |

The actual Store request includes the `/groundplane` prefix and failure reads.
Assertions pin complete operation counts and existing byte ceilings; serialized
timestamps may trim fractional zeros, so byte measurements are observations,
not unconditional constant-equality claims. Ordinary transactions pass the real
96-operation/1-MiB preparation; publication passes its existing 256-per-arm/1-MiB
preparation. Every individual durable record remains at most 262,144 bytes.

## Focused verification

- Both real publications, immutable plan reconstruction, actual Task claim,
  terminal acknowledgement, exact executed-artifact promotion and read-only
  terminal replay pass. The second claim preserves native and applied bytes.
- Real Agent WorkerPool admission accepts the reconstructed assignment/plan.
  This calls admission without effects; the assignment wire is built from the
  actual persisted claim. It is not a live gRPC or Docker execution proof.
- Missing candidate render input, foreign typed witness and changed marker
  digest reject both plan-input reading and actual claim without writes.
- Original current/retained source-CAS races, blue/green snapshot/inactive-slot
  cases, sealed native rollback recovery and reconnect proofs pass.
- The existing 32-candidate/16-hook completion regression passes unchanged.
- Combined race selection: 61.758 seconds, 14.1% etcd package coverage in
  `.tmp/blueprint-bounded-integration.cover`. Coverage is selection-local, not
  whole-package completeness.
- Final formatted validation delta: native rollback, blue/green, six source-CAS
  races and bounded running-update selection pass in 11.772 seconds, 11.7%
  package coverage (`.tmp/blueprint-reference-final.cover`).
- Tagged production Controller and Agent builds pass. Protocol and public API
  schemas are unchanged. Changed handwritten Go passes pinned formatting.

QA history at revision 3016 contains 16 markers, maximum 104,866 bytes, and
no inline serving witnesses requiring conversion. No history was rewritten.
ADR 0072 states the exact persisted-format boundary; no fallback was added.

## Delivery boundary

Live deployment and full-bundle Apply remain required before claiming acceptance.
The architecture checker remains red for deferred structural/test placement
findings (`.tmp/blueprint-reference-architecture.txt`), separately tracked in
`docs/issues/deferred-architecture-cleanup.md`; no allowance is enlarged.
