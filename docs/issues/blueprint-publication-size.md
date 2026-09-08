# Blueprint Release publication exceeds its bounded record

- Status: open runtime defect; not waived by successful split deployment
- Owner: deployment implementation owner
- Severity: high for full-Blueprint updates of running Services
- MVP-required: yes for the full Blueprint update contract; not a reason to undo working QA hosting

## Evidence

The QA full-bundle hostname update rejected before Task publication with a
`release-publication` encoded size of 692,408 bytes against the 262,144-byte
record ceiling. Smaller desired-state waves and explicit group/gateway Deploy
subsequently activated the hostnames. They do not prove full-bundle Apply works.

Read-only QA inspection on 2026-09-08 after gateway deployment found 13 stored
publication markers and 70 staged render inputs, with no truncated result page.
The largest stored marker was 104,866 bytes. Its ten native predecessors had
no serving runtime snapshots; it is not representative of a running-service
update. The rejected marker is absent, so its exact per-field breakdown was
not recovered from persistence.

Several current Service render inputs contain a 154,044-byte Environment
projection each. The newest gateway render input is 181,413 bytes, including
that projection and 25,433 JSON bytes of native prior-runtime authority.
Only identifiers and sizes were emitted; no configuration or secret contents
were copied into the evidence.

## Source-localized expansion

`internal/controller/blueprintrelease/native_predecessor.go` captures a complete
`ServiceLifecycleRelease` as `BlueprintNativePredecessor.Serving`. That embeds
current and optional retained `ReleaseRenderInput` snapshots, each including an
Environment projection. It also captures the rendered predecessor artifact bytes.

`internal/infra/etcd/blueprint_release_publication_prepare.go` places all those
snapshots, native artifact bytes, the executed Environment artifact, and the
candidate descriptor in one `ReleasePublicationMarker`.
`encodeReleaseRecord` correctly rejects the oversized result.

The duplicated snapshots are a demonstrated expansion mechanism. Their exact
share of the historical 692,408-byte failed marker remains unmeasured. Do not
claim that removing one field alone proves the full lifecycle fits.

## Required correction and proof

- Keep the record ceiling and immutable recovery witnesses. Neither truncating
  evidence nor raising etcd limits is an acceptable correction.
- Separate publication visibility from bulk immutable evidence using bounded,
  digest-bound storage. Resolve how existing durable history remains readable
  before changing a persisted representation; do not add an ad-hoc fallback.
- Preserve fixed-revision source validation, atomic visibility, claim/retry
  identity, exact executed bytes, native recovery proof, and retention ownership.
- Cover the final publication transaction budget and downstream assignment
  evidence bounds as well as the marker itself. The assignment also contains
  predecessor artifact bytes: native predecessor bytes have a 256 KiB aggregate
  validation ceiling, and the applied predecessor artifact is separately bounded.
- Reproduce a multi-Service running update through the actual publisher in a
  focused regression; then prove claim, terminal acknowledgement, and replay.
- Verify a relevant full-bundle change against QA without replaying an old,
  superseded desired revision over the working gateway.

No production-code change, new test run, deployment, or live Apply was performed
for this diagnosis. Broader tests remain paused; the prior focused-test approval
was for ordinary per-Service recovery authority, not this storage correction.
