# Retained Component runtime and Caddy reload

Task3 correction. File-only Configure previously recreated Caddy and Tunnel
because each reconcile rewrote their plan/generation labels. The old single-file
bind also pinned an inode, hiding atomically replaced bytes from an unchanged
container. ADR0075 now closes both sides of the reload contract.

The Controller retains ownership only when the complete effective generated
Service and image/replica metadata are equal, excluding only those two volatile
labels, and resource definitions are unchanged. Changed runtime gets new labels
and normal reconciliation. No native workload retention rule is replaced.
Component preparation supplies its existing CAS-fenced applied artifact; native
preflight supplies the same captured predecessor authority. Candidate replay
retains the frozen result without live reads. Returned source bytes are copied.

The Agent permits historical Component labels only for a unique owner and
pinned image with canonical nonfuture generation; selecting that Service requires
one non-forced no-dependencies up. Newer file generations require that exact
runtime reconciliation before activation. Route mutation/removal keeps its
materialize/up/activate chain but no longer forces replacement.

Caddy now binds`components/caddy`to`/etc/caddy`read-only. Activation rejects the
old file bind, writable/foreign parents and a shadowing child mount. Catalog
identity advances tov12. The pinned image, writable private data/config paths,
network allocation and aliases remain unchanged. Initial mount migration will
recreate Caddy; subsequent file-only edits use native reload. Caddy's native
WebSocket policy, including`stream_close_delay`, remains operator-owned.

## Local evidence

- Failing-first mount tests accepted the old file bind and rejected the intended
  directory; failing-first ownership test changed an identical runtime's labels.
- Pure retention proves runtime changes, input immutability, corrupted digest,
  foreign owner, duplicate source and malformed label rejection. The second
  Blueprint candidate render retains the already-frozen Component ownership.
- The actual managed-Service producer/replayer retains ownership and rejects
  forced/broad/dependency-enabled/native/future-generation substitutions.
- Route create/removal plans reconstruct the same hash without force-recreate.
  Their old fixture incorrectly put generated services in authored normalized
  Compose/desired-native state; it now matches the existing separate authorities.
- Complete shared-plan, Blueprint release, Compose helper, registered Caddy and
  catalog race suites pass. Generated protobuf/OpenAPI/both clients are unchanged.
- Affected Controller/etcd/composed-app/Agent Component and Route race selections
  pass, including frozen candidate rendering and immutable applied-source copies.
- Affected Controller, Blueprint release, Compose helper, composed-app and
  registered-package vet passes; the broader shared-plan vet failure below is
  separately retained and not hidden by that narrower passing selection.

Evidence:`caddy-reload-*`under`.tmp/production-mvp-20260910/`.
The architecture gate still reports accumulated mandatory task6 corrections.
Broader shared-plan vet additionally exposes existing protobuf copy-lock
assignments in`plan.go:322`and`service_lifecycle.go:27`; this vet run is not green.
Required full CI and production qualification are not claimed.

Live reload/full-policy proof follows deployment. Normal invalid native-file
retention already passed on the preceding I build; see the preflight report.
