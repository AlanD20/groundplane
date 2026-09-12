# ADR 0031: durable Attach credentials, facts, and grant references

Status: accepted

Date: 2026-08-22

## Context

An Attach connects exactly one consuming Service to exactly one Backing
Service network. The operator must be able either to provision a new managed
credential or reuse an existing credential for another Service without
provisioning or backing up the same database again.

Facts and grants belong to the provisioned credential, while network
membership belongs to each consuming Service. Failed retries must keep the
same database, role, password, fact references, credential source, and exact
network plan. Detach must not destroy a credential while another Attach uses
it.

Groundplane is pre-MVP. This decision cleanly replaces the plural
`service_ids` API and list-shaped Blueprint `services` field. There is no
compatibility decoder or schema version split.

## Decision

### Public shape

Create accepts one `service_id`, one `backing_service_id`, an optional name,
one required credential choice, and optional owner-only grants:

```json
{
  "service_id": "svc_...",
  "backing_service_id": "svc_...",
  "name": "app-database",
  "credential": {"mode": "new"},
  "grant_attach_ids": ["att_..."]
}
```

or:

```json
{
  "service_id": "svc_...",
  "backing_service_id": "svc_...",
  "name": "worker-database",
  "credential": {"mode": "existing", "attach_id": "att_..."}
}
```

`credential.mode` is exactly `new` or `existing`. `new` rejects `attach_id`.
`existing` requires `attach_id` and rejects `grant_attach_ids`. The source
must be ready, belong to the same consumer Environment, target the same
Backing Service, and own its credential directly. Reference chains are
rejected.

Human Attach representations use `service_id` and the same credential object.
The response credential object is derived from durable identity: owners expose
`{"mode":"new"}`; dependents expose
`{"mode":"existing","attach_id":"<owner>"}`.

Blueprint uses the singular `service` field and an explicit credential object:

```yaml
x-gp-attachments:
  app-db:
    service: app-api
    backing_project: postgres
    backing_service: postgres
    credential:
      mode: new
    grants: []
  worker-db:
    service: app-worker
    backing_project: postgres
    backing_service: postgres
    credential:
      mode: existing
      attach: app-db
```

An existing Blueprint reference names a credential-owning Attach in the same
Environment and Backing Service. It cannot name another dependent.

### Durable ownership

The durable Attach primary is `/v1/records/attaches/<attach-id>`. It stores:

- stable consumer Environment and singular Service id;
- stable backing Project, Environment, Service, and resolved Network ids;
- `credential_attach_id`;
- owner-only sorted unique `grant_attach_ids`;
- listable fact-set schema with key and secret classification, but no values;
- lifecycle status, current or last operation, Task id, mutable name, and
  creation time.

Credential ownership is derived, not duplicated. A new credential stores its
own Attach id in `credential_attach_id`. An existing credential stores the
source owner's Attach id. An Attach owns a credential exactly when
`id == credential_attach_id`.

Only owners have adapter provisioning identity, encrypted fact values, grants,
and database Backup-source identity. Their opaque fact envelope remains at
`/v1/secret-values/attach-facts/<owner-attach-id>`. A dependent fact reveal or
Entry `FactRef` resolves through its owner while preserving the consumer
Attach as the public reference. Dependents provision nothing and store no
duplicate encrypted values.

These backing facts are Attach-owned data, not Secret-store resources. The
Secret store separately owns reusable Project and Platform credentials.
Connector credentials follow ADR 0045: they either retain a reusable Secret
key reference or store a direct value as encrypted Connector-subordinate state.

Manual-adapter Attaches remain network-only. They own themselves but have no
facts, grants, provisioning steps, or managed database Backup source.

### Network and task behavior

Every Attach contributes the edge `(backing_network_id, service_id)` to the
Environment render union. The union is sorted and deduplicated. A dependent
create publishes only network reconciliation. An owner create publishes
adapter provision and grant steps before network reconciliation.

A dependent detach removes only its own Attach and network edge. It never
revokes grants or invokes adapter deprovision. An owner detach returns
`resource.in_use` while any dependent credential reference exists. Once no
dependents remain, owner detach revokes grants, removes the edge, and invokes
adapter deprovision normally.

The immutable render-input, retry, topology-fence, operation-lock, queue,
idempotency-marker, and successful-terminal-removal rules from the prior
contract remain. Retrying never changes `credential_attach_id`, facts, adapter
identity, or the sealed network union.

### Indexes and races

The membership and reverse-reference indexes are:

```text
/v1/indexes/attaches/by-owner/environment/<environment-id>/<attach-id>
/v1/indexes/attaches/by-service/service/<service-id>/<attach-id>
/v1/indexes/attaches/by-backing-service/service/<service-id>/<attach-id>
/v1/indexes/attaches/by-backing-project/project/<project-id>/<attach-id>
/v1/indexes/attaches/by-granted-attach/attach/<grant-attach-id>/<attach-id>
/v1/indexes/attaches/by-credential-attach/attach/<owner-id>/<consumer-id>
```

Each value is the indexed Attach id. Creating or removing a grant or credential
reference rewrites its target owner primary with identical bytes. Advancing
the target modification revision serializes target detach against concurrent
reference creation. The reverse indexes prevent owner deletion while a
committed dependent exists.

An Attach accepts at most eight grants. Create always publishes the Attach,
indexes, immutable render input, Agent Task, locks, queue membership, and
pending Environment-scoped idempotency evidence in one transaction. The
transaction remains below the repository's 96 compare-plus-mutation ceiling.

### Facts, grants, and backups

Blueprint fact sources remain `{attach, grant?, key}` and human Entry sources
remain `{kind: fact, attach_id, grant_attach_id?, fact}`. Omitted `grant`
selects the credential owner's own fact set. Present `grant` selects an
owner-declared granted Attach fact set. Grant semantics remain distinct from
credential reuse.

Only a credential-owning managed-adapter Attach may be selected as a database
Backup source. Dependent consumer Attaches are excluded, so one provisioned
database is captured once regardless of how many Services use it.

## Consequences

- Network membership remains explicit and per Service.
- Several Services may intentionally use one credential without duplicate
  provisioning, facts, grants, or backups.
- Credential ownership has one durable source of truth and no reference
  chains.
- Entry fact references remain stable when they name a dependent Attach.
- Owner detach and concurrent dependent creation cannot race into a dangling
  reference.
- PostgreSQL and Valkey keep separate Backing Service networks; a consumer
  joins each only through an Attach to that Backing Service.
- Every additional consuming Service still requires its own Attach record.
