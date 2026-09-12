# ADR 0054: Managed adapter release registry and Valkey 9 authority

- Status: Rejected
- Date: 2026-08-25

## Rejection

Rejected for the MVP on 2026-08-29. This proposal conflicts with the accepted
C10 implementation and with the dual-platform product requirement in ADR 0060.
It also adds compiled release history, revocation, durable reverse-reference
auditing, and unavailable release inputs that `docs/mvp.md` does not require
for the MVP.

The Valkey safety analysis may inform a later, smaller decision. None of this
proposal's registry, release, wire, helper, or ARM64-only clauses has current
product authority. ADRs 0047 and 0048 must remove their dependency on this
proposal before acceptance.

## Context

ADR 0037 defines the Backing Service facade, including the first-class
network-only `manual` adapter, and the managed MVP catalog entries
`postgres:16` and `valkey:9`. ADRs 0047 and 0048 define the exact managed
PostgreSQL 16 image and the sole schema-1 Agent Backup execution protocol.
Those decisions do not yet provide one shared immutable adapter-release
identity, a compiled current/history/revocation registry, durable reverse
release references, or an exact managed Valkey 9 image and administrative
procedure.

The current lower-level adapter interface conflicts with the intended product
boundary. It is keyed only by adapter name, returns mutable default image tags,
exposes advisory command strings, and cannot distinguish current, historical,
or revoked releases. The current Valkey implementation invokes unauthenticated
`valkey-cli`, does not durably save or verify ACL changes, and advertises an RDB
restore command that does not restore an RDB image. Those shapes cannot be
used as create, retry, Attach, Backup, or deletion authority.

No managed Backing Service release has shipped. These contracts are replaced
cleanly in schema 1. There is no schema 2, tag fallback, key-only adapter
lookup, dual Agent authentication field, runtime registry mutation, or
compatibility reader.

## Scope

This decision closes only the MVP managed-adapter release boundary for one
Controller, one Agent, and one ARM64 host. This rejected scope has no product
authority. ADR 0060 requires both `linux/amd64` and `linux/arm64`.

The rejected proposal defines:

- the immutable identity of a compiled adapter revision;
- the compiled registry, history, revocation, and durable-reference rules;
- the exact upstream and first-party derived Valkey 9 image authority;
- the compiled Valkey readiness, provision, and detach procedure;
- Controller and Agent agreement on adapter revision in wire schema 1;
- the safe public adapter catalog and pinned Backing Service projection; and
- the explicit absence of Valkey Backup support in the MVP.

It does not add adapter upgrades, Valkey clustering, Valkey keyspace grants,
runtime plugin loading, dynamic registry distribution, release mutation,
operator revocation actions, or Valkey Backup/Restore.

## Decision

### 1. Use one stable key and one immutable revision

The managed adapter keys are exactly:

```text
postgres:16
valkey:9
```

`manual` remains a public first-class network-only Backing Service and Attach
adapter. It is not a managed adapter and never receives an AdapterRevision,
managed image, compiled procedure, fact, grant, or Backup capability. Section
12 defines its separate catalog/create/binding variant.

For every digest in this ADR, `JCS(x)` is the RFC 8785 canonical JSON encoding
of `x`, and:

```text
D(domain, bytes) = SHA-256(ASCII(domain) || 0x00 || bytes)
```

`HEX64` is exactly 64 lowercase hexadecimal characters. `OCI_SHA256` is
exactly `sha256:` followed by `HEX64`. `SAFE_UINT_POSITIVE` is a positive JSON
integer no greater than `2^53-1`. A JSON member named `*_sha256` is `HEX64`
unless its type is stated as `OCI_SHA256`.

The immutable adapter-revision preimage is exactly:

```text
ManagedAdapterReleaseRecord = {
  adapter_key: "postgres:16" | "valkey:9",
  adapter_contract_version: SAFE_UINT_POSITIVE,
  managed_image_release_sha256: HEX64,
  runtime_major: 16 | 9,
  runtime_plan_contract_sha256: HEX64,
  procedure_contract_sha256: HEX64,
  catalog_contract_sha256: HEX64,
  backup_contract: "postgres-custom-v1" | "none"
}
```

`adapter_contract_version` must also fit a positive unsigned 32-bit integer.
The adapter revision is:

```text
adapter_revision =
  D("groundplane.managed-adapter-release.v1",
    JCS(ManagedAdapterReleaseRecord))
```

Internally and on protobuf, an AdapterRevision is exactly those 32 raw bytes.
JSON and human output encode it as `sha256:` plus 64 lowercase hexadecimal
characters. The release record deliberately excludes `adapter_revision`,
current/history/revocation state, timestamps, mutable tags, repository aliases,
host facts, and durable reference counts. Its construction is cycle-free.

For `postgres:16`, `managed_image_release_sha256` is exactly the
`managed_release_sha256` defined by [Backup artifact contract](../features/backups/artifacts.md). This ADR does not define a second
PostgreSQL image-release record. For `valkey:9`, it is the Valkey level-two
release digest defined below.

The three referenced contract digests use the same domain-separated rule:

```text
runtime_plan_contract_sha256 =
  D("groundplane.managed-adapter-runtime-plan.v1", JCS(RuntimePlanContract))

procedure_contract_sha256 =
  D("groundplane.managed-adapter-procedure.v1", JCS(ProcedureContract))

catalog_contract_sha256 =
  D("groundplane.managed-adapter-catalog.v1", JCS(CatalogContract))
```

The closed schemas are exactly:

```text
RuntimePlanContract = {
  schema: 1,
  adapter_key: "postgres:16" | "valkey:9",
  adapter_contract_version: SAFE_UINT_POSITIVE,
  runtime_major: 16 | 9,
  managed_image_release_sha256: HEX64,
  image_ref: OCI_REPOSITORY + "@" + OCI_SHA256,
  platform: {os: "linux", architecture: "arm64", variant: "v8" | null},
  inherited_config: {
    source_config_digest: OCI_SHA256,
    entrypoint: [string],
    cmd: [string],
    user: string | null,
    working_dir: string,
    environment: [string],
    exposed_ports: [string],
    volumes: [string],
    stop_signal: string | null
  },
  service: {
    command: [string],
    port: string,
    data_mount_target: string,
    strategy: "recreate",
    replicas: 1,
    default_healthcheck: null,
    readiness_contract: string
  },
  authored_policy: {
    forbidden_fields: [string],
    reserved_environment: [string],
    protected_mount_policy: string
  },
  release_labels: [{key: string, value_source: string}]
}

ProcedureContract = {
  schema: 1,
  adapter_key: "postgres:16" | "valkey:9",
  adapter_contract_version: SAFE_UINT_POSITIVE,
  runtime_major: 16 | 9,
  secret_transport: "task-secret-slot-v1",
  exec_contract: string,
  command_policy: {
    kind: "postgres_lifecycle_manifest",
    domain: "groundplane.postgres16.lifecycle-procedure-manifest.v1",
    manifest_sha256: HEX64,
    manifest_bytes: SAFE_UINT_POSITIVE,
    record_count: SAFE_UINT_POSITIVE,
    lifecycle_operation_count: SAFE_UINT_POSITIVE
  } | {
    kind: "valkey_acl_command_manifest",
    domain: "groundplane.valkey9.attach-command-set.v1",
    manifest_sha256: HEX64,
    manifest_bytes: SAFE_UINT_POSITIVE,
    command_count: SAFE_UINT_POSITIVE,
    source_commands_def_sha256: HEX64
  },
  operations: [{
    name: string,
    phase: string,
    contract_id: string,
    secret_purposes: [string],
    idempotency: string
  }]
}

CatalogContract = {
  schema: 1,
  adapter_key: "postgres:16" | "valkey:9",
  adapter_contract_version: SAFE_UINT_POSITIVE,
  display_name: string,
  default_facts_prefix: string,
  url_scheme: string,
  port: string,
  fact_schema: [{key: string, secret: boolean}],
  supports_grants: boolean,
  backup_supported: boolean,
  default_healthcheck: null,
  strategy: "recreate",
  replicas: 1,
  provision_summary: [string],
  detach_summary: [string]
}
```

No member is optional. Arrays are order-sensitive and appear in the exact order
shown by the instantiated objects below. JSON `null` is never omission. Before
hashing, release tokens in uppercase below are replaced by values already
validated from the nested managed image build/release record. They are not
literal strings in JCS.

`UPSTREAM_POSTGRES_CONFIG_ENV`, `UPSTREAM_POSTGRES_EXPOSED_PORTS`, and
`UPSTREAM_POSTGRES_VOLUMES` expand to the complete ordered arrays read from the
authenticated OCI config
`sha256:c05eced0bdb41ea9b95a656472a6aa4d50cad0d8a2e33d14eb1c53fd6204f2ae`.
The expansion happens before JCS and is byte-compared to that source; it is not
a mutable build input.

The exact PostgreSQL runtime object is:

```text
{
  "schema": 1,
  "adapter_key": "postgres:16",
  "adapter_contract_version": 1,
  "runtime_major": 16,
  "managed_image_release_sha256": POSTGRES_MANAGED_RELEASE_SHA256,
  "image_ref": POSTGRES_MANAGED_REPOSITORY + "@" + POSTGRES_MANAGED_INDEX_DIGEST,
  "platform": {"os":"linux","architecture":"arm64","variant":"v8"},
  "inherited_config": {
    "source_config_digest": "sha256:c05eced0bdb41ea9b95a656472a6aa4d50cad0d8a2e33d14eb1c53fd6204f2ae",
    "entrypoint": ["docker-entrypoint.sh"],
    "cmd": ["postgres"],
    "user": null,
    "working_dir": "/",
    "environment": UPSTREAM_POSTGRES_CONFIG_ENV,
    "exposed_ports": UPSTREAM_POSTGRES_EXPOSED_PORTS,
    "volumes": UPSTREAM_POSTGRES_VOLUMES,
    "stop_signal": "SIGINT"
  },
  "service": {
    "command": ["postgres"],
    "port": "5432/tcp",
    "data_mount_target": "/var/lib/postgresql/data",
    "strategy": "recreate",
    "replicas": 1,
    "default_healthcheck": null,
    "readiness_contract": "postgres16-authenticated-readiness-v1"
  },
  "authored_policy": {
    "forbidden_fields": ["build","command","entrypoint","healthcheck","image","platform","pull_policy","user"],
    "reserved_environment": ["DOCKER_PG_LLVM_DEPS","PGDATA","PG_MAJOR","PG_SHA256","PG_VERSION","POSTGRES_DB","POSTGRES_HOST_AUTH_METHOD","POSTGRES_INITDB_ARGS","POSTGRES_INITDB_WALDIR","POSTGRES_PASSWORD","POSTGRES_USER"],
    "protected_mount_policy": "managed-postgres16-protected-mounts-v1"
  },
  "release_labels": [
    {"key":"com.groundplane.adapter-key","value_source":"adapter_key"},
    {"key":"com.groundplane.adapter-revision","value_source":"derived_adapter_revision"},
    {"key":"com.groundplane.managed-image-release-sha256","value_source":"managed_image_release_sha256"}
  ]
}
```

The exact Valkey runtime object is:

```text
{
  "schema": 1,
  "adapter_key": "valkey:9",
  "adapter_contract_version": 1,
  "runtime_major": 9,
  "managed_image_release_sha256": VALKEY_MANAGED_RELEASE_SHA256,
  "image_ref": VALKEY_MANAGED_REPOSITORY + "@" + VALKEY_MANAGED_INDEX_DIGEST,
  "platform": {"os":"linux","architecture":"arm64","variant":null},
  "inherited_config": {
    "source_config_digest": "sha256:c6b941456ef0887bd5108424bf038ff7a727c07ad76381184c510b0effda19d2",
    "entrypoint": ["docker-entrypoint.sh"],
    "cmd": ["valkey-server"],
    "user": null,
    "working_dir": "/data",
    "environment": ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin","VALKEY_VERSION=9.1.1"],
    "exposed_ports": ["6379/tcp"],
    "volumes": [],
    "stop_signal": null
  },
  "service": {
    "command": ["valkey-server","/usr/local/share/groundplane/valkey9.conf"],
    "port": "6379/tcp",
    "data_mount_target": "/data",
    "strategy": "recreate",
    "replicas": 1,
    "default_healthcheck": null,
    "readiness_contract": "valkey9-authenticated-readiness-v1"
  },
  "authored_policy": {
    "forbidden_fields": ["build","command","entrypoint","healthcheck","image","platform","pull_policy","user"],
    "reserved_environment": ["VALKEYCLI_AUTH","VALKEY_EXTRA_FLAGS","VALKEY_VERSION"],
    "protected_mount_policy": "managed-valkey9-protected-mounts-v1"
  },
  "release_labels": [
    {"key":"com.groundplane.adapter-key","value_source":"adapter_key"},
    {"key":"com.groundplane.adapter-revision","value_source":"derived_adapter_revision"},
    {"key":"com.groundplane.managed-image-release-sha256","value_source":"managed_image_release_sha256"}
  ]
}
```

The exact PostgreSQL procedure object is:

```text
{
  "schema":1,
  "adapter_key":"postgres:16",
  "adapter_contract_version":1,
  "runtime_major":16,
  "secret_transport":"task-secret-slot-v1",
  "exec_contract":"managed-adapter-exec-v1",
  "command_policy":{
    "kind":"postgres_lifecycle_manifest",
    "domain":"groundplane.postgres16.lifecycle-procedure-manifest.v1",
    "manifest_sha256":"988236f06d1f495090ef4eb3bf39c11ff760562e29da2a0925cfb473ce8d6975",
    "manifest_bytes":13427,
    "record_count":86,
    "lifecycle_operation_count":5
  },
  "operations":[
    {"name":"readiness","phase":"readiness","contract_id":"postgres16-authenticated-readiness-v1","secret_purposes":[],"idempotency":"read_only"},
    {"name":"provision","phase":"provision","contract_id":"postgres16-provision-v1","secret_purposes":["managed_adapter_attach_password"],"idempotency":"convergent_same_intent"},
    {"name":"grant","phase":"grant","contract_id":"postgres16-grant-v1","secret_purposes":[],"idempotency":"convergent_same_intent"},
    {"name":"revoke","phase":"revoke","contract_id":"postgres16-revoke-v1","secret_purposes":[],"idempotency":"convergent_same_intent"},
    {"name":"detach","phase":"detach","contract_id":"postgres16-detach-v1","secret_purposes":[],"idempotency":"convergent_same_intent"},
    {"name":"backup","phase":"backup","contract_id":"postgres-custom-v1","secret_purposes":[],"idempotency":"adr0047_0048"},
    {"name":"restore","phase":"restore","contract_id":"postgres-custom-v1","secret_purposes":[],"idempotency":"adr0047_0048"}
  ]
}
```

The PostgreSQL lifecycle manifest bytes are exactly the following ASCII bytes,
one LF after every shown record including the last. Its digest is
`D("groundplane.postgres16.lifecycle-procedure-manifest.v1", manifest_bytes) =
988236f06d1f495090ef4eb3bf39c11ff760562e29da2a0925cfb473ce8d6975`.
It is compiled into Controller and Agent registry entries and is never accepted
as plan input or sent over the channel:

```text
schema=1
encoding=ASCII lines, one LF per record including final LF
identifier={I:x} is ASCII double-quote + x with each double-quote doubled + ASCII double-quote
literal={L:x} is ASCII single-quote + x with each single-quote doubled + ASCII single-quote
secret={S:password} is a 43-byte unpadded-base64url value rendered as literal; it exists only in transient SQL stdin
marker={M:identity} is ASCII groundplane.postgres16/v1;backing=<backing_service_id>;identity=<identity>;revision=<64-lowercase-adapter-revision-hex>
owned_role={O:r,marker} expands to r.rolcanlogin AND r.rolinherit AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls AND r.rolconnlimit=-1 AND r.rolvaliduntil IS NULL AND r.rolpassword IS NOT NULL AND r.rolconfig IS NULL AND shobj_description(r.oid,'pg_authid')={L:marker} AND NOT EXISTS(SELECT 1 FROM pg_auth_members m WHERE m.member=r.oid OR m.roleid=r.oid)
exec=/usr/local/bin/psql
exec_user=exact numeric PostgreSQL server uid:gid attested by the nested [Backup artifact contract](../features/backups/artifacts.md) managed image release; no name lookup
bounds=stdin 8192 bytes|stdout 256 bytes|stderr 0 bytes
argv=--no-psqlrc|--quiet|--tuples-only|--no-align|--set=ON_ERROR_STOP=1|--host=/var/run/postgresql|--username=postgres|--dbname={database}
env=PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin|HOME=/var/lib/postgresql|LC_ALL=C|TZ=UTC|PGAPPNAME=groundplane-postgres16-lifecycle|PGOPTIONS=-cclient_min_messages=error
input=one exact rendered UTF-8 SQL program on attached stdin followed by EOF; no SQL, identifier, or secret in argv or env
result=normal exit 0 and stderr empty; mutation stdout empty; probe/proof stdout byte-equals its declared value; every other result fails closed
readiness.database=postgres
readiness.sql=SELECT current_setting('server_version_num')::integer / 10000,current_database(),current_user,pg_is_in_recovery();\n
readiness.stdout=16|postgres|postgres|f\n
provision.preflight.database=postgres
provision.preflight.sql=SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname={L:role}) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}) THEN 'new' WHEN EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname={L:role} AND {O:r,{M:role}}) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}) THEN 'role' WHEN EXISTS(SELECT 1 FROM pg_roles r JOIN pg_database d ON d.datdba=r.oid WHERE r.rolname={L:role} AND d.datname={L:database} AND d.datallowconn AND NOT d.datistemplate AND {O:r,{M:role}}) THEN 'both' ELSE 'conflict' END;\n
provision.preflight.stdout=new\n durably checkpoints role_create_prepared=true under the task-assignment-step-nonce-revision fence before role_create; role\n or both\n adopts only exact owned state and resets the same sealed password; conflict\n fails before mutation
provision.role_create.database=postgres
provision.role_create.sql=BEGIN;\nCREATE ROLE {I:role} WITH LOGIN PASSWORD {S:password} NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT -1;\nCOMMENT ON ROLE {I:role} IS {L:{M:role}};\nCOMMIT;\n
provision.role_create_rule=after success,error,or unknown never blind-rerun CREATE ROLE; execute role_create_readback under the durable role_create_prepared fence
provision.role_create_readback.database=postgres
provision.role_create_readback.sql=SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname={L:role}) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}) THEN 'absent' WHEN EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname={L:role} AND {O:r,{M:role}}) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}) THEN 'created' ELSE 'conflict' END;\n
provision.role_create_readback.stdout=created\n durably sets new_role_this_attempt=true and clears role_create_prepared before database_create; absent\n permits same-intent role_create retry only after the old exec is proven not running and while the same deadline remains; conflict\n is foreign or unsafe and clears no evidence
provision.role_adopt.database=postgres
provision.role_adopt.sql=ALTER ROLE {I:role} WITH LOGIN PASSWORD {S:password};\n
provision.database_create.database=postgres
provision.database_create.sql=CREATE DATABASE {I:database} OWNER {I:role};\n
provision.database_create_rule=execute only for new or role preflight; after success,error,or unknown always execute database_adopt_probe before any scope step
provision.database_adopt_probe.database=postgres
provision.database_adopt_probe.sql=SELECT d.datdba=r.oid AND d.datallowconn AND NOT d.datistemplate FROM pg_roles r JOIN pg_database d ON d.datdba=r.oid WHERE r.rolname={L:role} AND d.datname={L:database} AND {O:r,{M:role}};\n
provision.database_adopt_probe.stdout=t\n adopts exact owned create/readback; every other output is collision or failure and authorizes compensation only when new_role_this_attempt=true
provision.compensation_probe.database=postgres
provision.compensation_probe.sql=SELECT {O:r,{M:role}} AND NOT EXISTS(SELECT 1 FROM pg_database d WHERE d.datdba=r.oid) FROM pg_roles r WHERE r.rolname={L:role};\n
provision.compensation_probe.stdout=t\n authorizes DROP ROLE only for new_role_this_attempt=true; otherwise retain exact marked state and fail retryable
provision.compensation.database=postgres
provision.compensation.sql=DROP ROLE {I:role};\n
provision.compensation_proof.database=postgres
provision.compensation_proof.sql=SELECT NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname={L:role});\n
provision.compensation_proof.stdout=t\n
provision.scope.database={database}
provision.scope.sql=REVOKE CONNECT ON DATABASE {I:database} FROM PUBLIC;\nREVOKE ALL ON SCHEMA public FROM PUBLIC;\nGRANT ALL PRIVILEGES ON DATABASE {I:database} TO {I:role};\nGRANT ALL ON SCHEMA public TO {I:role};\n
provision.proof.database=postgres
provision.proof.sql=SELECT {O:r,{M:role}},d.datdba=r.oid AND d.datallowconn AND NOT d.datistemplate,has_database_privilege(r.rolname,d.datname,'CONNECT') FROM pg_roles r JOIN pg_database d ON d.datname={L:database} WHERE r.rolname={L:role};\n
provision.proof.stdout=t|t|t\n
grant.owner_probe.database=postgres
grant.owner_probe.sql=SELECT {O:g,{M:role}},{O:o,{M:grant_on}} AND d.datdba=o.oid AND d.datallowconn AND NOT d.datistemplate FROM pg_roles g,pg_roles o,pg_database d WHERE g.rolname={L:role} AND o.rolname={L:grant_on} AND d.datname={L:grant_on};\n
grant.owner_probe.stdout=t|t\n
grant.database=postgres
grant.sql=REVOKE CONNECT ON DATABASE {I:grant_on} FROM {I:role};\nGRANT CONNECT ON DATABASE {I:grant_on} TO {I:role};\n
grant.scope.database={grant_on}
grant.scope.sql=ALTER DEFAULT PRIVILEGES FOR ROLE {I:grant_on} IN SCHEMA public REVOKE ALL ON TABLES FROM {I:role};\nALTER DEFAULT PRIVILEGES FOR ROLE {I:grant_on} IN SCHEMA public REVOKE ALL ON SEQUENCES FROM {I:role};\nREVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {I:role};\nREVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {I:role};\nREVOKE ALL ON SCHEMA public FROM {I:role};\nGRANT USAGE ON SCHEMA public TO {I:role};\nGRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO {I:role};\nGRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA public TO {I:role};\nALTER DEFAULT PRIVILEGES FOR ROLE {I:grant_on} IN SCHEMA public GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO {I:role};\nALTER DEFAULT PRIVILEGES FOR ROLE {I:grant_on} IN SCHEMA public GRANT USAGE,SELECT,UPDATE ON SEQUENCES TO {I:role};\n
grant.proof.database={grant_on}
grant.proof.sql=WITH n AS (SELECT oid FROM pg_namespace WHERE nspname='public'),g AS (SELECT oid FROM pg_roles WHERE rolname={L:role}),o AS (SELECT oid FROM pg_roles WHERE rolname={L:grant_on}),t AS (SELECT COALESCE(bool_and(has_table_privilege({L:role},c.oid,'SELECT') AND has_table_privilege({L:role},c.oid,'INSERT') AND has_table_privilege({L:role},c.oid,'UPDATE') AND has_table_privilege({L:role},c.oid,'DELETE')),true) ok FROM pg_class c,n WHERE c.relnamespace=n.oid AND c.relkind IN ('r','p','v','m','f')),s AS (SELECT COALESCE(bool_and(has_sequence_privilege({L:role},c.oid,'USAGE') AND has_sequence_privilege({L:role},c.oid,'SELECT') AND has_sequence_privilege({L:role},c.oid,'UPDATE')),true) ok FROM pg_class c,n WHERE c.relnamespace=n.oid AND c.relkind='S'),dt AS (SELECT count(*)=4 AND count(DISTINCT a.privilege_type)=4 AND bool_and(NOT a.is_grantable) ok FROM pg_default_acl d,n,o,g CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclrole=o.oid AND d.defaclnamespace=n.oid AND d.defaclobjtype='r' AND a.grantee=g.oid AND a.privilege_type IN ('SELECT','INSERT','UPDATE','DELETE')),ds AS (SELECT count(*)=3 AND count(DISTINCT a.privilege_type)=3 AND bool_and(NOT a.is_grantable) ok FROM pg_default_acl d,n,o,g CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclrole=o.oid AND d.defaclnamespace=n.oid AND d.defaclobjtype='S' AND a.grantee=g.oid AND a.privilege_type IN ('USAGE','SELECT','UPDATE')) SELECT has_database_privilege({L:role},{L:grant_on},'CONNECT'),has_schema_privilege({L:role},'public','USAGE'),t.ok,s.ok,dt.ok,ds.ok FROM t,s,dt,ds;\n
grant.proof.stdout=t|t|t|t|t|t\n
revoke.owner_probe.database=postgres
revoke.owner_probe.sql=SELECT {O:g,{M:role}},{O:o,{M:grant_on}} AND d.datdba=o.oid AND NOT d.datistemplate FROM pg_roles g,pg_roles o,pg_database d WHERE g.rolname={L:role} AND o.rolname={L:grant_on} AND d.datname={L:grant_on};\n
revoke.owner_probe.stdout=t|t\n
revoke.database=postgres
revoke.sql=REVOKE CONNECT ON DATABASE {I:grant_on} FROM {I:role};\n
revoke.scope.database={grant_on}
revoke.scope.sql=ALTER DEFAULT PRIVILEGES FOR ROLE {I:grant_on} IN SCHEMA public REVOKE ALL ON TABLES FROM {I:role};\nALTER DEFAULT PRIVILEGES FOR ROLE {I:grant_on} IN SCHEMA public REVOKE ALL ON SEQUENCES FROM {I:role};\nREVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {I:role};\nREVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {I:role};\nREVOKE ALL ON SCHEMA public FROM {I:role};\n
revoke.proof.database={grant_on}
revoke.proof.sql=WITH n AS (SELECT oid FROM pg_namespace WHERE nspname='public'),g AS (SELECT oid FROM pg_roles WHERE rolname={L:role}),o AS (SELECT oid FROM pg_roles WHERE rolname={L:grant_on}),t AS (SELECT COALESCE(bool_and(NOT has_table_privilege({L:role},c.oid,'SELECT') AND NOT has_table_privilege({L:role},c.oid,'INSERT') AND NOT has_table_privilege({L:role},c.oid,'UPDATE') AND NOT has_table_privilege({L:role},c.oid,'DELETE')),true) ok FROM pg_class c,n WHERE c.relnamespace=n.oid AND c.relkind IN ('r','p','v','m','f')),s AS (SELECT COALESCE(bool_and(NOT has_sequence_privilege({L:role},c.oid,'USAGE') AND NOT has_sequence_privilege({L:role},c.oid,'SELECT') AND NOT has_sequence_privilege({L:role},c.oid,'UPDATE')),true) ok FROM pg_class c,n WHERE c.relnamespace=n.oid AND c.relkind='S'),dt AS (SELECT NOT EXISTS(SELECT 1 FROM pg_default_acl d,n,o,g CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclrole=o.oid AND d.defaclnamespace=n.oid AND d.defaclobjtype='r' AND a.grantee=g.oid) ok),ds AS (SELECT NOT EXISTS(SELECT 1 FROM pg_default_acl d,n,o,g CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclrole=o.oid AND d.defaclnamespace=n.oid AND d.defaclobjtype='S' AND a.grantee=g.oid) ok) SELECT NOT has_database_privilege({L:role},{L:grant_on},'CONNECT'),NOT has_schema_privilege({L:role},'public','USAGE'),t.ok,s.ok,dt.ok,ds.ok FROM t,s,dt,ds;\n
revoke.proof.stdout=t|t|t|t|t|t\n
detach.precondition=all durable grants from or to this Attach have completed their exact revoke proof; otherwise detach is not assigned
detach.state_probe.database=postgres
detach.state_probe.sql=SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname={L:role}) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}) THEN 'none' WHEN EXISTS(SELECT 1 FROM pg_roles r WHERE r.rolname={L:role} AND {O:r,{M:role}}) AND NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}) THEN 'role' WHEN EXISTS(SELECT 1 FROM pg_roles r JOIN pg_database d ON d.datdba=r.oid WHERE r.rolname={L:role} AND d.datname={L:database} AND NOT d.datistemplate AND {O:r,{M:role}}) THEN 'both' ELSE 'conflict' END;\n
detach.state_probe.stdout=none\n means success; role\n means drop exact marked role then prove; both\n means quiesce/drop exact owned database then drop role; conflict\n fails untouched
detach.quiesce.database=postgres
detach.quiesce.sql=ALTER DATABASE {I:database} WITH ALLOW_CONNECTIONS false;\nSELECT COALESCE(bool_and(pg_terminate_backend(pid)),true) FROM pg_stat_activity WHERE datname={L:database} AND pid<>pg_backend_pid();\n
detach.quiesce.stdout=t\n
detach.connection_proof.database=postgres
detach.connection_proof.sql=SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname={L:database});\n
detach.connection_proof.stdout=t\n
detach.database_drop.database=postgres
detach.database_drop.sql=DROP DATABASE {I:database};\n
detach.database_drop_rule=after success,error,or unknown rerun state_probe; only role\n or none\n proves database removal; both\n repeats quiesce/drop under the same fence
detach.role_drop.database=postgres
detach.role_drop.sql=DROP ROLE {I:role};\n
detach.role_drop_rule=execute only after state_probe role\n and exact marked safe-role proof; DROP failure retains role and fails closed; after success,error,or unknown rerun state_probe
detach.proof.database=postgres
detach.proof.sql=SELECT NOT EXISTS(SELECT 1 FROM pg_database WHERE datname={L:database}),NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname={L:role});\n
detach.proof.stdout=t|t\n
```

The renderer accepts only the already validated typed fields sealed in the
procedure, applies the manifest's exact `I`, `L`, and `S` transforms, and
rejects an output over the manifest input bound before ExecCreate. Each record
named `.sql` is one separate single-start attached Exec except the exact
multi-statement records shown with `\n`. Branches are authorized only by the
byte-exact probe output. On an unknown Exec result, a fresh exec may rerun only
when that manifest record's explicit rule permits it, after the prior exec is
proven not running, and while the sealed deadline remains. `CREATE ROLE` is
never blindly rerun: its durable prepared fence requires exact role readback,
and only proven absence authorizes same-intent retry. All mutation steps and
branches converge under the same typed intent. PostgreSQL password bytes arrive only through the Task
secret slot and are cleared under the exhaustive rule below. The nested ADR
0047 release continues to own the immutable image and PostgreSQL Backup helper;
this manifest neither changes nor competes with that release record.

For these PostgreSQL lifecycle steps, `ManagedAdapterExecV1` supplies the same
pre-ExecCreate and pre-start tuple/image/label/mount re-attestation, inherited
environment name-removal construction, direct absolute executable rule, sole
attached start, concurrent exact input write and `CloseWrite`, TTY-false
demultiplexing, EOF/ExecInspect proof, no-overlap rule, and unknown-result
recovery defined below. ExecCreate uses the manifest's exact numeric nested
[Backup artifact contract](../features/backups/artifacts.md) server identity, direct `/usr/local/bin/psql` argv, working directory
`/`, `Privileged=false`, `Tty=false`, `DetachKeys=""`, all three Attach flags
true, and the six manifest environment entries in their shown order after
sorted name-only removal of every inherited/configured key. Its attached stdin
is the compiled bounded SQL program, not `AdapterSecretStdinV1`; stdout/stderr
use the manifest bounds and exact result predicate. No shell, generic SQL,
secret argv/environment, separate ExecStart, or alternate `psql` path exists.

The exact role `COMMENT` marker in `{M:identity}` is the sole PostgreSQL
ownership marker. It binds the Backing Service id, generated identity, and raw
AdapterRevision hexadecimal value. Only the local superuser/CREATEROLE
authority may change a role comment; the managed login is explicitly
`NOCREATEROLE`, has no membership or ADMIN OPTION, and cannot change its own
marker. A database comment is neither required nor trusted. Database authority
is the exact same-name database owned by the exact safe marked role. Every
provision preflight completes before mutation. Foreign/partial name collisions
are untouched; compensation may drop only a role created by this exact attempt
after re-proving its marker, least-privilege attributes, lack of memberships,
and absence of any owned database. `role_create_prepared` is durably fenced by
Task, assignment, step, execution nonce, AdapterRevision, marker, and exact
`new` preflight before ExecCreate. After every normal/error/unknown outcome,
the total joint role/database readback returns exactly `created`, `absent`, or
`conflict`. Only `created` durably converts it to `new_role_this_attempt`; only
simultaneous role-and-database `absent` permits retry; every mixed/foreign state
conflicts untouched. Thus a crash after
role-creation COMMIT but before the compensation checkpoint is recovered from
the prepared fence without replaying `CREATE ROLE`. Clearing or replacing that
assignment removes compensation authority but retains the exact marked role
for ordinary same-intent recovery.

The PostgreSQL grant capability is intentionally non-owner DML access. It
grants `CONNECT`, public-schema `USAGE`, existing and owner-created future table
`SELECT|INSERT|UPDATE|DELETE`, and existing and owner-created future sequence
`USAGE|SELECT|UPDATE`. It does not grant ownership, schema `CREATE`, database
creation, role administration, `TRUNCATE`, `REFERENCES`, `TRIGGER`, function
execution, or other DDL/server authority. Revoke removes exactly both the
current-object and owner-scoped default privileges and proves effective
absence. Detach is assigned only after every durable inbound/outbound grant has
completed that revoke proof; it then proves the exact marker/owner pair,
disables new connections, terminates and proves absence of sessions for only
that database, drops the database, drops the role, and proves both names
absent. Every unknown result returns to the sealed state probe; a retained or
foreign database is never adopted as Detach success.

The exact Valkey procedure object is:

```text
{
  "schema":1,
  "adapter_key":"valkey:9",
  "adapter_contract_version":1,
  "runtime_major":9,
  "secret_transport":"task-secret-slot-v1",
  "exec_contract":"managed-adapter-exec-v1",
  "command_policy":{
    "kind":"valkey_acl_command_manifest",
    "domain":"groundplane.valkey9.attach-command-set.v1",
    "manifest_sha256":"4489ba6a2e7e12667136571fd84e752b1c018300cf6138a38d4701697396165a",
    "manifest_bytes":1918,
    "command_count":223,
    "source_commands_def_sha256":"a7243704d13f8cc72a84800fa31d6be49714e3c823e78fbb7745ca4caea41bfd"
  },
  "operations":[
    {"name":"ready","phase":"readiness","contract_id":"valkey9-ready-v1","secret_purposes":["managed_adapter_admin_password"],"idempotency":"read_only"},
    {"name":"provision","phase":"provision","contract_id":"valkey9-provision-v1","secret_purposes":["managed_adapter_admin_password","managed_adapter_attach_password"],"idempotency":"convergent_same_intent"},
    {"name":"detach","phase":"detach","contract_id":"valkey9-detach-v1","secret_purposes":["managed_adapter_admin_password"],"idempotency":"convergent_same_intent"}
  ]
}
```

The exact catalog objects are:

```text
{
  "schema":1,
  "adapter_key":"postgres:16",
  "adapter_contract_version":1,
  "runtime_major":16,
  "display_name":"PostgreSQL 16",
  "default_facts_prefix":"pg16_",
  "url_scheme":"pgsql://",
  "port":"5432",
  "fact_schema":[
    {"key":"DATABASE","secret":false},
    {"key":"HOST","secret":false},
    {"key":"PASSWORD","secret":true},
    {"key":"PORT","secret":false},
    {"key":"ROLE","secret":false},
    {"key":"URL","secret":true}
  ],
  "supports_grants":true,
  "backup_supported":true,
  "default_healthcheck":null,
  "strategy":"recreate",
  "replicas":1,
  "provision_summary":["Creates one dedicated PostgreSQL database and login role with a generated password.","Applies the declared cross-database grants."],
  "detach_summary":["Revokes declared grants and removes the dedicated database and login role."]
}

{
  "schema":1,
  "adapter_key":"valkey:9",
  "adapter_contract_version":1,
  "runtime_major":9,
  "display_name":"Valkey 9",
  "default_facts_prefix":"valkey9_",
  "url_scheme":"redis://",
  "port":"6379",
  "fact_schema":[
    {"key":"CHANNEL_PREFIX","secret":false},
    {"key":"HOST","secret":false},
    {"key":"KEY_PREFIX","secret":false},
    {"key":"PASSWORD","secret":true},
    {"key":"PORT","secret":false},
    {"key":"URL","secret":true}
  ],
  "supports_grants":false,
  "backup_supported":false,
  "default_healthcheck":null,
  "strategy":"recreate",
  "replicas":1,
  "provision_summary":["Creates one DB 0 ACL user with a generated password.","Limits keys and Pub/Sub channels to the published per-Attach prefixes; the application must configure both prefixes."],
  "detach_summary":["Deletes the ACL user, saves the ACL file, and verifies final absence."]
}
```

Registry validation requires exact equality of the entry, release record, and
every nested object's `adapter_key` and `adapter_contract_version`. It also
requires runtime major and Backup contract equality, and requires
`image_ref == managed_repository + "@" + managed_index_digest`. A revision
label preimage contains only the fixed label key and
`value_source="derived_adapter_revision"`; it never contains the derived
AdapterRevision value and therefore cannot form a cycle. Any semantic change
to a contract object requires a new contract id or value and creates a new
AdapterRevision.

### 2. Compile one immutable release ledger

The Controller and Agent compile the same closed ledger schema:

```text
ManagedAdapterReleaseLedger = {
  schema: 1,
  revision: SAFE_UINT_POSITIVE,
  entries: [{
    adapter_key: "postgres:16" | "valkey:9",
    adapter_revision: OCI_SHA256,
    state: "current" | "historical" | "revoked",
    revoked_in_revision: SAFE_UINT_POSITIVE | null,
    adapter_release_record: ManagedAdapterReleaseRecord,
    managed_image_build_contract: CLOSED_BUILD_CONTRACT,
    managed_image_release_record: CLOSED_RELEASE_RECORD,
    runtime_plan_contract: RuntimePlanContract,
    procedure_contract: ProcedureContract,
    catalog_contract: CatalogContract
  }]
}
```

Entries sort by `adapter_key` ascending UTF-8 bytes and then the raw 32
AdapterRevision bytes ascending. Duplicate or unsorted input is rejected; it
is not normalized at a trust boundary. `adapter_revision` must recompute from
`adapter_release_record`, every nested digest must recompute from its supplied
closed object, and the nested image release must recompute to
`managed_image_release_sha256`.

`state=current` and `state=historical` are executable. `state=revoked` is
readable but never executable. A current or historical entry has
`revoked_in_revision=null`. A revoked entry has a positive
`revoked_in_revision` no greater than the ledger revision. Exactly one
non-revoked current entry exists for each public key. A revoked entry cannot
be current.

The optional compiled-artifact self-digest is:

```text
managed_adapter_release_ledger_sha256 =
  D("groundplane.managed-adapter-release-ledger.v1",
    JCS(ManagedAdapterReleaseLedger))
```

It verifies local packaging and diagnostics only. It is not an Agent
authentication equality field because Controller and Agent binaries may
contain different harmless additional historical revisions.

Registry construction fails the process before readiness for any duplicate
tuple, duplicate current, malformed ordering, hash mismatch, invalid nested
release, mutable image reference, wrong runtime major, revoked current, or
missing current `postgres:16` or `valkey:9` entry. The only runtime lookup
operations are:

```text
Current(adapter_key)
Resolve(adapter_key, adapter_revision)
CurrentManaged()
```

`Current` is used only while first claiming a new Backing Service preparation.
Existing managed resources, lifecycle operations, Attach, Backup, Retry, and
Delete must call `Resolve` with their pinned revision. `CurrentManaged` returns
the two current executable managed entries in adapter-key byte order. Public
catalog assembly adds the separate constant manual variant; the managed
registry never fabricates a manual release.

The registry is immutable after construction. All revisions shipped in an MVP
binary remain compiled. There is no runtime collection or registry update. A
future binary may omit a descriptor only after a pre-activation audit proves
that no durable reference row exists for it.

### 3. Use exact reverse references, never an authoritative count

Every durable primary that requires an adapter revision has one or more exact
reverse rows. The logical row is:

```text
ManagedAdapterReference = {
  schema: 1,
  adapter_key: "postgres:16" | "valkey:9",
  adapter_revision: OCI_SHA256,
  reference_kind:
    "service" |
    "immutable_task_input" |
    "retry_authority" |
    "deletion_plan" |
    "retained_task",
  owner_id: id,
  owner_generation: SAFE_UINT_POSITIVE,
  ordinal: SAFE_UINT_POSITIVE
}
```

The key includes the tuple, reference-kind enum ordinal, owner id,
owner generation, and ordinal. Rows sort in that same order. An ordinal is
stable for a primary containing multiple references. It is not inferred from
iteration order.

The reference row is put or deleted in the same etcd transaction that
publishes or removes the owning primary. No integer refcount is authority. Any
aggregate count is a derived diagnostic only.

Every managed Backing Service persists one immutable generated release
snapshot:

```text
ManagedAdapterSnapshot = {
  schema: 1,
  adapter_key: "postgres:16" | "valkey:9",
  adapter_revision: OCI_SHA256,
  managed_image_release_sha256: HEX64,
  image_ref: OCI_REPOSITORY + "@" + OCI_SHA256,
  runtime_major: 16 | 9,
  adapter_contract_version: SAFE_UINT_POSITIVE,
  display_name: string
}
```

The Backing Service record uses one closed binding union:

```text
BackingAdapterBinding =
  managed {
    adapter_key: "postgres:16" | "valkey:9",
    adapter_revision: OCI_SHA256,
    snapshot: ManagedAdapterSnapshot
  }
| manual {
    adapter_key: "manual"
  }
```

The manual branch contains no omitted/null/fake release member and creates no
managed reverse-reference row. The same managed adapter key and revision are
sealed into create preparation, Service
desired state, the generated Compose artifact, Attach/lifecycle/Backup/Delete
plans, retry authority, and retained terminal evidence. The other snapshot
members are copied from the same validated compiled entry and never repaired
from `Current`.

Controller startup reads one linearizable etcd snapshot and proves both
directions: every reverse row names a matching primary containing the same
tuple, and every primary tuple has its required row. An unknown or missing
compiled revision degrades Controller readiness and blocks mutations, but
safe list/show reads remain available from immutable snapshots. Structural
compiled-registry corruption is process-fatal. Cleanup never drops a reference
because its executable descriptor is missing or revoked.

### 4. Lock the exact upstream Valkey 9 bytes

The exact upstream runnable authority is:

```text
repository:              docker.io/valkey/valkey
release:                 9.1.1-alpine3.24
OCI index:
  digest:                sha256:de31910896150d5e754a07d57d227cfdde4e258ddd0d1aa4607f2d2f95843715
  size:                  1216
  media type:            application/vnd.oci.image.index.v1+json
ARM64 child:
  digest:                sha256:80236c273b4a41011e96a13eaeb4fdba34808fad5a415652bc959783e535516a
  size:                  1616
  media type:            application/vnd.oci.image.manifest.v1+json
  platform.os:           linux
  platform.architecture: arm64
  platform.variant:      absent
OCI config:
  digest:                sha256:c6b941456ef0887bd5108424bf038ff7a727c07ad76381184c510b0effda19d2
  size:                  3198
  media type:            application/vnd.oci.image.config.v1+json
```

The authenticated upstream OCI config is exactly:

```text
Entrypoint:  ["docker-entrypoint.sh"]
Cmd:         ["valkey-server"]
User:        unset
WorkingDir:  /data
Env:
  PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
  VALKEY_VERSION=9.1.1
ExposedPorts:
  6379/tcp
Volumes:     unset
StopSignal:  unset
```

The source traceability facts are:

```text
container repository:   https://github.com/valkey-io/valkey-container.git
container commit:       e02f4d048b8b422624024ad28c4f7731bb2739b1
container directory:    9.1/alpine
Valkey repository:      https://github.com/valkey-io/valkey.git
Valkey tag commit:      d27f9ba65a04e80d9c417112a7621fc98a56f70d
source tar SHA-256:     7d7232acd1b8a49b4e05d07a00b3ca8c801ae06ab633ca6a3423bc5f385ab7ee
```

The ARM64 descriptor has no variant. Neither upstream evidence nor the
first-party managed descriptor may claim an authenticated `v8` variant.
`GOARM64=v8.0` is only the helper build baseline.

The registry bytes are executable authority. The source commits and tar digest
are traceability evidence, not reproducible-build proof. The upstream
publication has no authenticated attached SLSA statement proving source to
binary equivalence, and this ADR makes no stronger provenance claim.

Official source evidence is the immutable [Valkey container recipe](https://github.com/valkey-io/valkey-container/blob/e02f4d048b8b422624024ad28c4f7731bb2739b1/9.1/alpine/Dockerfile),
[entrypoint](https://github.com/valkey-io/valkey-container/blob/e02f4d048b8b422624024ad28c4f7731bb2739b1/9.1/alpine/docker-entrypoint.sh),
[Valkey 9.1.1 release](https://github.com/valkey-io/valkey/releases/tag/9.1.1),
and the [official container package](https://github.com/valkey-io/valkey-container/pkgs/container/valkey).

### 5. Build one cycle-free managed Valkey image contract

The level-one digest domain is exactly:

```text
groundplane.valkey9.managed-image-build-contract.v1
```

The complete level-one object is:

```text
ManagedValkey9BuildContract = {
  schema: 1,
  adapter_key: "valkey:9",
  adapter_contract_version: 1,

  upstream: {
    repository: "docker.io/valkey/valkey",
    release: "9.1.1-alpine3.24",
    index_digest:
      "sha256:de31910896150d5e754a07d57d227cfdde4e258ddd0d1aa4607f2d2f95843715",
    index_size_bytes: 1216,
    runnable_child_digest:
      "sha256:80236c273b4a41011e96a13eaeb4fdba34808fad5a415652bc959783e535516a",
    runnable_child_size_bytes: 1616,
    config_digest:
      "sha256:c6b941456ef0887bd5108424bf038ff7a727c07ad76381184c510b0effda19d2",
    config_size_bytes: 3198,
    platform: {
      os: "linux",
      architecture: "arm64",
      variant: null
    },
    source_container_commit:
      "e02f4d048b8b422624024ad28c4f7731bb2739b1",
    source_directory: "9.1/alpine",
    source_valkey_commit:
      "d27f9ba65a04e80d9c417112a7621fc98a56f70d",
    source_valkey_sha256:
      "7d7232acd1b8a49b4e05d07a00b3ca8c801ae06ab633ca6a3423bc5f385ab7ee"
  },

  helper: {
    directory_path: "/usr/local/libexec",
    directory_uid: 0,
    directory_gid: 0,
    directory_mode: 493,
    path: "/usr/local/libexec/groundplane-valkey9-helper",
    uid: 0,
    gid: 0,
    mode: 365,
    os: "linux",
    architecture: "arm64",
    elf_class: "ELF64",
    elf_data: "little-endian",
    elf_machine: "AArch64",
    linkage: "static",
    protocol_version: 1,
    secret_frame_version: 1,
    size_bytes: SAFE_UINT_POSITIVE,
    sha256: HEX64
  },

  configuration: {
    directory_path: "/usr/local/share/groundplane",
    directory_uid: 0,
    directory_gid: 0,
    directory_mode: 493,
    path: "/usr/local/share/groundplane/valkey9.conf",
    uid: 0,
    gid: 0,
    mode: 292,
    size_bytes: 302,
    sha256:
      "e5e290f22f14f6dc20640fecf6f93546326ef0f05538609ffc5f68fa41a85ec5"
  },

  runtime: {
    entrypoint: ["docker-entrypoint.sh"],
    image_cmd: ["valkey-server"],
    service_command: [
      "valkey-server",
      "/usr/local/share/groundplane/valkey9.conf"
    ],
    environment: [
      "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "VALKEY_VERSION=9.1.1"
    ],
    user: null,
    startup_uid: 0,
    server_uid: 999,
    server_gid: 1000,
    working_dir: "/data",
    exposed_port: "6379/tcp",
    data_path: "/data",
    acl_path: "/data/groundplane-users.acl",
    stop_signal: null,
    volumes: null
  },

  probes: {
    valkey_cli_version: "9.1.1",
    valkey_server_version: "9.1.1",
    runtime_major: 9,
    server_mode: "standalone",
    helper_environment: [
      "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "HOME=/nonexistent",
      "LC_ALL=C",
      "TZ=UTC"
    ]
  },

  image_labels: {
    adapter_contract_version_key:
      "com.groundplane.valkey9.adapter-contract-version",
    contract_sha256_key:
      "com.groundplane.valkey9.contract-sha256"
  }
}
```

The helper `size_bytes` and `sha256` are measured release inputs. They cannot
be filled until the static ARM64 helper is built. The contract digest is:

```text
contract_sha256 =
  D("groundplane.valkey9.managed-image-build-contract.v1",
    JCS(ManagedValkey9BuildContract))
```

The exact baked configuration is 302 UTF-8 bytes, uses LF endings, and has one
final LF:

```text
acl-pubsub-default resetchannels
aclfile /data/groundplane-users.acl
appendonly no
bind 0.0.0.0
daemonize no
databases 16
dbfilename dump.rdb
dir /data
logfile ""
port 6379
protected-mode yes
rdbchecksum yes
rdbcompression yes
save 3600 1 300 100 60 10000
stop-writes-on-bgsave-error yes
supervised no
```

Its SHA-256 is
`e5e290f22f14f6dc20640fecf6f93546326ef0f05538609ffc5f68fa41a85ec5`.
It explicitly retains RDB persistence and protected mode. An RDB file is live
runtime data; it is not a Groundplane Backup artifact.

The managed child adds the helper, configuration, and exactly these labels:

```text
com.groundplane.valkey9.adapter-contract-version=1
com.groundplane.valkey9.contract-sha256=<contract_sha256>
```

It adds no package, user, group, environment member, port, volume, entrypoint,
command, working directory, stop signal, runtime secret, ACL verifier, or
mutable tag.

The sole added OCI layer uses media type
`application/vnd.oci.image.layer.v1.tar` and is an uncompressed USTAR archive.
It contains exactly these entries in unsigned path-byte order:

| Path | Type | uid:gid | Mode | Size |
| --- | --- | --- | --- | ---: |
| `usr/local/libexec/` | directory | `0:0` | `0755` | 0 |
| `usr/local/libexec/groundplane-valkey9-helper` | regular | `0:0` | `0555` | measured helper bytes |
| `usr/local/share/groundplane/` | directory | `0:0` | `0755` | 0 |
| `usr/local/share/groundplane/valkey9.conf` | regular | `0:0` | `0444` | 302 |

Paths have no leading slash. Each entry begins with exactly one 512-byte POSIX
USTAR header. Bytes 0..99 are the exact path bytes followed by NUL padding;
mode is at 100..107, uid 108..115, gid 116..123, size 124..135, mtime
136..147, checksum 148..155, typeflag 156, linkname 157..256, magic 257..262,
version 263..264, uname 265..296, gname 297..328, devmajor 329..336,
devminor 337..344, prefix 345..499, and reserved padding 500..511.

`magic` is exactly `ustar` plus NUL and `version` is exactly ASCII `00`.
Mode, uid, and gid fields are seven zero-padded octal digits plus NUL. Size and
mtime are eleven zero-padded octal digits plus NUL; mtime is zero. Device fields
are seven ASCII zero digits plus NUL. The checksum field is six zero-padded
octal digits plus NUL plus space, computed as the unsigned sum of all 512 header
bytes while bytes 148..155 are eight ASCII spaces. Typeflag is ASCII `5` for a
directory and ASCII `0` for a regular file. `linkname`, `uname`, `gname`,
`prefix`, and reserved padding are all NUL bytes.

Directory payloads are empty. Regular payloads are byte-for-byte the measured
helper or the exact 302-byte configuration and are followed by the minimum NUL
padding to a 512-byte boundary. Exactly two terminal all-NUL 512-byte blocks
follow the final entry and no byte follows them. PAX or GNU headers, links,
whiteouts, xattrs, ACLs, file capabilities, devices, FIFOs, sockets, sparse
data, extra entries, and extended metadata are forbidden.

SHA-256 of those complete uncompressed tar bytes is the sole appended OCI
DiffID and, because the descriptor is uncompressed, the exact layer blob
digest. No compressor is part of authority. The upstream rootfs DiffIDs and
manifest layer descriptors remain the exact prefix; exactly this one layer is
appended.

The derived image config is not byte-equal to the upstream config. Verification
builds one closed inherited runtime projection containing Entrypoint, Cmd,
User, WorkingDir, ordered Env, ExposedPorts, Volumes, StopSignal, and every
other unchanged `Config` member, and requires it equal to the authenticated
upstream projection. Exactly three structures change:

- `rootfs.diff_ids` is the upstream list plus the one new DiffID;
- `history` is the upstream list plus exactly
  `{created:"1970-01-01T00:00:00Z",created_by:"groundplane.valkey9.managed-image-build-contract.v1",author:"Groundplane",comment:"managed Valkey 9 contract v1",empty_layer:false}`;
- `Config.Labels` is the upstream map plus the two exact Groundplane labels;
  an existing collision is fatal.

All other top-level and Config members are canonically equivalent to upstream.
The managed config, child, and index digests and sizes are registry-readback
outputs. They are never predicted from a local serializer.

### 6. Use one exact Valkey runtime and ACL authority

The authored backing Service omits `image`, `build`, `platform`, `pull_policy`,
`entrypoint`, `command`, and `user`. It may not override the adapter port,
release-owned health behavior, replicas, or strategy. The Controller injects
the managed digest image, exact service command, `recreate`, and one replica
from the pinned AdapterRevision. The resolved canonical bundle contains those
derived facts; authored desired state does not.

The authored and resolved Compose environment must not override
`VALKEY_EXTRA_FLAGS`, `VALKEYCLI_AUTH`, or `VALKEY_VERSION`, including through
an `env_file`. A mount may not shadow the helper, baked configuration,
entrypoint, or ACL file. The sole exception is the required persistent data
Volume mounted exactly at `/data`; that Volume owns
`/data/groundplane-users.acl`. No second or nested mount may cover that path.

At Backing Service creation, the Controller generates one stable administrative
password from exactly 32 random bytes, encoded as 43 unpadded base64url
characters. It persists only as encrypted subordinate state and is never
returned.

Before first container start, the Agent atomically seeds
`/data/groundplane-users.acl` as a regular, one-link file owned by `999:1000`
with mode `0600`. Its exact initial bytes, with one final LF, are:

```text
user default reset off
user groundplane-admin reset on sanitize-payload #<sha256-of-admin-password> db=0 +ping +info +acl|whoami +acl|setuser +acl|deluser +acl|save +acl|getuser +acl|dryrun
```

The password verifier is exactly 64 lowercase hexadecimal characters. Existing
safe ACL state is never regenerated or overwritten during Start. Missing,
unsafe, multiply linked, wrong-owner, or wrong-mode state after initial
creation is `state.conflict`; regenerating it would erase persisted Attach
users.

#### Crash-safe create-only ACL seed

ACL seed publication reuses ADR 0020's constrained short-lived materializer,
extended with one create-only managed-Volume output. The persistent Agent and
Valkey helper never open a host Volume path. Controller authority durably seals
the Service id, Volume id, AdapterRevision, bootstrap-secret generation,
destination `groundplane-users.acl`, uid 999, gid 1000, mode `0600`, exact byte
length, SHA-256, and one 32-byte seed-intent nonce. Plaintext ACL bytes remain
transient. Seed completion is Controller-acknowledged before container Start is
authorized.

The materializer helper runs as root with network none, read-only rootfs, no
Docker socket, device, or other mount, `no-new-privileges`, default seccomp,
all capabilities dropped except `CHOWN` and `FOWNER`, and exactly the pinned
named Volume mounted at `/run/groundplane/materialize`. It opens the Volume
root and every descendant descriptor-relatively with:

```text
RESOLVE_BENEATH
RESOLVE_NO_SYMLINKS
RESOLVE_NO_MAGICLINKS
RESOLVE_NO_XDEV
```

It requires root and destination device identity to remain constant. The sole
reserved temp is `.gp-acl-seed-<64-lowercase-hex-intent-nonce>.tmp`. It may
reconcile only that exact sealed-intent name.

When the destination is absent, it creates the unpredictable temp exclusively,
writes and hashes the exact bytes, requires one additional EOF, calls
`fdatasync`, `fchown(999,1000)`, and `fchmod(0600)`, then calls
`fsync(temp_fd)` so final content, uid, gid, and mode are durable before it
`fstat`s and rehashes.
The temp must be regular, one link, on the root device, and have the exact
inode, uid, gid, mode, length, and digest just produced. Publication is one
`renameat2(RENAME_NOREPLACE)`. The helper reopens by name, proves the published
inode and every exact fact, calls `fsync(published_fd)`, then fsyncs the opened
root directory. It never overwrites or repairs a destination.

Unknown outcome and retry have exactly three destination outcomes:

```text
absent:
  rerun the same create-only intent
present and exact initial type/nlink/uid/gid/mode/length/digest:
  adopt success, fsync the opened published fd, then fsync the root directory
present with any mismatch:
  state.conflict; retain encrypted admin secret and durable intent
```

`EEXIST` after rename uses the same exact inspection. Orphan temp cleanup is
descriptor-relative, accepts only a regular one-link temp whose name, inode,
metadata, and digest belong to the same sealed intent, then unlinks and fsyncs;
anything else is `state.conflict`.

Later Start never calls seed and never compares live ACL bytes to the initial
bytes because `ACL SAVE` legitimately evolves them. It verifies safe metadata,
starts the server, and requires authenticated readiness to prove admin state.
After every `ACL SAVE`, the Agent opens the already-attested `/data` root and
`groundplane-users.acl` descriptor-relatively with
`RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV`.
It requires the root device to equal the sealed Volume device, takes two
`fstat`s around the proof, and requires the opened ACL device/inode pair to be
unchanged between them, regular, one link, `999:1000`, and `0600`. Pathname
`stat`, link following, or a substituted object cannot satisfy the predicate.
This descriptor proof is part of ready, provision, and detach success after
their last applicable `ACL SAVE`. Release/runtime probes verify Valkey 9.1.1
preserves those facts; implementation does not assume it without proof.

The attested static helper is the only Valkey administrative client. It opens
only `127.0.0.1:6379`, encodes RESP arrays directly, owns and clears mutable
secret/reply buffers, and never invokes a shell or `valkey-cli`. Docker Exec
invokes `/usr/local/libexec/groundplane-valkey9-helper` directly. `/usr/bin/env`
is not an executable prefix or interception boundary.

The exact non-secret suffix is one of:

```text
run,1,<nonce>,<hard-deadline-unix-nano>,ready
run,1,<nonce>,<hard-deadline-unix-nano>,provision,<role>
run,1,<nonce>,<hard-deadline-unix-nano>,detach,<role>
```

These are argv elements separated by commas only for presentation. Each token
is a distinct argv element. `nonce`, deadline, and resolved Attach role are
sealed in the immutable plan; the helper never derives them from environment
or Docker labels.

`ready` has no role. `provision` and `detach` require exactly one role argv
element, 8 through 63 ASCII bytes, matching:

```text
^[A-Za-z0-9_.-]{1,56}_[0-9a-hjkmnp-tv-z]{6}$
```

The suffix is lowercase Crockford Base32 from characters 10 through 15 of the
canonical Attach ULID random tail. The prefix is the already validated Service
name. This is the existing `attachProvisionIdentity`; it is not a second
Valkey username grammar. Unicode, control/space, slash, colon, glob, escape,
case folding, normalization, and truncation are forbidden. The Agent also
compares the role byte-for-byte with the sealed plan value; syntax alone is
not authority.

The hard deadline argv is a canonical unsigned base-10 signed-64-bit Unix
nanosecond value matching `[1-9][0-9]{0,18}`, with no sign or leading zero and
value no greater than `MaxInt64`. At entry and immediately before every
blocking action, the helper requires `now < D`. Secret-frame reads are
cancelable or polled to `D`. Dial is TCP4 to the exact literal
`127.0.0.1:6379` with `Dialer.Deadline=D`. Immediately after connection the
helper calls `SetDeadline(D)`, so every RESP write and read uses the same
absolute deadline. The second provision connection uses the same `D`. The
helper never derives, resets, extends, rounds, or replaces it with a relative
or per-operation timeout.

Secret stdin is the `valkey9-secret-frame-v1` branch of the generic
`AdapterSecretStdinV1` owned-byte type. Framing is exactly:

```text
8 bytes: ASCII "GPVK9SF1"
1 byte:  secret count
for each secret:
  1 byte:  kind
  2 bytes: unsigned big-endian byte length
  N bytes: secret
no trailing bytes
```

Secret kinds are:

```text
1 = administrative_password
2 = attach_password
```

Every password is exactly 43 bytes. `ready` and `detach` require exactly kind
1 and therefore exactly 55 frame bytes. `provision` requires exactly kinds 1
then 2 and therefore exactly 101 frame bytes:

```text
8 + 1 + (1 + 2 + 43)       = 55
8 + 1 + 2 * (1 + 2 + 43)   = 101
```

The maximum accepted frame is 101 bytes. Golden tests require 55-byte ready,
55-byte detach, and 101-byte provision success; 100-byte provision is
truncated, 101 is valid, and 102 has forbidden trailing data. Duplicate,
missing, reordered, unknown, wrong-length, truncated, or trailing data fails
before a Valkey connection.

#### Sole secret-bearing managed-adapter Exec

This section cleanly generalizes [Backup execution contract](../features/backups/agent-protocol.md)'s Moby Exec authority with one
`ManagedAdapterExecV1` branch. [Backup execution contract](../features/backups/agent-protocol.md) continues to own the Moby boundary;
this ADR supplies Valkey argv, stdin, result, and recovery policy. Before
ExecCreate and again immediately before attached start, the Agent reattests the
exact Service, container id, AdapterRevision, managed index image, release
labels, and complete mount multiset.

Valkey ExecCreate is exactly:

```text
User:          "999:1000"
Privileged:    false
Tty:           false
WorkingDir:    "/data"
DetachKeys:    ""
AttachStdin:   true
AttachStdout:  true
AttachStderr:  true
Cmd[0]:        "/usr/local/libexec/groundplane-valkey9-helper"
Cmd[1:]:       sealed non-secret action argv
ConsoleSize:   zero
```

No secret is present in argv, Env, labels, plan hash, checkpoint, inspect, or
log. To prevent inherited/configured environment interception under Moby API
1.52, the Agent validates the attested container `Config.Env`, rejects malformed
or duplicate names, emits one name-only removal entry for every inherited name
in ascending byte order, then appends exactly these four values in this order:

```text
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HOME=/nonexistent
LC_ALL=C
TZ=UTC
```

API 1.52 treats a name without `=` as an Exec environment removal. The helper's
first action, before reading stdin or dialing, requires its actual environment
to contain exactly those four entries. The behavior is release-tested against
Moby 29.1.3/API 1.52. It does not depend on or expose authored Service
environment.

The Exec starts only through one `ContainerExecAttach` call, the client binding
of `POST /exec/{id}/start`, with `Detach=false`, `Tty=false`, and zero
ConsoleSize. There is no separate ExecStart and no second start. Concurrently,
the Agent writes the exact 55- or 101-byte owned frame, requires a complete
write, and calls `CloseWrite` exactly once to deliver EOF without closing the
read side. It demultiplexes the TTY-false raw stream with `stdcopy.StdCopy`,
drains both streams under their caps, and requires stream EOF. It then requires
ExecInspect `ContainerID` equality, `Running=false`, normal exit, and the exact
exit/result rule. Short or extra input, CloseWrite failure, raw-stream failure,
cap overflow, inspect mismatch, or missing terminal proof is never success.

There is no reattach or reuse after a lost hijack. The Agent waits and inspects
the old exec until it proves `Running=false`. Only then may ready, provision,
or detach create a fresh exec with byte-identical argv and frame under the same
nonce and deadline, because all three operations are convergent. Execs never
overlap. If the old exec remains running at the absolute deadline, the Task
retains unknown/recovery state and does not start another. Exit 10 follows the
same rule. A later Controller Retry uses a new nonce and deadline but the same
persisted password for an existing role; it never substitutes a new password
under the old role intent.

Canonical desired mount targets and every ContainerInspect destination are
compared by path components. The complete observed Mounts multiset must equal
the sealed set. A target equal to, inside, or an ancestor of any of these is
rejected:

```text
/bin
/sbin
/lib
/lib64
/usr
/etc
/run
/var/run
/proc
/sys
/dev
/usr/local/bin/docker-entrypoint.sh
/usr/local/libexec/groundplane-valkey9-helper
/usr/local/share/groundplane/valkey9.conf
```

String-prefix comparison is forbidden. The sole exception is one exact
read-write `volume` mount at `/data`, whose stable Volume id, Docker name,
source, type, mode, read/write flag, and propagation are sealed and byte-equal
on inspect. Every other target equal to, below, or above `/data` is rejected,
including `/data/...`; this prevents another mount from shadowing the ACL file.
The explicit protected paths under `/usr` are diagnostic redundancy.

`ready` executes and verifies this fixed sequence:

```text
AUTH groundplane-admin <administrative_password>
ACL WHOAMI                         -> groundplane-admin
PING                               -> PONG
INFO server                        -> valkey_version:9.1.1
INFO server                        -> valkey_mode:standalone
INFO replication                   -> role:master
ACL GETUSER groundplane-admin      -> exact required admin capability
```

The admin `GETUSER` reply uses RESP2 and is parsed as a top-level array of
exactly 14 elements forming seven unique field/value pairs. Pair order is not
authority. The helper rejects a missing, duplicate, or unknown field and every
wrong RESP type. The normalized predicate is exactly:

```text
flags:      array set exactly {on,sanitize-payload}, no duplicate
passwords:  array containing exactly the expected 64-byte lowercase verifier
commands:   bulk string; first token -@all; then exactly eight unique +fullname tokens
keys:       zero-length bulk string
channels:   zero-length bulk string
databases:  bulk string db=0
selectors:  empty array
```

The eight command fullnames normalize by stripping `+`, sorting unsigned ASCII
bytes, and serializing one name plus LF including final LF:

```text
acl|deluser
acl|dryrun
acl|getuser
acl|save
acl|setuser
acl|whoami
info
ping
```

Those bytes are exactly 77 bytes and
`D("groundplane.valkey9.admin-command-set.v1", bytes) =
56517c2acf1dbe3c184918288f8188592e13fa0595c7d5addd22fe5a41f9fe8c`.
Category, negative, first-argument, duplicate, or extra tokens fail. The raw
pinned 9.1.1 rendering may retain rule application order; semantic proof uses
only this normalized set. The explicit baked
`acl-pubsub-default resetchannels`, explicit admin `db=0`, and exact empty
key/channel predicates are release authority. This admin proof and the
descriptor-relative ACL-file proof above are both required by ready and after
the final `ACL SAVE` in provision/detach.

`provision` executes and verifies:

```text
AUTH groundplane-admin <administrative_password>
ACL SETUSER <role> reset on sanitize-payload #<attach-password-sha256>
  ~<role>:* &<role>:* db=0 <fixed-command-grants>
ACL SAVE
ACL GETUSER <role>
new connection
AUTH <role> <attach_password>
PING
```

The Attach scope is exactly database 0, key prefix `<role>:`, and Pub/Sub
channel prefix `<role>:`. The helper computes lowercase hexadecimal SHA-256 of
the exact 43 password bytes and passes only `#<verifier>` to `ACL SETUSER`.
`reset` clears prior passwords, key/channel/database selectors, and commands.
There is exactly one password verifier, one key rule `~<role>:*`, one channel
rule `&<role>:*`, one database rule `db=0`, and no category, negative,
first-argument, `nopass`, allkeys, allchannels, alldbs, allcommands, or second
selector/password/pattern/database rule.

The fixed command manifest is exactly 1,918 ASCII bytes: these 223 lowercase
full command names in ascending unsigned-byte order, one per LF-terminated line
including the final LF:

```text
append
bitcount
bitfield
bitfield_ro
bitop
bitpos
blmove
blmpop
blpop
brpop
brpoplpush
bzmpop
bzpopmax
bzpopmin
client|getname
client|id
client|info
client|setinfo
client|setname
command|info
copy
decr
decrby
del
delifeq
discard
dump
echo
eval
eval_ro
evalsha
evalsha_ro
exec
exists
expire
expireat
expiretime
geoadd
geodist
geohash
geopos
georadius
georadiusbymember
georadiusbymember_ro
georadius_ro
geosearch
geosearchstore
get
getbit
getdel
getex
getrange
getset
hdel
hello
hexists
hexpire
hexpireat
hexpiretime
hget
hgetall
hgetdel
hgetex
hincrby
hincrbyfloat
hkeys
hlen
hmget
hmset
hpersist
hpexpire
hpexpireat
hpexpiretime
hpttl
hrandfield
hscan
hset
hsetex
hsetnx
hstrlen
httl
hvals
incr
incrby
incrbyfloat
lcs
lindex
linsert
llen
lmove
lmpop
lpop
lpos
lpush
lpushx
lrange
lrem
lset
ltrim
memory|usage
mget
mset
msetex
msetnx
multi
object|encoding
object|freq
object|idletime
object|refcount
persist
pexpire
pexpireat
pexpiretime
pfadd
pfcount
pfmerge
ping
psetex
psubscribe
pttl
publish
punsubscribe
quit
rename
renamenx
rpop
rpoplpush
rpush
rpushx
sadd
scard
script|exists
script|load
sdiff
sdiffstore
select
set
setbit
setex
setnx
setrange
sinter
sintercard
sinterstore
sismember
smembers
smismember
smove
spop
spublish
srandmember
srem
sscan
ssubscribe
strlen
subscribe
substr
sunion
sunionstore
sunsubscribe
time
touch
ttl
type
unlink
unsubscribe
unwatch
wait
waitaof
watch
xack
xadd
xautoclaim
xclaim
xdel
xgroup|create
xgroup|createconsumer
xgroup|delconsumer
xgroup|destroy
xgroup|setid
xinfo|consumers
xinfo|groups
xinfo|stream
xlen
xpending
xrange
xread
xreadgroup
xrevrange
xsetid
xtrim
zadd
zcard
zcount
zdiff
zdiffstore
zincrby
zinter
zintercard
zinterstore
zlexcount
zmpop
zmscore
zpopmax
zpopmin
zrandmember
zrange
zrangebylex
zrangebyscore
zrangestore
zrank
zrem
zremrangebylex
zremrangebyrank
zremrangebyscore
zrevrange
zrevrangebylex
zrevrangebyscore
zrevrank
zscan
zscore
zunion
zunionstore
```

Its authority is:

```text
SHA-256(
  ASCII("groundplane.valkey9.attach-command-set.v1") || 0x00 || manifest
) = 4489ba6a2e7e12667136571fd84e752b1c018300cf6138a38d4701697396165a
```

It is derived from Valkey 9.1.1 commit
`d27f9ba65a04e80d9c417112a7621fc98a56f70d`; exact generated
`src/commands.def` SHA-256 is
`a7243704d13f8cc72a84800fa31d6be49714e3c823e78fbb7745ca4caea41bfd`.
The helper appends one `+<fullname>` token per manifest line in that order.
The SETUSER RESP array has exactly 233 elements: `ACL`, `SETUSER`, role, seven
pre-grant rules, and 223 grants. Normalized GETUSER `commands` is exactly 2,146
bytes: `-@all` followed by 223 space-prefixed `+name` tokens.

`ACL GETUSER` is parsed as a RESP2 top-level order-independent map with exactly
the seven fields `flags`, `passwords`, `commands`, `keys`, `channels`,
`databases`, and `selectors`, each exactly once. Missing, duplicate, unknown,
or wrong-type fields fail. Normalized proof requires:

```text
flags:       set exactly {on, sanitize-payload}
passwords:   exactly [expected_verifier]
keys:        exactly bulk "~<role>:*"
channels:    exactly bulk "&<role>:*"
databases:   exactly "db=0"
selectors:   exact empty array
commands:    first token exactly -@all; remaining unique +<fullname> tokens
```

Command proof strips `+`, sorts unsigned ASCII, emits one name plus LF including
the final LF, and requires count 223, length 1,918, and the domain digest above.
Category grants, negatives after `-@all`, first-argument rules, future/module
commands, or raw-order-only comparison fail closed.

Authenticated `ACL DRYRUN <role> ...` must return exact simple-string `OK` for:

```text
PING
HELLO 2
SELECT 0
SET <key-prefix>probe value
GET <key-prefix>probe
MGET <key-prefix>a <key-prefix>b
COPY <key-prefix>a <key-prefix>b
PUBLISH <channel-prefix>probe x
SUBSCRIBE <channel-prefix>probe
PSUBSCRIBE <role>:*
MULTI
EVAL "return 1" 0
EVALSHA <known-loaded-sha> 0
SCRIPT EXISTS <sha>
SCRIPT LOAD "return 1"
```

Negative DRYRUN uses exact RESP2
`E(["ACL","DRYRUN",R]+target)`, where `R` is the role byte string and `E`
emits canonical array and bulk-string length framing. A denial is a RESP bulk,
not an error. With byte concatenation, its exact payload is one of:

```text
C(f) = "User " + R + " has no permissions to run the '" + f + "' command"
K(v) = "User " + R + " has no permissions to access the '" + v + "' key"
H(v) = "User " + R + " has no permissions to access the '" + v + "' channel"
D(v) = "User " + R + " has no permissions to access database " + v
```

Their byte lengths are respectively `46+len(R)+len(f)`,
`45+len(R)+len(v)`, `49+len(R)+len(v)`, and `44+len(R)+len(v)`. The key cases
below use `K("foreign:probe")`; publish and subscribe use
`H("foreign:probe")`; global PSUBSCRIBE uses `H("*")`; narrow PSUBSCRIBE uses
`H(R+":events:*")`; SELECT, COPY DB, and MOVE DB 1 use `D("1")`; FLUSHALL uses
`D("FLUSHALL")`; and every command case uses `C` with the lowercase command
fullname shown. Missing user, unknown command, or wrong arity produces a RESP
error and is not denial proof. These payloads are locked to Valkey source
commit `d27f9ba65a04e80d9c417112a7621fc98a56f70d`; they are compared by exact
RESP kind and bytes with no trim, case fold, or substring match. These commands
prove the stated denial dimension:

```text
key:      GET foreign:probe
key:      MGET <key-prefix>probe foreign:probe
key:      COPY <key-prefix>probe foreign:probe
channel:  PUBLISH foreign:probe x
channel:  SUBSCRIBE foreign:probe
channel:  PSUBSCRIBE *
channel:  PSUBSCRIBE <role>:events:*
database: SELECT 1
database: COPY <key-prefix>a <key-prefix>b DB 1
database: MOVE <key-prefix>k 1
database: FLUSHALL
command:  MOVE <key-prefix>k 0
command:  ACL WHOAMI
command:  CONFIG GET *
command:  MODULE LIST
command:  INFO server
command:  MONITOR
command:  DEBUG HELP
command:  SHUTDOWN NOSAVE
command:  REPLICAOF NO ONE
command:  BGSAVE
command:  SAVE
command:  FLUSHDB
command:  KEYS *
command:  SCAN 0
command:  RANDOMKEY
command:  DBSIZE
command:  MIGRATE x 1 k 0 1
command:  RESTORE <key-prefix>k 0 x
command:  SORT <key-prefix>k
command:  FUNCTION LIST
command:  FCALL f 0
command:  SCRIPT FLUSH
command:  SCRIPT SHOW 0000000000000000000000000000000000000000
command:  CLIENT LIST
command:  CLIENT KILL ID 1
command:  CLUSTER NODES
command:  PSYNC ? -1
command:  MEMORY STATS
command:  SLOWLOG GET
command:  LATENCY LATEST
command:  COMMANDLOG GET
```

On a fresh authenticated application connection, actual Lua execution must
also prove that `GET foreign:probe`, `PUBLISH foreign:probe x`, `SELECT 1`, and
`INFO server` each fail ACL checks inside a script, while a prefixed SET/GET
script succeeds. The four negative Lua replies are explicitly designated
expected-error proof slots. They require RESP Error and byte equality with,
respectively:

```text
ERR ACL failure in script: No permissions to access a key script: on @user_script:1.
ERR ACL failure in script: No permissions to access a channel script: on @user_script:1.
ERR ACL failure in script: No permissions to access database script: on @user_script:1.
ERR ACL failure in script: User <role> has no permissions to run the 'info' command script: on @user_script:1.
```

The first three complete RESP wire lengths are exactly 87, 91, and 90 bytes;
the INFO wire length is `107+len(role)`. After the parser removes the exact
leading `-` and final CRLF, the compared payload lengths are 84, 88, 87, and
`104+len(role)`. A matching expected Error is proof success for only that slot.
Every other RESP Error remains exit 8. ACL is re-applied inside Lua, but cannot
bound script CPU or memory; allowing scripting retains shared-instance
availability risk.

MULTI, EXEC, WATCH, UNWATCH, and DISCARD are allowed and every queued command
remains independently ACL-checked. Pub/Sub permits the listed publish,
subscribe, and unsubscribe variants; Pub/Sub introspection remains denied.
Valkey compares a PSUBSCRIBE request literally with an ACL channel pattern, so
`&<role>:*` permits PSUBSCRIBE only for the exact requested pattern
`<role>:*`. A narrower dynamic pattern such as `<role>:events:*` and `*` are
both denied. PUNSUBSCRIBE without arguments remains allowed because it clears
only the current connection. Arbitrary application PSUBSCRIBE patterns and
safe cross-Attach isolation cannot both be offered without predeclaring every
exact pattern.

Database-global SCAN, KEYS, RANDOMKEY, DBSIZE, FLUSHDB, and cross-database MOVE
remain denied because a prefix cannot constrain their output or effect. The
helper never treats SETUSER/SAVE without normalized GETUSER, DRYRUN, fresh
AUTH/PING, and Lua proof as completion.

`AUTH` is intentionally absent from the 223 grants. Valkey 9.1.1 marks it
`CMD_NO_AUTH`, so ACL command-bit checks skip it. HELLO, PING, SELECT,
CLIENT SETINFO/SETNAME/GETNAME/ID/INFO, and COMMAND INFO are explicitly granted
for RESP negotiation, DB-0 URL selection, client metadata/name, and safe
feature inspection.

The generated role and password contain only URI-unreserved bytes. The URL fact
is exactly:

```text
redis://<role>:<password>@<host>:6379/0
```

`KEY_PREFIX` and `CHANNEL_PREFIX` are both exactly `<role>:` and non-secret.
The URL cannot carry either prefix, so an application must consume/configure
both facts. This contract supports cache read/write/expiry/counters, atomic
lock/rate-limit/queue Lua, lists, sorted sets, streams, transactions, and exact
or fixed-prefix Pub/Sub. It intentionally does not support database-global
flush, prefix enumeration, random-key/DBSIZE, cross-database MOVE, arbitrary
PSUBSCRIBE subpatterns, client-side tracking, functions, or server/script
administration. It is not a transparent URL-only or every-Redis-API Attach.

`detach` executes and verifies:

```text
AUTH groundplane-admin <administrative_password>
ACL DELUSER <role>
ACL SAVE
ACL GETUSER <role>                 -> absent
```

A delete count of zero or one is idempotent success only when `ACL SAVE`
succeeds and final absence is proven. Valkey supports no cross-Attach grants in
the MVP; grant and revoke procedures are closed unsupported operations, not
empty successful procedure lists.

The helper uses RESP2 only. It sends only arrays of bulk-string command
arguments and never sends `HELLO`, pipelines commands, subscribes, or accepts
RESP3/push types. The closed parser limits are:

```text
maximum_RESP_reply_frame_bytes:      65536
maximum_RESP_reply_aggregate_bytes:  262144
maximum_RESP_line_bytes:             1024
maximum_RESP_nesting_depth:          8
top_level_depth:                     1
maximum_decoded_values_per_reply:    256
```

The frame bound counts one complete top-level encoded reply exactly as read
from the socket, including type/length prefixes, nested members, payload, and
CRLF. The aggregate counts encoded reply bytes across every command and both
provision connections and is not reset on reconnect. A declared size or count
is rejected before allocation when it cannot fit every remaining bound. The
reader consumes at most bound plus one byte to identify overflow and never
allocates from an unchecked length.

The exact success records are:

```text
ready:      ASCII "GPVK9/1 ready ok\n"       (17 bytes)
provision:  ASCII "GPVK9/1 provision ok\n"   (21 bytes)
detach:     ASCII "GPVK9/1 detach ok\n"      (18 bytes)
```

No CR, NUL, role, nonce, deadline, password, verifier, hash, RESP value, or
other byte is permitted. The record is written in one write only after the
action's final proof. Agent success requires exit 0, byte-equal operation
record, EOF, and zero stderr. Stdout is capped at 21 bytes and sanitized stderr
is capped at exactly zero bytes for every invocation. A failed or short
terminal write is exit 10; any partial prefix is not success. Numeric exit plus
the Agent-owned static mapping is the sole failure diagnostic. Upstream error
text, RESP/ACL error text, INFO bodies, ACL GETUSER replies, password hashes,
and verifiers are discarded or retained only in owned internal buffers and are
never stdout, stderr, or log fields.

The helper declares explicit numeric constants, never `iota`, for this complete
normal exit table:

| Exit | Name | Exact meaning |
| --- | --- | --- |
| 0 | `success` | final proof completed and the exact terminal record was fully written |
| 1 | `request_invalid` | malformed, extra, or reordered argv; protocol/version/nonce/deadline/action/role failure; or malformed, missing, duplicate, reordered, wrong-length, truncated, or trailing secret frame, all before connect |
| 2 | `environment_invalid` | exact environment, uid/gid, cwd, executable, fd/stream, or runtime-envelope check failed before connect |
| 3 | `deadline_exceeded` | absolute deadline was nonfuture at entry or expired while reading the frame, dialing, writing, reading, proving, or writing the terminal record |
| 4 | `connect_failed` | exact TCP4 dial failed while the absolute deadline still had time |
| 5 | `transport_failed` | post-connect write, read, EOF, reset, or close failed before a complete expected reply while time remained |
| 6 | `reply_limit_exceeded` | top-level encoded reply, invocation aggregate, line, element, or nesting bound would be exceeded |
| 7 | `reply_invalid` | complete bytes violated the closed canonical RESP2 grammar, had the wrong type or shape, contained unsolicited/extra data, or could not be parsed unambiguously |
| 8 | `valkey_error` | a well-formed RESP2 error occupied a response slot other than one of the four exact Lua expected-error proof slots, or failed byte equality in one of those slots; its payload is discarded after classification and never emitted |
| 9 | `proof_mismatch` | well-formed non-error replies did not prove the exact WHOAMI, PONG, INFO, ACL SETUSER/SAVE/GETUSER/DELUSER state |
| 10 | `result_write_failed` | mutation/readback proof completed but the one exact stdout terminal record was not fully written |
| 11 | `internal_failure` | implementation invariant or owned-buffer/allocation failure not assigned above |

Any normal exit outside 0 through 11 is forbidden. Signal death or non-normal
Docker Exec termination is not remapped; the Agent treats it as unknown
execution and applies the same recovery/retry authority. Precedence is
request, environment, deadline, then the stage-specific code. Deadline wins
over a simultaneous underlying I/O error. Once a complete RESP error exists it
is exit 8, never exit 7 or 9, except that byte equality in one of the four
explicit Lua expected-error proof slots is success for that proof slot.

`ready` is read-only and safe to repeat. `provision` and `detach` are
convergent/idempotent only for byte-identical sealed helper intent and secret
frame while the same deadline remains. Provision repeats `reset` with the same
password and rules, saves, reads back, and proves fresh AUTH/PING. Detach
accepts delete count zero or one, then saves and proves absence. No intermediate
reply is success and there is no rollback.

Exits 4, 5, 8, and 10 may retry with bounded backoff only while `D` remains and
only with byte-identical argv and frame. Exits 1, 2, 6, 7, 9, and 11 fail
without automatic retry. Exit 3 cannot retry under the expired authority. Any
changed role, password, nonce, or deadline is a new Controller-authorized
execution, never a helper retry. A different password must not be substituted
for the same role.

Owned mutable command, reply, password, verifier, and frame buffers are cleared
after proof or failure. This contract does not claim global Go-runtime,
`net/http`, `os/exec`, kernel, or server-memory zeroization. Valkey upstream
defines no safe response-size, diagnostic, or deadline bound; the numeric
limits above are Groundplane fail-closed client constants.

Valkey's official [ACL documentation](https://valkey.io/topics/acl/) defines
SHA-256 password verifiers, `reset`, external ACL files, and `ACL SAVE`.
Authenticated [PING](https://valkey.io/commands/ping/) is part of readiness
because it fails while persistence is loading or the server cannot serve data.

### 7. Publish the managed Valkey release only after registry readback

The level-two digest domain is exactly:

```text
groundplane.valkey9.managed-image-release-record.v1
```

The closed object is:

```text
ManagedValkey9ReleaseRecord = {
  schema: 1,
  contract_sha256: HEX64,
  managed_repository: OCI_REPOSITORY,
  managed_index_digest: OCI_SHA256,
  managed_arm64_child_digest: OCI_SHA256,
  platform: {
    os: "linux",
    architecture: "arm64",
    variant: null
  }
}
```

`managed_repository` is a first-party release input. The two managed digests
are outputs read back from that registry after push. They are intentionally not
guessed or selected in this ADR. The release digest is:

```text
managed_release_sha256 =
  D("groundplane.valkey9.managed-image-release-record.v1",
    JCS(ManagedValkey9ReleaseRecord))
```

Release automation must, in order:

1. Build the static ARM64 helper and measure its exact size and SHA-256.
2. Materialize and verify the exact 302-byte configuration.
3. Construct and hash the complete level-one contract before image build.
4. Build without network from the exact upstream ARM64 child.
5. Add only the exact four-entry uncompressed USTAR layer and two labels.
6. Verify the closed inherited runtime projection, ordered `Config.Env`, and
   the three permitted derived config changes above.
7. Verify the upstream rootfs is an exact prefix and exactly one specified
   Groundplane layer was added.
8. Verify file type, uid, gid, mode, size, digest, ELF identity, and static
   linkage.
9. Run `valkey-cli --version` and `valkey-server --version` and require exact
   `9.1.1`.
10. Launch with the exact service command and seeded ACL state.
11. Prove unauthenticated PING fails and administrative readiness succeeds.
12. Prove Attach provision persists across stop/start through `ACL SAVE`.
13. Prove Detach persists across stop/start.
14. Push to the first-party repository.
15. Fetch the managed index, child, config, and added layer from the registry
    rather than trusting local build output.
16. Require exactly one runnable `linux/arm64` child with variant absent.
17. Publish the level-two record only after every readback and runtime probe
    succeeds.

The Valkey AdapterRevision cannot be `current` until its measured helper,
level-one contract, registry repository/digests, level-two record, runtime,
procedure, and catalog preimages are all present and self-consistent in the
compiled ledger.

### 8. Close the Valkey adapter contract

The Valkey runtime plan contract has these exact product values:

```text
schema:                         1
adapter_key:                    valkey:9
adapter_contract_version:       1
platform:                       linux/arm64, variant absent
runtime_major:                  9
entrypoint:                     ["docker-entrypoint.sh"]
image_cmd:                      ["valkey-server"]
service_command:                ["valkey-server", "/usr/local/share/groundplane/valkey9.conf"]
working_dir:                    /data
startup_user:                   image default, uid 0
server_user:                    999:1000
port:                           6379/tcp
data_mount_target:              /data
strategy:                       recreate
replicas:                       1
default_healthcheck:            null
readiness_kind:                 compiled_authenticated_valkey9
backup_contract:                none
```

Generated Compose adds these release labels in addition to the ordinary stable
Groundplane ownership labels:

```text
com.groundplane.adapter-key=valkey:9
com.groundplane.adapter-revision=<sha256:HEX64>
com.groundplane.managed-image-release-sha256=<HEX64>
```

The Valkey catalog contract values are the exact closed `CatalogContract`
object above. In projection form they include:

```text
display_name:          Valkey 9
default_facts_prefix:  valkey9_
url_scheme:            redis://
port:                  6379
fact_schema:
  CHANNEL_PREFIX:  secret=false
  HOST:      secret=false
  KEY_PREFIX:      secret=false
  PASSWORD:  secret=true
  PORT:      secret=false
  URL:       secret=true
supports_grants:       false
backup_supported:      false
default_healthcheck:   null
strategy:              recreate
replicas:              1
```

Fact schema entries are in fact-key byte order. The generated role remains
embedded in the secret URL; Valkey does not publish a separate `ROLE` fact in
the MVP. Its URL selects DB 0. `KEY_PREFIX=<role>:` and
`CHANNEL_PREFIX=<role>:` are mandatory application configuration because a URL
cannot encode either isolation boundary. The role is confined to DB 0, keys
matching its one prefix, channels matching its one prefix, and the fixed
223-command allowlist. It is not a user across the configured database set.

The PostgreSQL catalog contract retains its accepted [Backup artifact contract](../features/backups/artifacts.md)/0048 semantics
and uses a fact-key-byte-sorted schema:

```text
DATABASE:  secret=false
HOST:      secret=false
PASSWORD:  secret=true
PORT:      secret=false
ROLE:      secret=false
URL:       secret=true
```

Its default prefix remains `pg16_`, URL scheme `pgsql://`, port `5432`, grant
support is true, and Backup is supported only through
`postgres-custom-v1`. This ADR does not change its helper, SQL, artifact, or
restore semantics.

### 9. Select current exactly once and pin every operation

`POST /backing-services` accepts a closed adapter union. A managed branch
authors only `x-gp-adapter.key` (`postgres:16` or `valkey:9`) and the accepted
optional facts-prefix decision. Adapter revision, repository, tag, digest,
platform, release values, helper, command, and procedure are forbidden authored
managed inputs. A manual branch authors exactly `x-gp-adapter.key: manual`,
forbids a facts-prefix override, and permits the authoritative operator-owned
complete Service runtime input under the existing ordinary Blueprint and ADR
0037 bounds.

For a managed branch, the Controller calls `Current(key)` exactly once while
atomically claiming the protected idempotent create preparation. It stores the
complete immutable snapshot. Replay of the same preparation uses the stored
revision even after a Controller update; it never reselects current. A manual
branch stores the typed manual binding and performs no registry lookup.

Managed Blueprint input forbids authored `image`, `build`, `platform`,
`pull_policy`, `entrypoint`, command/Cmd override, reserved adapter environment,
release-owned healthcheck, adapter port override, replica override, and a
strategy other than `recreate`. Operator-owned resources, logging,
non-reserved environment, logical Volumes, and non-reserved mounts remain
available within ADR 0037 bounds.

Manual Blueprint input retains operator-authored image, command, entrypoint,
platform, environment, healthcheck, mounts, resources, logging, and release
policy wherever the existing Blueprint permits them. A manual Backing Service
still uses the facade's explicit Network and lifecycle, but a manual Attach is
network membership only: no procedure, role, password, fact, grant, or Backup.

Generated managed Compose contains the exact immutable
`repository@sha256:index-digest` reference and release labels. Mutable tags
never enter managed desired state, plans, observations, retry, Backup, or
deletion authority. Manual uses the exact operator-authored image reference
per the existing Blueprint policy, including a tag when that policy permits
one; Groundplane does not convert it into managed release authority.

There is no MVP in-place adapter-upgrade operation, endpoint, CLI command, or
Console action. A new Controller release may change `current` only for future
creates. Existing Start, Stop, Destroy, Attach, Detach, Backup, Retry, and
Delete remain pinned. Migration is create-new, migrate data explicitly, then
delete-old.

### 10. Replace Agent authentication inside schema 1

The Agent wire remains schema 1. [Backup execution contract](../features/backups/agent-protocol.md)'s dedicated
`managed_postgres16_release_sha256` authentication field is removed. It is not
retained alongside a generic field, and no whole-ledger equality field is
added.

Authenticate tag 5 is clean-reused with the same length-delimited wire class;
there is no reserved tag or old decoder:

```proto
message ManagedAdapterCapabilityV1 {
  string adapter_key = 1;
  bytes adapter_revision = 2;
}

message Authenticate {
  string agent_id = 1;
  bytes token = 2;
  uint32 execution_plan_schema = 3;
  bytes process_generation = 4;
  repeated ManagedAdapterCapabilityV1 managed_adapter_capabilities = 5;
}
```

An Agent advertises exactly every executable current and historical entry
compiled into that release. It recognizes but does not advertise revoked
entries. Capabilities sort by adapter-key UTF-8 bytes then raw revision bytes
and are unique. There are at most 16 executable entries per managed key and 32
total. Release activation fails above either bound until a fixed-revision
pre-activation audit proves an unreferenced historical descriptor may be
omitted. Wrong-length revisions, unknown keys, duplicates, unsorted entries,
or omission of the Agent's compiled executable entry fail authentication.
A Controller ignores a well-formed extra revision it does not compile and
assigns only an exact pair in the intersection.

Authenticate size arithmetic replaces [Backup execution contract](../features/backups/agent-protocol.md) exactly. Fields 1 through 4
cost 86 bytes. A `postgres:16` capability is 47 inner bytes and 49 with its
tag-5 wrapper; a `valkey:9` capability is 44 inner and 46 wrapped. At 16 of
each:

```text
Authenticate inner = 86 + 16*49 + 16*46 = 1,606 bytes
AgentMessage outer = 1 + 2 + 1,606       = 1,609 bytes
```

The schema-1 Authenticate inner ceiling is 1,606 and its complete AgentMessage
variant ceiling is 1,609, replacing 256. The global AgentMessage ceiling is
unchanged. The 32-record/262,144-byte connection-control queue remains exact:
32 maximum Authenticate envelopes cost 51,488 bytes.

ExecutionPlan gains the next collision-free field:

```proto
message ManagedServiceAuthorityV1 {
  string service_id = 1;       // exact canonical 30-byte Service id
  string adapter_key = 2;
  bytes adapter_revision = 3;  // exactly 32 bytes
}

message ExecutionPlan {
  // existing tags 1..8 unchanged
  repeated ManagedServiceAuthorityV1 managed_service_authorities = 9;
}
```

There are at most 256 authorities, sorted unique by Service-id bytes, adapter
key bytes, then raw revision. There is exactly one for every managed Service
any artifact, step, or Backup source can touch, and none for a manual Service.
A maximum entry is 79 inner bytes and 81 with its tag-9 wrapper; 256 cost
20,736 bytes. The existing 4,194,304-byte deterministic ExecutionPlan ceiling
includes that reserve. The prior plan payload budget shrinks by up to 20,736;
the plan ceiling, 5,242,880 assignment ceiling, and exact 4,782,788 maximum
ControllerMessage assignment proof remain unchanged.

AdapterProcedure is clean-replaced inside schema 1:

```proto
message AdapterProcedure {
  string adapter_key = 1;
  AdapterProcedurePhase phase = 2;
  string attach_id = 3;
  string backing_service_id = 4;
  string role = 5;
  reserved 6;
  reserved "password";
  string database = 7;
  string grant_on = 8;
  bytes adapter_revision = 9;          // exactly 32 bytes
  bytes execution_nonce = 10;          // exactly 32 random bytes
  int64 hard_deadline_unix_nano = 11;  // positive, <=MaxInt64
}
```

The nonce becomes exactly 64 lowercase hexadecimal helper-argv bytes. Deadline
becomes canonical unsigned decimal helper argv. Each procedure tuple equals
its matching plan authority. There are at most 256 AdapterProcedure steps per
plan. Password tag 6 is not decoded, translated, or retained; secret bytes do
not enter deterministic plan bytes.

ControllerMessage tag 7 and its nested length-delimited tags are clean-reused
to generalize the transient slot, with no Backup compatibility decoder:

```proto
message TaskSecretSlotTransfer {
  string task_id = 1;
  string assignment_id = 2;
  string step_id = 3;
  TaskSecretSlotPurpose purpose = 4;
  oneof record {
    TaskSecretSlotHeader header = 5;
    TaskSecretSlotChunk chunk = 6;
    TaskSecretSlotEnd end = 7;
  }
}

enum TaskSecretSlotPurpose {
  TASK_SECRET_SLOT_PURPOSE_UNSPECIFIED = 0;
  TASK_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY = 1;
  TASK_SECRET_SLOT_PURPOSE_S3_SECRET_KEY = 2;
  TASK_SECRET_SLOT_PURPOSE_CURRENT_AGE_IDENTITY = 3;
  TASK_SECRET_SLOT_PURPOSE_OPERATOR_OLD_AGE_IDENTITY = 4;
  TASK_SECRET_SLOT_PURPOSE_MANAGED_ADAPTER_ADMIN_PASSWORD = 5;
  TASK_SECRET_SLOT_PURPOSE_MANAGED_ADAPTER_ATTACH_PASSWORD = 6;
}
```

`TaskSecretSlotHeader`, `Chunk`, and `End` retain the prior field tags. Every
managed-adapter password is exactly one 43-byte chunk. A slot is fenced by
task, assignment, step, purpose, nonce, plan hash, and AdapterRevision. Ready
and detach require the admin slot; provision requires admin then Attach. The
Agent assembles the helper frame in kind order and never persists or logs slot
bytes. It clears every complete or incomplete slot buffer and every assembled
frame on consume, success, failure, abort, hard-deadline expiry, timeout,
cancellation, assignment replacement, assignment retirement, Task
terminalization, stream disconnect, stream reconnect, and Agent process
shutdown, including every path before ExecCreate. Redelivery always starts
from an empty slot and is accepted only under the same complete fence;
byte-identical records may then redeliver. A mismatched,
duplicate-nonidentical, reordered, or surplus record fails the Task.

For one 43-byte adapter slot, Header is 4 bytes, Chunk 47, and End 2. Including
transfer and Controller tag-7 wrappers, the three complete outer records are
109, 153, and 107 bytes. One executing procedure has at most two slots and six
active records, so the existing per-assignment 8-record/262,144-byte inline
queue remains unchanged. Across at most 256 procedures, the exact cumulative
adapter-secret limits are 512 slots, 1,536 records, 22,016 content bytes, and
188,928 serialized bytes. A plan operation cannot combine Backup source steps
with AdapterProcedure steps, so Backup and adapter cumulative slot budgets do
not add. Existing S3 and age slot sizes remain unchanged.

Before the first host effect and before every adapter exec, the Agent requires
equality across the plan service authority, procedure, compiled entry,
generated Compose image and labels, and observed Moby image/child/runtime
facts. It never substitutes current, another historical entry, a tag-compatible
image, or a same-major image.

For PostgreSQL Backup, the generic AdapterRevision resolves a release record
whose nested `managed_image_release_sha256` must equal the [Backup artifact contract](../features/backups/artifacts.md)
`managed_release_sha256`. The Agent then applies all [Backup artifact contract](../features/backups/artifacts.md)/0048 image,
helper, execution, and runtime checks. Generic adapter identity does not weaken
PostgreSQL's nested managed-image authority.

### 11. Expose the closed current managed/manual catalog union

`GET /backing-service-adapters` returns exactly three entries in adapter-key
byte order: `manual`, `postgres:16`, and `valkey:9`. Its item is a closed
discriminated union.

A managed item is exactly the matching current executable `CatalogContract`
projection plus these release fields:

```text
ManagedBackingServiceAdapterCatalogItem = {
  kind: "managed",
  key: "postgres:16" | "valkey:9",
  display_name: string,
  adapter_revision: OCI_SHA256,
  image_ref: managed_repository + "@" + managed_index_digest,
  runtime_major: 16 | 9,
  default_facts_prefix: string,
  url_scheme: string,
  port: uint16,
  fact_schema: [{key: string, secret: boolean}],
  supports_grants: boolean,
  backup_supported: boolean,
  strategy: "recreate",
  replicas: 1,
  provision_summary: [string],
  detach_summary: [string]
}
```

`image_ref` uses the managed index digest, never the platform child digest.
Summary members are the safe literal non-secret arrays sealed by the exact
catalog object. Fact schemas use fact-key byte order.

The manual item is exactly:

```text
ManualBackingServiceAdapterCatalogItem = {
  kind: "manual",
  key: "manual",
  display_name: "Manual",
  runtime_input: "operator_owned",
  fact_schema: [],
  supports_grants: false,
  backup_supported: false,
  provision_summary: [],
  detach_summary: []
}
```

It has no AdapterRevision, release/image/runtime-major authority, generated
fact, managed procedure, grant, Backup, strategy, or replica member. The
response exposes no SQL, RESP, executable name, argv, helper path, procedure
template, secret, mutable tag, child/config digest, ACL rule, release history,
or revocation metadata.

Backing Service list/detail is also a closed binding projection. Managed rows
project the pinned release as:

```text
ManagedBackingServiceAdapterProjection = {
  kind: "managed",
  key: "postgres:16" | "valkey:9",
  adapter_revision: OCI_SHA256,
  display_name: string,
  image_ref: managed_repository + "@" + managed_index_digest,
  runtime_major: 16 | 9,
  availability: "current" | "historical" | "revoked" | "missing"
}
```

Manual rows project exactly
`{kind:"manual",key:"manual",display_name:"Manual"}`. Managed safe members
come from the immutable durable snapshot and only availability is joined from
the compiled registry. Read projection does not fail only because a managed
revision is revoked or missing. A malformed durable snapshot is an internal
integrity failure and is never repaired from `Current`.

The exact discovery parity is one Console adapter-catalog load/action,
`groundplane backing-service adapter list`, and
`GET /backing-service-adapters`. All three consume the same three-item union;
no fixture catalog or frontend adapter switch survives. Revocation is compiled
managed release metadata, not an operator capability, so it adds no mutation
surface.

### 12. Fail closed without losing recovery state

The exact public and Task error mapping is:

| Condition | Result |
| --- | --- |
| authored managed field, unknown/non-public key, or attempted revision selection | HTTP 422 `validation.failed` before mutation |
| no executable current revision | HTTP 409 `state.conflict`; no Task |
| pinned revision missing or revoked | HTTP/Task `state.conflict` |
| Agent lacks exact pair | HTTP/Task `state.conflict` |
| Controller/Agent/plan/procedure/Compose/Moby pair mismatch | HTTP/Task `state.conflict` |
| malformed durable release snapshot or reference invariant | `internal` integrity failure |
| Valkey chosen as a Backup source | `strategy.not_implemented` before Task publication |

When a synchronous mutation detects a conflict, it returns 409 and publishes
no Task. When detection occurs after Task publication but before a host effect,
the Task terminalizes failed with `state.conflict`. A successor Retry preserves
the same pair.

Revocation is compiled release metadata. A newly revoked revision cannot be
current, so a release carrying revocation must also carry another current entry
for that public key. Activation assigns no new work for the revoked pair.
Active protected work prevents successful finalization; it is not silently
accepted or redirected.

Revocation disables create selection, assignment, readiness, Attach, Backup,
lifecycle, and cleanup for that revision. It preserves list/show diagnostics,
the descriptor, durable snapshot, reverse references, tombstones, and retry
ownership. Delete encountering missing or revoked authority retains all of
those plus durable data. It does not skip adapter cleanup or select current.

### 13. Valkey Backup is explicitly absent

`postgres:16` has `backup_supported=true` only when
`backup_contract=postgres-custom-v1` and the nested [Backup artifact contract](../features/backups/artifacts.md) release authority
matches exactly. Backup and Restore plans carry both adapter key and revision.
A missing or revoked PostgreSQL revision returns `state.conflict`; no current
or same-major fallback exists.

`valkey:9` has `backup_supported=false` and `backup_contract=none` for the MVP.
Backup policy/source construction rejects Valkey with
`strategy.not_implemented` before Task publication. The current advisory
`valkey-cli --rdb` / `valkey-cli --pipe` strings are removed and have no
product authority. A future Valkey Backup requires a separate accepted exact
capture, artifact, consistency, and restore-publication contract.

## Scoped adoption and supersession

This ADR uses scoped replacement rather than a second parallel contract:

| Prior decision | Retained authority | Clean replacement by ADR 0054 |
| --- | --- | --- |
| ADR 0037 | Backing Service facade, public network-only `manual` adapter, explicit Network choice, complete operator-owned manual Service decisions, lifecycle, readiness requirement, 1:1 surface | Managed Services no longer author image/build/platform/pull policy/entrypoint/command/user; the catalog is the exact three-item managed/manual union above; release history is compiled and never runtime-collected in MVP; Valkey health is release-owned and not a catalog-edit default |
| [Backup artifact contract](../features/backups/artifacts.md) | PostgreSQL 16 upstream/derived image, helper, artifact, release preimages, Backup/Restore mechanics | Its managed release becomes the nested `postgres:16` image release; the generic AdapterRevision and registry own selection/history/revocation without redefining PostgreSQL mechanics |
| [Backup execution contract](../features/backups/agent-protocol.md) | Sole schema-1 channel, task/receipt/checkpoint semantics, PostgreSQL helper execution, single-start hijacked Moby Exec mechanics | Authenticate tag 5 is clean-reused for sorted adapter capabilities; ExecutionPlan tag 9 and AdapterProcedure tags 9-11 carry revision/nonce/deadline; tag 7 becomes the generic Task secret slot; its prior secret-bearing Exec restriction is replaced by `ManagedAdapterExecV1`; all exact caps and queue arithmetic above replace the narrower values; PostgreSQL still verifies [Backup artifact contract](../features/backups/artifacts.md) through the resolved generic revision |
| ADR 0032 | Typed AdapterProcedure lifecycle phases and non-secret deterministic plan authority | AdapterProcedure password tag 6 is reserved and never decoded; all managed adapter password delivery moves to fenced transient Task secret slots |
| [Backup contract](../features/backups.md) | PostgreSQL Backup and explicit deferral of Valkey Backup | `backup_contract=none` and pre-publication `strategy.not_implemented` make the deferral machine-readable |

The earlier proposal to send a whole-ledger digest while retaining the
dedicated PostgreSQL digest is rejected. It would create dual authority and
would reject a newer Agent solely for carrying an additional harmless compiled
history entry. Exact tuple intersection is the narrow executable agreement.

## Required synchronized clean replacements

Acceptance and implementation require one synchronized replacement across:

- `docs/mvp.md`: replace the statement that every adapter dumps/restores with
  an explicit Backup capability; define the three-way `manual|postgres:16|valkey:9`
  create/catalog union and typed binding; preserve manual's operator-owned
  runtime and pre-Task Backup rejection; remove mutable managed-image/default
  wording; and require Valkey DB 0 plus `KEY_PREFIX` and `CHANNEL_PREFIX` with
  the documented command, flush/enumeration, scripting, and PSUBSCRIBE limits;
- `docs/api-cli.md`: add the exact three-item adapter catalog parity, managed
  and manual create arms, pinned managed/manual projections, literal summaries,
  both Valkey prefix facts/consumer warning, and manual/Valkey Backup rejection;
- `docs/blueprint.md`: define the managed/manual adapter union; forbid managed
  release-owned fields in authored backing Services while retaining them in
  generated projections; preserve the authoritative full operator-owned manual
  runtime input; and make the Valkey DB0/prefix constraints non-authored derived
  authority;
- `docs/architecture.md`: replace key-only adapter lookup and mutable image
  defaults with the two-key compiled registry and reverse-reference audit;
- `docs/capabilities.md`: do not claim C10 or Valkey Backup implemented until
  their production verticals and evidence exist;
- ADR 0037: replace its contradictory authored-image and broad catalog shapes;
- [Backup execution contract](../features/backups/agent-protocol.md) and `proto/agent.proto`: clean-remove the dedicated PostgreSQL auth
  digest and AdapterProcedure password; clean-reuse Authenticate tag 5 and
  ControllerMessage tag 7; add the exact capability, plan-authority,
  revision/nonce/deadline, and Task-secret messages/tags/caps/arithmetic above;
  generalize secret-bearing Moby Exec; then regenerate protobuf rather than
  editing generated Go;
- `internal/adapters/registry.go`: replace global key-only `Register/Get/All`,
  `DefaultImage`, advisory `Step`, and string `BackupStrategy` with the
  immutable two-key registry, closed procedures, release binding, and closed
  Backup capability;
- `internal/adapters/postgres16`: remove `postgres:16-alpine` and key-only
  mutable-image authority while preserving [Backup artifact contract](../features/backups/artifacts.md)/0048 semantics;
- `internal/adapters/valkey9`: remove mutable image, unauthenticated CLI steps,
  no-op grant/revoke success, and false RDB restore authority;
- Controller persistence: add the immutable Service snapshot, exact reverse
  rows, typed managed/manual binding, startup audit, preparation pin, and
  revision-preserving retry/delete;
- renderer and observer: inject and attest the exact index-digest image,
  image/release labels, complete mount multiset, protected roots, and inherited
  Config projection; reject mutable, child-digest, or same-major substitutes;
- OpenAPI and generated clients: replace the catalog and Backing Service
  adapter projection cleanly;
- Console and CLI: remove fixture switches/default images, consume the live
  three-item catalog union, render literal provision/detach summaries, and
  require both Valkey prefix facts; and
- Agent runtime: add the direct static-helper `ManagedAdapterExecV1`, exact
  environment erasure, transient slot/frame assembly, descriptor-safe ACL seed
  materializer, and normalized ACL readback/behavioral proof;
- release automation: build, read back, verify, and compile both managed image
  releases without invented repository or post-push digests.

No alias, deprecated field, tag fallback, dual registry, dual Agent auth field,
or schema translation remains after replacement.

## Consequences

- A stable adapter key is human product identity; a revision is exact
  executable authority.
- New creates see only current releases. Existing work remains reproducibly
  pinned to compiled history.
- Revocation fails closed without destroying the evidence and state needed to
  diagnose or recover.
- Controller and Agent may have different harmless history while agreeing on
  the exact executable tuple assigned to a Task.
- The Valkey server retains its official runtime shape while Groundplane adds
  only one static helper, one fixed config, one normalized layer, and two
  release labels.
- Valkey credentials persist through ACL state in the owned data Volume, and
  readiness/provision/detach are authenticated and verified.
- Each Valkey Attach is confined to DB 0, its own key prefix, its own channel
  prefix, and a fixed application command set; consumers must configure both
  prefix facts because the URL cannot convey them.
- The public `manual` adapter remains a first-class network-only escape hatch
  for operator-owned runtime input without being assigned a false managed
  revision.
- Valkey Backup remains visibly and mechanically absent instead of pretending
  that an incompatible CLI pipe is restore authority.
- The cost is a release-time build/readback pipeline, compiled history,
  durable reverse references, and revision propagation through every managed
  runtime plan.

## Rejected alternatives

### Key-only adapter lookup

Rejected because it cannot reproduce retry/delete behavior after the current
release changes and cannot represent revocation.

### Mutable image tags or operator-selected managed images

Rejected because tag movement, arbitrary config, or same-major compatibility
cannot prove helper, procedure, Backup, or cleanup identity.

### A whole-ledger Agent equality digest

Rejected because Controller and Agent need exact executable intersection, not
identical harmless history. A whole-ledger digest would turn safe binary version
skew into unnecessary channel failure.

### Keep the dedicated PostgreSQL auth digest as well

Rejected because one operation would have two release authorities. PostgreSQL
release equality is derived and verified through its generic revision.

### Runtime registry mutation or garbage collection

Rejected for the MVP because it adds a second distributed control plane and
can remove recovery authority while durable references remain.

### Recreate the Valkey ACL file on every Start

Rejected because it deletes persisted Attach users and converts a detected
state conflict into silent data-plane credential loss.

### Grant `allcommands`, `allkeys`, `allchannels`, or all databases

Rejected because multiple Attach credentials share one Valkey instance. Any of
those grants permits cross-Attach observation or mutation. DB 0 plus exact key
and channel prefixes and the fixed command manifest is the MVP isolation
boundary; it deliberately excludes global keyspace/server administration.

### Put `manual` into the managed release ledger

Rejected because manual is intentionally operator-owned network attachment. A
fake AdapterRevision would assert image/procedure/fact authority Groundplane
does not own and would make the existing public manual behavior unreachable.

### Use `valkey-cli`, shell command strings, or environment secrets

Rejected because they expose secret-bearing argv/environment/parser surfaces,
provide weak result typing, and do not bind the exact helper release.

### Claim Valkey RDB Backup from `--rdb` and `--pipe`

Rejected because `--pipe` imports RESP commands; it does not publish an RDB
image as restored server state.

## Acceptance evidence

This ADR may move from Proposed only when independent contract, transaction,
release, wire, and security reviews agree that no competing authority remains.
Implementation completion additionally requires:

- digest golden tests for every domain, JCS preimage, malformed number,
  ordering, duplicate, and nested-hash mismatch;
- PostgreSQL lifecycle-manifest golden tests proving the exact 86 records,
  13,427-byte domain digest, identifier/literal/secret renderers, direct psql
  argv/environment/bounds, mutation/probe/proof output rules, create/adopt and
  exact marker/least-privilege collision rejection, prepared-before-Exec role
  creation, COMMIT-before-checkpoint total role/database readback,
  both-namespaces-absent-only CREATE ROLE retry,
  new-role-only compensation,
  current/future table-and-sequence DML grant/revoke symmetry, fenced
  connection termination, database/role drop branches, unknown-result
  convergence, and all five lifecycle actions;
- compiled-registry tests for exactly two managed current entries, historical resolve,
  revocation, missing current, wrong major, immutable construction, and newer
  Agent extra-capability intersection;
- startup-audit tests proving both reference directions at one fixed revision
  and degraded-read/mutation-block behavior for a missing descriptor;
- create/replay tests proving current is selected once and a Controller update
  cannot repin preparation;
- lifecycle, Attach, Retry, Backup, and Delete tests proving they never call
  `Current` and never substitute another revision;
- wire tests for schema 1, deliberate tag-5/tag-7 reuse, tuple
  ordering/uniqueness, 16-per-key/32-total capability caps, exact intersection,
  the 1,606/1,609 Authenticate bounds, 51,488-byte queue proof, tag-9 plan
  authorities, AdapterProcedure tags 9-11, reserved password tag 6, the
  20,736-byte plan reserve, exact Task-secret records, active/cumulative slot
  bounds, revision in procedure/plan hashes, and removal of both dedicated
  fields;
- authored Blueprint tests rejecting every release-owned field through root,
  include, extends, interpolation, and `env_file` paths;
- Valkey helper parser tests for every malformed argv/frame/secret condition,
  exact 55-byte ready/detach and 101-byte provision frames, golden rejection at
  provision lengths 100 and 102 and acceptance at 101,
  all 12 normal exits and precedence, exact 17/21/18-byte success records,
  21-byte stdout and zero-byte stderr caps, 65,536-byte reply and 262,144-byte
  aggregate boundaries, line/depth/value limits, absolute deadline, nonce,
  RESP error, reconnect, retry classes, the exact 223-command/1,918-byte
  manifest and digest, seven-field normalized ACL readback, positive/negative
  DRYRUN matrix, nested Lua isolation, literal PSUBSCRIBE boundary, ACL-file
  post-SAVE metadata, and credential redaction;
- Moby Exec contract tests against 29.1.3/API 1.52 proving the direct attested
  helper path, exact env-name erasure and four-entry environment, complete
  protected-mount/mount-multiset equality, one attached start, concurrent exact
  stdin write/CloseWrite/demux, result caps, inspect equality, unknown-result
  recovery, no overlap, and convergent byte-identical retry;
- ACL seed materializer fault-injection tests at every write, `fdatasync`,
  post-metadata temp-file `fsync`, rename, reopen, published-file `fsync`, and
  directory-sync boundary proving absent rerun, exact
  initial-state adoption, safe owned-temp reconciliation, and mismatch conflict;
- Valkey release tests proving exact upstream index/child/config, the closed
  inherited Config projection, rootfs prefix, the exact four-entry uncompressed
  USTAR layer and headers/order/DiffID, file metadata/digests, ARM64 variant
  absence, exact version probes, authentication, provision/restart, and
  detach/restart;
- API/CLI/Console contract tests proving exactly the same three-item
  managed/manual union, key order, fact order, literal provision/detach
  summaries, Valkey prefix instructions, managed pinned availability, and
  manual projection/create behavior;
- Backup tests proving Valkey returns `strategy.not_implemented` before Task
  publication and PostgreSQL resolves the nested [Backup artifact contract](../features/backups/artifacts.md) release; and
- minimal Ubuntu 24.04 ARM64 acceptance that creates, stops, starts, attaches,
  detaches, destroys, reconstructs, and permanently deletes both managed
  adapters using registry-readback digest images without handwritten runtime
  scripts.

## Remaining release inputs

No product or helper-protocol decision remains. The following are deliberately
release-time inputs or outputs and must not be guessed in source documentation:

- the first-party Valkey managed OCI repository;
- the measured Valkey helper size and SHA-256;
- the Valkey level-one `contract_sha256` derived after helper measurement;
- the managed index and ARM64 child digests read back after push;
- the resulting Valkey `managed_release_sha256` and AdapterRevision; and
- equivalent first-party PostgreSQL repository/readback outputs already left
  open by [Backup artifact contract](../features/backups/artifacts.md).

Until those values are produced, verified, and compiled, the affected entry
cannot be marked current and C10 cannot be declared create-ready.
