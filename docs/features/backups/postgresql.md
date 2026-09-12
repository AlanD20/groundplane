# Managed PostgreSQL Backup execution

Read this document for the PostgreSQL 16 workload image, release authority,
root helper and private client gate, process supervision, Docker Exec contract,
and restore-apply recovery boundary. Artifact and operator behavior live in
[artifact formats](artifacts.md#postgresql-format-and-restore-semantics) and the
[feature entrypoint](../backups.md). The [Agent protocol](agent-protocol.md)
owns assignment/checkpoint messages and common recovery.

## Supported release

The MVP supports adapter contract version 1 and PostgreSQL 16 through one
first-party derived OCI index containing exactly one native runnable child for
each:

```text
linux/amd64
linux/arm64
```

Emulation is outside the MVP. Each child derives from the platform child of the
authenticated PostgreSQL `16.15-alpine3.24` upstream index:

```text
repository:       docker.io/library/postgres
index digest:     sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685
source repository:https://github.com/docker-library/postgres.git
source commit:    9d15534160ade17f2b6c455a39ee967c49b1937d
source directory: 16/alpine3.24
PostgreSQL:       16.15
source SHA-256:   c1575341fa7bd40f5274ea465b34390f4dc64cdd0770af327005caaeb9f6b7ed
```

The authenticated upstream ARM64 descriptor decides whether its variant is
`v8` or null. Evidence for one platform is never borrowed from the other.

The derived image adds only:

```text
/usr/local/libexec                         root:root 0755
/usr/local/libexec/groundplane-postgres16-helper
                                             root:root 0555, static native ELF64
/usr/local/libexec/groundplane-postgres16-client-gate
                                             root:root 0500, static native ELF64
/run/groundplane-postgres16                 root:root 0700
```

Helper state files are root-owned regular single-link mode `0600`. The database
uid 70 cannot traverse the state directory or read, write, link, unlink,
rename, replace, chmod, or chown its files. The helper/gate/state are image
contents in the existing managed database container. They are not in the Agent,
copied or mounted at runtime, or supplied through another image/container,
volume, bind, tmpfs, Docker socket, or TCP path.

The derived Dockerfile does not set or replace Entrypoint, Cmd, User, WorkingDir,
Env, StopSignal, exposed ports, or volumes. Release verification requires:

```text
Entrypoint: ["docker-entrypoint.sh"]
Cmd:        ["postgres"]
User:       unset
WorkingDir: /
StopSignal: SIGINT
PG_MAJOR:   16
PG_VERSION: 16.15
PGDATA:     /var/lib/postgresql/data
```

Each platform retains its complete authenticated upstream Config.Env
byte-for-byte and in order. Normal startup begins uid 0 and the inherited
entrypoint performs its ordinary `gosu postgres` drop. This differs from helper
Docker Exec, which is exact numeric `0:0`; the private gate launches only the
fixed database client as exact numeric `70:70`.

No Project, Environment, Blueprint, operator setting, Controller configuration,
or Agent configuration can select or override repository, tag, digest,
platform, helper, client gate, or contract version. The former mutable
`postgres:16-alpine` identity has no alias, fallback, dual acceptance, or
migration reader. A pre-contract backing container is recreated through the
canonical Service lifecycle only after old helper state is proved empty.

## Two-level release authority

The release is cycle-free. For both levels:

```text
D(domain, bytes) = SHA-256(ASCII(domain) || 0x00 || bytes)
JCS(x)            = RFC 8785 canonical JSON
```

### Level one: build contract

The binaries are built and measured first. The closed
`ManagedPostgres16BuildContract` includes schema/adapter identity; the exact
upstream source and index; ordered amd64 then arm64 platform evidence; helper
and gate paths, ownership, mode, native ELF identity, lengths and digests;
runtime fields and full per-platform upstream Config.Env; the exact container
security projection; helper/client identities and environment; state metadata;
client/gate/fd protocol versions; per-platform seccomp digests; and probe
requirements. Missing, duplicate, reordered, extra, guessed, or cross-platform
evidence is invalid.

The common security fields are exactly:

```text
Privileged:       false
CapDrop:          [ALL]
CapAdd:           [CHOWN,DAC_OVERRIDE,FOWNER,KILL,SETGID,SETPCAP,SETUID,SYS_PTRACE]
SecurityOpt:      [no-new-privileges:true]
postmaster uid/gid/fsuid/fsgid and saved ids: 70
postmaster groups: []
postmaster inheritable/permitted/effective/ambient capabilities: 0
postmaster bounding capabilities: exactly the eight CapAdd names
postmaster NoNewPrivs: true
```

The client security profile is exact `70:70` across real/effective/saved/
filesystem ids, no groups, all five capability sets zero, `NoNewPrivs=true`,
seccomp mode 2, and nofile soft/hard limit 64. The four-entry replacement
environment is:

```text
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HOME=/nonexistent
LC_ALL=C
TZ=UTC
```

Fixed client paths are `/usr/local/bin/pg_dump`, `pg_restore`, and `psql`.
The amd64 helper/gate machine is `EM_X86_64`; arm64 is `EM_AARCH64`. Both are
ELF64 little-endian, static, native, and behaviorally identical.

Each platform seccomp digest is raw SHA-256 over its exact classic-BPF
`sock_filter` array serialized as little-endian `u16 code`, `u8 jt`, `u8 jf`,
and little-endian `u32 k`, without header/padding. Release proof reconstructs
the array, verifies the digest and semantics, proves libc and raw
fork/vfork/clone/clone3 fail `EPERM`, and proves the selected client works.

The launch-profile digest is computed before the build-contract digest:

```text
launch_profile_sha256 =
  D("groundplane.postgres16.client-launch-profile.v1",
    JCS(build_contract.process))

contract_sha256 =
  D("groundplane.postgres16.managed-image-build-contract.v1",
    JCS(build_contract))
```

The process object includes both platform seccomp digests but no binary or
managed-image digest. The build-contract preimage includes no self digest,
managed repository, managed index, or managed child digest. Helper and gate
embed the launch-profile digest. The derived image adds exactly:

```text
com.groundplane.postgres16.adapter-contract-version=1
com.groundplane.postgres16.contract-sha256=<contract_sha256>
```

### Level two: post-push release record

After push, release automation supplies the canonical lowercase repository and
reads the registry-reported managed index and exact amd64/arm64 child, config,
and added-layer digests. Repository has no tag, digest, scheme, query, fragment,
or whitespace. The release record contains schema 1, `contract_sha256`,
repository, index digest, and exactly those two ordered platform records. Its
digest is:

```text
managed_release_sha256 =
  D("groundplane.postgres16.managed-image-release-record.v1",
    JCS(release_record))
```

The digest is external to the record. Controller and Agent artifacts embed the
same level-one object/digest and level-two object/digest. Agent Authenticate
carries the decoded 32-byte managed release digest. The Controller assigns
PostgreSQL work only on exact equality.

Release verification fetches the managed index and proves exactly one native
child per supported platform; every upstream and derived descriptor/layer/
Config.Env relationship; retained inherited runtime fields; exact two labels;
helper/gate/state metadata; no volume/mount for state; exact HostConfig security;
root gate READY identity/fds/capabilities; uid-70 child identity/security; exact
client paths/bytes; PostgreSQL client/server major 16; and a temporary initialized
server. Each derived child appends only its platform's recorded added layers to
its matching authenticated upstream layers.

Canonical Compose and durable Service desired state use only:

```text
managed_repository + "@" + managed_index_digest
```

The adapter exposes no image/default/repository/digest/platform/helper option.
Service, Compose artifact, required-label digest, and Backup authority seal the
same identity. Before every helper exec, Moby revalidates the exact Service and
container, native release child, release labels, adapter version, label digest,
Service/Compose revisions, and immutable container id.

## Container and mount attestation

Compile/authority validation and runtime ContainerInspect canonicalize mounts
and require exact equality with sealed Compose. A mount destination may not be
equal to, inside, or a path-component ancestor of:

```text
/bin /sbin /lib /lib64 /usr /etc /run /var/run
/usr/local/libexec/groundplane-postgres16-helper
/usr/local/libexec/groundplane-postgres16-client-gate
/run/groundplane-postgres16
```

The managed data mount at `/var/lib/postgresql/data` is the sole exception and
is accepted only when its source, destination, type/mode, propagation, and
read/write facts match sealed authority exactly. String prefix comparison is
insufficient. Extra targets cannot shadow helper, `env`, clients, runtime,
identities, or database socket.

ContainerInspect must equal the HostConfig projection above. After startup the
Agent authenticates the init/postmaster using container pid plus host `/proc`
start ticks and boot id. All four uid/gid values are 70, groups are empty,
inheritable/permitted/effective/ambient capabilities zero, bounding set exactly
the eight permitted startup capabilities, and `NoNewPrivs:1`; executable and
closed server argv match release. Release rootfs proof ensures no uid-70-
reachable setuid/setgid/capability executable or writable executable path.
Failure blocks Ready and all ordinary PostgreSQL Backup work.

Container root, Docker daemon/host root, and Agent Docker authority are trusted
boundary actors. PostgreSQL uid 70 is not trusted with supervisor state. The
contract does not claim protection against host root or container root.

## Shared helper vocabulary

`internal/common/postgres16protocol` is the sole side-effect-free owner of
protocol version, paths, operations, command construction, uid/gid/security,
launch/gate/fd/state vocabulary, nonce/deadline encoding, argument encodings,
stream rules, and numeric exit meanings. It owns no Docker client, process, or
filesystem state. The Moby caller consumes this leaf; no caller imports the
helper implementation or copies a path, token, argument order, or exit table.

`cmd/postgres16-helper` is the only public Docker Exec helper.
`cmd/postgres16-client-gate` is private and callable only by the helper's closed
raw-process boundary. `internal/postgres16helper` is the root supervisor and
the sole narrow exception allowed to use raw process/Linux control for this
gate. It maps a validated operation to one fixed client, argv, replacement
environment, fd profile, and security profile. The generic subprocess Runner is
not broadened.

The helper requires all real/effective/saved/filesystem uid/gid values zero and
no supplementary groups before it parses an operation or touches state. A uid
70 direct invocation fails caller authentication and cannot invoke the gate.

The normal helper exit table is exact:

| Exit | Meaning |
| ---: | --- |
| 0 | operation semantic/stream proof complete, fixed child reaped once by its original parent, state durably removed; for stop, exact normal or non-parent retirement is complete without claiming child success |
| 1 | malformed, noncanonical, wrong-version, or invalid request |
| 2 | caller passed but helper executable/environment/working directory/fd envelope invalid before ambiguous release |
| 3 | recovery required because identity, release, execution, I/O, terminal/reap/retirement, or spent restore mutation is absent or ambiguous |
| 4 | classified launch/admission failure with complete retirement and unspent retry authority |
| 5 | deadline exceeded after complete TERM/KILL/reap/retirement and no stronger ambiguity |
| 6 | admitted child normal nonzero exit and complete retirement; spent restore apply maps to recovery required |
| 7 | root caller identity invalid |
| 8 | helper-owned stream/pipe I/O failed with complete retirement and no more specific class |
| 9 | restore input size/hash/EOF integrity failed; spent apply maps to recovery required |
| 10 | stdout/stderr or checked count limit exceeded; spent apply maps to recovery required |
| 11 | complete bounded zero-exit result failed exact probe/semantic proof |
| 12 | pre-ambiguity helper invariant/resource failure with complete retirement |

No other normal exit is valid. `126`, `127`, and signal-style `128..255` are
never helper meanings; signal death or missing proof is not remapped by
subtracting 128. Classification order is caller, environment, request. After a
state/child may exist, recovery-required overrides deadline or stage errors when
identity, release, terminal, reap, or mutation authority is uncertain. A
complete gate FATAL is launch failure only with consistent status, complete
retirement, and unspent authority.

## Docker Exec and client operations

Every run command has this exact prefix:

```text
/usr/bin/env
-i
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HOME=/nonexistent
LC_ALL=C
TZ=UTC
/usr/local/libexec/groundplane-postgres16-helper
run,1,<64-lowercase-hex nonce>,<canonical hard-deadline unix nanos>
```

Its only suffixes are:

```text
probe-pg-dump,16
probe-pg-restore,16
probe-psql,16
server-major,16,<database>
dump,<database>,<role>
restore-list,<source-size>,<source-sha256>
terminate-db-connections,<database>
assert-zero-db-connections,<database>
restore-apply,<source-size>,<source-sha256>,<database>,<role>
post-restore-verify,<database>
```

The only other public operation is
`stop,1,<nonce>,<cleanup-deadline>`. There is no collect, shell, generic
executable/argv/SQL, filename, password, ambient environment, or diagnostic
mode.

Every ExecCreate uses `User="0:0"`, `Privileged=false`, `Tty=false`,
`WorkingDir=/`, empty Env/DetachKeys, zero ConsoleSize, stdout/stderr attached,
and stdin attached only for restore-list/apply. There is no `OpenStdin` field.
One acknowledged execution uses exactly one attached start through
ContainerExecAttach / `POST /exec/{id}/start`, Detach false, Tty false, zero
ConsoleSize. Docker raw streams are demultiplexed with `stdcopy.StdCopy`; there
is no second start path or raw parser.

Client argv is fixed:

```text
pg_dump --format=custom --compress=0 --no-owner --no-acl
  --host=/var/run/postgresql --username=postgres --no-password
  --role=<role> --dbname=<database>

pg_restore --list --no-password

pg_restore --clean --if-exists --no-owner --no-acl --exit-on-error
  --single-transaction --host=/var/run/postgresql --username=postgres
  --no-password --role=<role> --dbname=<database>
```

The fixed psql modes use `--no-psqlrc --quiet --tuples-only --no-align
--set=ON_ERROR_STOP=1`, Unix socket, `postgres` user, `--no-password`, and the
selected database. Their fixed SQL proves server major, terminates other
connections only to current database, proves zero remaining, or proves current
database after restore. Expected outputs are exactly `16\n`, `t\n`, `0\n`, and
`<database>\n`.

The four SQL strings are exact and are passed directly as one `--command`
argument:

```sql
SELECT pg_catalog.current_setting('server_version_num')::integer / 10000;

SELECT pg_catalog.coalesce(pg_catalog.bool_and(pg_catalog.pg_terminate_backend(a.pid)), true) FROM pg_catalog.pg_stat_activity AS a WHERE a.datname = pg_catalog.current_database() AND a.pid <> pg_catalog.pg_backend_pid();

SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity AS a WHERE a.datname = pg_catalog.current_database() AND a.pid <> pg_catalog.pg_backend_pid();

SELECT pg_catalog.current_database();
```

Capture stdout is the sole artifact. Capture stderr is discarded at 32 KiB.
Each probe has independent 32-KiB stdout and stderr caps. Restore-list stdout is
drained/discarded in O(1) memory with checked u64 count and no size limit;
stderr is 32 KiB. Restore-apply stdout and stderr have independent 32-KiB caps.
No diagnostic byte is logged, persisted, returned, or used as protocol input.

Raw ExecInspect is decoded by one presence-preserving narrow adapter because the
high-level Moby client flattens nullable exit status to zero. Expected exec id,
container id, `Running=false`, and present 0..255 exit are required. Null,
missing, malformed, identity mismatch, signal-like/forbidden exit, or Moby error
is recovery-required, never zero. Helper exit zero is still insufficient:
operation streams, helper child/reap/state proof, and same-container terminal
ExecInspect must all agree.

## Private gate and single reap

Before any process exists, helper constructs one immutable launch intent with
schema/protocol versions, nonce, operation/deadline, supervisor identity, fixed
gate/client file identities, complete argv/environment, fd profile, exact child
security, nofile 64, launch-profile digest, and selected seccomp digest. It has
no secret or generic process field. The complete intent is at most 32,768
bytes. Helper durably writes `phase_created` before spawning.

The freestanding, static, single-threaded gate receives only the closed internal
gate argv and independently rebuilds the intent. Its descriptors are exactly
operation-selected 0/1/2, fd 3 release pipe, and fd 4 status pipe; 3/4 are
CLOEXEC and no fd >=5 exists.

Before READY it performs, in order: `setpgid(0,0)` and prove pgid=pid; set both
nofile limits to 64; arm parent-death SIGKILL; recheck parent pid/start/boot;
rebuild/match intent; send READY. Parent authenticates pidfd, pid/pgid/start/
boot, executable, argv, root identity, intent, limits, and exact fd set, then
durably records `gate_durable` and `gate_released` with file and parent fsyncs
before writing the sole fd-3 byte `0x01` and EOF.

After release, gate rechecks parent; drops every capability from the bounding
set; clears groups; sets all gid and uid variants to 70; clears all capabilities;
sets no-new-privileges; rearms parent-death signal; rechecks parent; verifies
fixed client file; installs the platform seccomp filter; applies
`close_range(3, UINT_MAX, CLOSE_RANGE_UNSHARE|CLOEXEC)`; verifies exact fd
profile and no fd 5..63; emits PROFILE_APPLIED; and directly `execve`s the fixed
client. Wrong architecture kills the process. The filter denies fork, vfork,
clone, and clone3 with EPERM.

PROFILE_APPLIED is the sole deterministic pre-exec security/fd proof. After it,
the gate has only the fixed exec edge. Exec failure emits the only valid
sequence-3 FATAL then exits. Fd-4 EOF alone is insufficient. With no FATAL, the
original parent makes one non-reaping pidfd wait observation: still alive means
exec evidence 1; a normal fast exit means exec evidence 2 and the later raw
Wait4 status must match. Signal/core/other wait, missing/inconsistent FATAL, or
any gate invariant is recovery-required.

All ordinary observations are non-reaping pidfd waitid calls. Only after a
positive terminal observation may the original parent call
`Wait4(exact_positive_pid)`; it retries EINTR and permits exactly one successful
reap. ECHILD, a different pid, a second success, or blocking wait without prior
terminal evidence is recovery-required. Every signal targets only the
authenticated positive leader pidfd; negative pid/process-group signals are
forbidden.

Gate status frames are length-prefixed, at most 4,096 bytes, and contain exact
magic/schema/sequence/kind/nonce/intent digest. READY is sequence 1,
PROFILE_APPLIED sequence 2, and FATAL sequence 1, 2, or 3 according to whether
failure occurred before READY, after READY, or after profile. The fixed FATAL
stage order is:

```text
1 setpgid; 2 setrlimit; 3 initial pdeathsig; 4 initial parent identity;
5 intent rebuild; 6 READY frame; 7 release read; 8 post-release parent;
9 cap_last_cap; 10 bounding drop; 11 groups; 12 gids; 13 uids;
14 capability clear; 15 no_new_privs; 16 pdeathsig rearm;
17 final parent identity; 18 client file; 19 seccomp;
20 close_range/fd set; 21 PROFILE frame; 22 execve
```

Duplicate, skipped, partial, oversized, wrong nonce/digest, wrong stage/sequence,
extra-after-final, or noncanonical frames fail closed. A partial READY/profile
write closes fd 4 without appending FATAL.

The selected client's fd 0/1/2 profile is exact:

| Operations | fd 0 | fd 1 | fd 2 |
| --- | --- | --- | --- |
| probes, server-major, terminate, zero-connections, post-verify | read-only `/dev/null` | validating result pipe | 32-KiB diagnostic pipe |
| dump | read-only `/dev/null` | sole artifact pipe | 32-KiB diagnostic pipe |
| restore-list | validated source-input pipe | O(1) discard/count pipe | 32-KiB diagnostic pipe |
| restore-apply | validated source-input pipe | independent 32-KiB diagnostic pipe | independent 32-KiB diagnostic pipe |

The canonical fd profile codes are 1 read-only `/dev/null`, 2 validated restore
input, 3 validating result, 4 artifact output, 5 bounded diagnostic, and 6
discard/count. Other operation/code combinations reject.

Status evidence uses exact domains. Each frame digest hashes the complete
`u32(payload_length)||canonical status payload` under respectively
`groundplane.postgres16.gate-ready-frame.v1`,
`.gate-profile-applied-frame.v1`, or `.gate-fatal-frame.v1`. Applied profile
hashes canonical security profile followed by its raw fd-set digest under
`groundplane.postgres16.gate-applied-profile.v1`. Payload-only, unframed, wrong-
kind, or double-framed hashing is invalid.

## Durable helper state

`/run/groundplane-postgres16` is the managed image writable layer, not a mount.
Helper revalidates its inode/owner/mode descriptor-relatively before every
access and never creates, repairs, chmods, or chowns the directory. State
survives Agent/Controller reconnect/restart and stop/start of the same container.

One nonce-named state file is exclusively created and durably advanced by
temp-write, file fsync, atomic rename, and parent fsync. It is removed only after
normal reap or authenticated recovery retirement, followed by parent fsync.
The uid-70 client never opens the directory or inherits its descriptor.

Canonical state uses unsigned big-endian `u8/u32/u64`, bool 0/1, raw 32-byte
digests, raw 16-byte boot id, u32-length bytes/text, and u32-count vectors.
Text is UTF-8 without NUL. Fields occur exactly once in declared order with no
tags, padding, alignment, unknowns, implicit defaults, or trailing bytes.

The launch intent is at most 32,768 bytes. Helper state is at most 65,536 and
contains, in order: magic/schema, monotonically increasing sequence, phase and
progress phase, prior state digest, launch intent and digest, supervisor;
presence-gated READY/release/profile/FATAL facts; presence-gated child identity
and exec evidence; presence-gated I/O counters/hashes/EOF; presence-gated
terminal kind/status/sole-reap evidence; and a final record digest. The first
prior digest is zero and every replacement names the previous record digest.
Same-phase replacement may only increase bounded I/O counters.

Phases are exact:

```text
phase_created -> gate_durable -> gate_released -> profile_applied
-> child_durable -> io_complete -> terminal -> reaped
```

The original parent may transition any reached phase to terminal, then reaped,
then unlink. A different recovery invocation may transition any reached phase
only to `recovery_retired`, then unlink. Invalid encoding, digest, sequence,
presence, phase edge, or same-phase mutation retains state and returns
recovery-required.

## Recovery retirement

Recovery is pidfd-first for every supervisor, gate, client, or scan candidate.
`/proc` enumeration yields only a numeric candidate. Recovery opens its pidfd,
requires zero-timeout `ppoll(POLLIN)` to show nonterminal before and after one
complete `/proc` identity snapshot, and matches stored pid/parent/pgid/start/
boot, all credentials/groups/capabilities, security profile, executable,
argv/environment, and fds before signaling. Failure, disappearance, mismatch,
or terminal POLLIN is not signal authority. Non-parent recovery never calls
waitid or Wait4.

`phase_created` is not retired merely because no child was recorded. Recovery
first proves supervisor absence and unchanged boot, then performs two stable
complete process scans for an exact root gate with matching parent identity,
file device/inode/digest/mode, nonce, and closed argv. A uid-70 argv copy is
excluded by credentials/file identity. Ambiguous root candidate retains state.

For later gate/client phases, recovery opens stored positive pids first and
requires exact authentication or bounded proof of absence/reuse. Unknown
release consumption or exec is not retirement evidence. Signal uses only
`pidfd_send_signal` to the authenticated positive leader and finite operation
and post-SIGKILL deadlines; waiting uses only bounded pidfd ppoll for POLLIN.

Once the exact lifetime is gone, non-parent recovery writes
`recovery_retired` with `unavailable_not_parent`, fsyncs, unlinks, and fsyncs
parent. Boot-id mismatch positively proves the old lifetime cannot exist and
allows `unavailable_boot_changed` without scan/signal. Neither status is a wait
status, child result, `reaped`, or operation success. Identity ambiguity or
deadline expiry retains state.

The recovery branch applies to every operation. For restore-apply after its
start acknowledgement or durable gate release, retirement or boot change never
proves apply success and never permits replay; the Task remains
recovery-required with artifact, locks, holds, and stopped consumers.

A workload container with unresolved state cannot be removed, recreated,
exchanged, or upgraded. A zero-old-state activation barrier inspects every
managed container before true Ready, positive capacity, or ordinary Backup.
Legacy uid-70 ownership, unknown schema/profile, or any state file blocks
activation. The barrier does not chown, delete, migrate, adopt, or interpret old
state. Authentication, false Ready, inventory, dispositions, stop, and recovery
remain available.

## Capture and restore barriers

PostgreSQL capture uses one `pg_dump` under exclusive-unknown staging. It never
estimates, counts, dry-runs, or performs a second dump. Agent stages and probes,
ExecCreates but does not start, then obtains Controller acknowledgement for a
fresh dump-start intent binding point, nonce, container/exec ids, repository and
label digests, adapter version, database/role, and the applicable plaintext
ceiling. No helper or stdout byte exists before ack.

After acknowledgement the start right is spent. Agent reattests and attaches/
starts once, continuously streaming stdout. Reconnect never attaches or starts
that exec again because Docker cannot prove it unstarted and a lost hijack
cannot recover stdout. It authenticates/stops/retires the exec and discards
stage. Only one continuous stream through helper reap, terminal ExecInspect,
fsync/fstat/hash can create `artifact_prepared`. Failure creates no salvageable
archive, even if local bytes parse. Generic retry uses a new point/execution and
new barrier.

Restore opens the retained decoded source twice from offset zero. The list and
apply helpers receive at most 32-KiB chunks; Agent closes write only at exact
EOF. Helper verifies exact size/SHA and one extra EOF read. There is no Docker
copy or in-container source file.

After list validation, Controller holds Attach, grants, and every consuming or
fact-exposed Service across Environments; locks affected Environments in id
order; and orders Services by stable id. The Agent stops prior-running Services,
then performs termination and zero-connection proof.

Apply is ExecCreated but unstarted until Controller acknowledges an intent
binding nonce, container/exec, repository/labels, source evidence, database,
role, target/dependency revisions, locks, and holds. No apply helper or stdin
byte exists before ack. After ack, Agent reattests and uses a fresh second reader
for its sole attached start. This is the destructive boundary.

Crash before acknowledgement is safe redo. After acknowledgement the attempt is
spent and cannot be blindly started or reapplied. Positive terminal proof is
exact input size/hash/EOF, child exit zero and sole reap, plus same-container
ExecInspect not-running/zero. Anything less—including zero forwarded bytes,
pre-exec failure, boot change, unknown release consumption, or missing wait
status—is recovery-required. Only after positive proof may fixed post-restore
verification run. Once verification is checkpointed, restart failure resumes
only consumer recovery and never apply.

## Required verification

Release proof covers authenticated source/platform descriptors, inherited
fields, exact layers/Env/security, helper/gate/state metadata, no-volume rule,
rootfs escalation absence, exact Exec request and stream demultiplexing, shared
vocabulary, and byte-identical published release authority.

Golden/corrupt/fuzz proof covers every state/intent field order, enum, presence
rule, digest, size, status stage, phase edge, and forbidden mutation. Crash and
race injection straddles every state write/fsync, spawn, READY, release,
PROFILE_APPLIED, exec EOF, fast/slow terminal classification, I/O, signal,
terminal observation, sole Wait4, unlink, and parent fsync. It proves pidfd-only
positive-leader signaling, no group/negative-pid signal, exact one reap, safe
boot-change retirement, and recovery-required for every acknowledged apply
without complete successful proof.
