# Backup artifacts and restore publication

Read this document when producing, validating, or restoring Backup bytes. It
owns source representations and target-local publication. The [Agent protocol](agent-protocol.md)
owns wire transport, authority, checkpoints, and reconnect. The [PostgreSQL
runtime](postgresql.md) owns the managed image and helper process contract.

## Stored object

The decoded source is exactly one of:

- canonical `environment-config-v1` USTAR;
- canonical `volume-tar-v1` USTAR; or
- opaque PostgreSQL 16 `pg_dump` custom-format bytes with compression disabled.

There is no outer compression. With `encryption: none`, stored bytes equal
source bytes. With `encryption: age`, stored bytes are unarmored binary age v1
for exactly one X25519 recipient. Armor, another recipient stanza, content
encoding, archive, or compressor is invalid.

For source length `S`, the fixed encoder produces:

```text
chunks(S) = max(1, ceil(S / 65536))
T(S)      = S + 184 + 16 * chunks(S)
```

Arithmetic, allocation, and offsets are checked unsigned 64-bit values. Exact
vectors are:

```text
S =      0 -> T(S) =    200
S =      1 -> T(S) =    201
S =  65535 -> T(S) =  65735
S =  65536 -> T(S) =  65736
S =  65537 -> T(S) =  65753
S = 131072 -> T(S) = 131288
S = 131073 -> T(S) = 131305
```

The universal stored-object ceiling is 5 TiB:

```text
L = 5 * 2^40 = 5,497,558,138,880
```

Unencrypted plaintext may equal `L`. With age the maximum plaintext is
`5,496,216,289,016`: its 83,865,605 chunks produce exactly `L`; one more byte
does not fit. Config has smaller closed bounds below. Volume uses its exact
preflight. PostgreSQL enforces the applicable ceiling before every incremental
allocation and write.

A producer completes EOF, validates the format, fsyncs the source file and
directory, and proves exact `fstat` length and SHA-256. Age is written once to
a distinct file while the source remains. The final stored file is fsynced and
hashed over its exact length.

One source attempt allocates a stable Recovery Point id before execution. Its
object key is exactly:

```text
<prefix>/<environment-id>/<source-id>/<recovery-point-id>/artifact.bin
```

An empty normalized prefix omits that component. Labels and caller-chosen path
components never enter the key. Put uses immutable-create semantics and never
overwrites an existing object.

The schema-1 metadata set contains exactly these seven sorted lowercase keys:

```text
groundplane-encryption
groundplane-format
groundplane-point-id
groundplane-source-sha256
groundplane-source-size
groundplane-stored-sha256
groundplane-stored-size
```

Values are the closed tokens, canonical point id, lowercase hex digests, and
canonical unsigned decimal lengths derived from sealed authority. ETag is not
an integrity digest. A present S3 VersionId, including an empty-present value,
is the immutable discriminator; only absent VersionId selects the non-empty
ETag. Head must match object key, all metadata, the four source/stored evidence
values, and the immutable discriminator where Put returned one.

Before restore mutation, the Agent Heads the exact object, downloads and fsyncs
all stored bytes, verifies stored length/SHA-256, decrypts to a distinct source
when needed, verifies source evidence, and strictly parses through exact EOF.
No download or decrypt stream feeds the live target.

## Canonical USTAR

Config and Volume use 512-byte USTAR headers with the standard fields at these
exact offsets and widths:

```text
name       0 / 100      mode      100 / 8
uid      108 /   8      gid       116 / 8
size     124 /  12      mtime     136 / 12
checksum 148 /   8      typeflag  156 / 1
linkname 157 / 100      magic     257 / 6
version  263 /   2      uname     265 / 32
gname    297 /  32      devmajor  329 / 8
devminor 337 /   8      prefix    345 / 155
padding  500 /  12
```

`magic` is `ustar\0`; `version` is `00`. Linkname, uname, and gname are empty;
device numbers are zero; bytes 500..511 and every unused byte are zero. Regular
files use ASCII `0`, never NUL; directories use `5`; permitted Volume-local PAX
headers use `x`.

Numeric fields are unsigned ASCII octal only. Mode, uid, gid, devmajor, and
devminor are seven zero-padded digits plus NUL. Size and mtime are eleven plus
NUL. Checksum is six zero-padded digits, NUL, and space, computed with its eight
bytes treated as spaces. Base-256 and signed forms reject.

Payload uses minimum zero padding to a 512-byte boundary. Exactly two zero
blocks and EOF terminate the archive. Missing, additional, or non-zero footer
blocks, trailing bytes, truncation, non-zero padding, or checksum mismatch
rejects.

```text
round512(x) = x + ((512 - (x mod 512)) mod 512)
```

## Environment Config format

### Members and manifest

`environment-config-v1` contains `manifest.json`, then
`values/<entry-id>` in ascending unsigned UTF-8 bytes of stable Entry id. No
other member or PAX header is allowed. All headers have empty prefix/linkname/
uname/gname, zero uid/gid/mtime/devices, and type `0`. Manifest and plain values
use mode `0444`; secret values use `0600`.

`manifest.json` is RFC 8785 JCS without BOM or terminal LF. Its root has exactly
`entries,format`; format equals `environment-config-v1`. Entries are id-sorted
and contain exactly exposure, id, destination metadata, secret boolean, desired
source descriptor, and selected-value evidence. The closed shapes are:

```json
{"key":"NAME","type":"env"}
{"gid":0,"mode":292,"path":"relative/path","type":"file","uid":0}
{"kind":"literal"}
{"kind":"secret_ref","secret_ref":"key"}
{"attach_id":"att_...","fact":"pg16_URL","kind":"fact"}
{"attach_id":"att_...","fact":"pg16_URL","grant_attach_id":"att_...","kind":"fact"}
{"kind":"all"}
{"kind":"services","service_ids":["svc_..."]}
{"path":"values/<entry-id>","sha256":"<64 lowercase hex>","size_bytes":0}
```

JCS supplies key order. Missing, unknown, duplicate, or inapplicable fields
reject. File mode is decimal 292 for plain and 384 for secret. File uid/gid are
explicit uint32 values. Env Entries have no file fields and file Entries have
no env key. Service ids are non-empty, unique, byte-sorted, and limited to 128
per Entry. Environment keys are 1..255 accepted bytes; file destinations are
1..240 accepted relative-path bytes.

The `secret` boolean is explicit and required. It is never inferred from mode,
source, or an omitted protobuf default. Stable ids identify Entries, Attaches,
grants, and exposure Services.

`value.path`, size, and SHA-256 match the exact value member. Values never
appear in the manifest. Restore retains the artifact's literal, reusable-secret
key, or fact/grant descriptor but selects the captured immutable value bytes;
it does not re-resolve a current Secret or fact to replace them. Env-destination
values from every source are UTF-8 without NUL. Literals remain authored UTF-8.
File-destination secret/fact values may be arbitrary bytes. No normalization,
replacement decoding, truncation, or coercion occurs.

### Bounds and length

Let `N` be Entries, `V_i` each value size, `T` their sum, `M` manifest bytes,
and `S` source archive bytes:

```text
0 <= N <= 4096
0 <= V_i <= 262144
T <= 1073741824
deterministic direction-selected Entry metadata <= 7936 bytes
complete schema-1 EntryHeader outer envelope <= 8118 bytes
sum(deterministic metadata bytes) <= 32505856
M <= 67108864
S <= 1142949376
age stored length <= 1143228616
age peak source-plus-stored staging <= 2286177992

footer = 512 * (N + 1) + round512(M) + sum(round512(V_i))
S      = footer + 1024
```

Marshal bounds are checked after encoding. Zero Entries is a valid full
replacement. The empty manifest is exactly:

```text
{"entries":[],"format":"environment-config-v1"}
```

Its manifest SHA-256 is
`b42cc47ac095a7216a02db99c618694c48e69f0e9c230731c6c8f04943c5c37a`,
manifest-header checksum bytes are `011706\0 `, complete length is 2048, and
complete tar SHA-256 is
`0b726da4797d0420a45848a567ed63cb4798a2f8c688a3340e01f9a4fbc6904f`.

### Capture and restore

Capture reads Entry primaries, selected immutable value generations, and values
from one linearizable etcd revision. The Controller derives canonical metadata,
manifest, bounds, and hashes and sends typed metadata/values to the Agent. The
Agent places values directly at final tar offsets, independently regenerates
manifest bytes, fills footer, fsyncs, checks `fstat=S`, and hashes exactly `S`.
It creates no second plaintext spool.

Restore is two passes over the same immutable decoded file:

1. Parse through EOF and validate every header, order, path, JCS byte, shape,
   bound, member size/hash, padding, footer, and source digest. Retain only
   bounded typed manifest data and one 32-KiB value buffer. Change no target.
2. After the Controller accepts complete content authority, rewind to zero,
   revalidate framing, and transfer typed metadata and selected values in
   archive order. Verify the complete transcript before publication.

Restore replaces the complete Environment Entry set. Artifact Entry ids remain
canonical primary ids; omitted Entries are deleted and present Entries are
upserted. There is no Environment-wide active-generation pointer.

Before value staging, a read-hidden fence pins point, target and coordination
revisions, manifest, Entry ids, retry-stable restore generation, each
preallocated `cfg_` value generation, next positive render generation,
dependency revisions/holds, and bounded cursors. It validates every exposure
Service, fact/grant Attach, reusable Secret, and surviving external Entry
consumer. Missing/cross-scope dependencies, Secret-kind mismatch, an omitted
consumed Entry, or incompatible immutable type/destination/secrecy/ownership
fails before mutation. Reads and conflicting writes return busy/state conflict
while hidden.

Values are written as hidden immutable generations. Canonical Entry primaries
and owner indexes roll forward in stable-id batches under the same fence,
coordination, dependency, and cursor checks. After the first primary batch,
recovery only rolls forward and never reallocates ids. One final transaction
publishes the preallocated render generation and removes read hiding. The Agent
materializes exactly that generation. Post-publication failure resumes
materialization, not publication. Acknowledged cleanup removes only superseded
unreferenced generations/staging, releases a dependency hold after its last
batch, and removes the fence last.

## Volume format

### Canonical tree

`volume-tar-v1` orders root `.` first and descendants by ascending unsigned raw
path bytes. There are 1..2,048 logical entries including root; paths are at most
4,095 bytes and unique. A descendant has slash separators but no leading slash,
`./`, empty, `.`, or `..` component, or trailing slash.

Only directories and regular files are supported. Directories have size zero.
Both preserve uint32 uid/gid and low twelve permission bits. Files preserve
length, bytes, and SHA-256. Mtime is zero. Capture rejects symlinks, hard-linked
files, devices, sockets, FIFOs, magic links, sparse semantics, flags, ACLs,
capabilities, labels, xattrs, nested mounts, and filesystem transitions.
Traversal is descriptor-relative with
`RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV`
and a same-device bind-mount topology check.

Plain USTAR is used when path and numerics fit. Path splitting selects the
rightmost slash with non-empty prefix at most 155 and name at most 100 bytes;
no-prefix name is at most 100. Otherwise exactly one local PAX `x` header
immediately precedes the member. Allowed keys, in this order when needed, are
`path,uid,gid,size`, each once. Numeric values are canonical unsigned decimal
and path is exact UTF-8.

A PAX record is `<length> <key>=<value>\n`; length has no leading zero, includes
its own digits, and is the unique byte-length fixed point. Header name is
`PaxHeaders/<ordinal>` and overridden payload name is
`PaxPayload/<ordinal>`, using zero-based twelve-digit decimal ordinal. Its
mode/uid/gid/mtime are zero and prefix empty. An overridden numeric field in the
following header is zero; other fields remain canonical. Non-UTF-8 paths are
valid only when plain USTAR can encode them. Global, GNU, sparse, vendor, xattr,
ACL, unknown, chained, or unconsumed extensions reject.

Tree digest input is exactly:

```text
ASCII "groundplane.volume-tree.v1"; NUL; entry count u64 BE
for each entry:
  path length u32 BE; raw path
  kind byte (directory=1, regular=2)
  mode u32 BE; uid u32 BE; gid u32 BE; size u64 BE
  content SHA-256 32 raw bytes
```

Directories contribute size zero and 32 zero digest bytes. The digest is not an
archive member. The content-manifest digest separately hashes canonical ordered
manifest messages and is independent of task authority and transfer replay.

The immutable Volume projection pins Volume/Environment revisions, Blueprint
seal revision, Compose artifact id/digest/revision, render generation, Compose
volume key, generated Docker Volume name, and exact derived authorized
directory. Artifact revision equals the Blueprint seal revision, and projection
and seal carry the same render generation. Every consumer reference resolves
to the Task's sole common Service fact set. None of these facts is reconstructed
from mutable runtime state.

Before capture, exact tar bytes plus 67,108,864 bytes must fit Agent staging.
Before restore construction, total regular-file bytes plus 67,108,864 bytes and
enough inodes must fit the target filesystem. Query, overflow, quota,
allocation, or inode failure rejects before output or mutation.

### Consumer quiescence

The Controller pins each mounting Service and its prior runtime intent and
orders effects by stable Service id. The Agent alone stops, inspects, restarts,
and health-checks. A sealed `RUNNING` Service crosses acknowledged stop intent,
stopped proof, restart intent, restarted proof, health wait, and healthy proof.
A sealed non-running Service is recorded `NOT_RUNNING` and never started.
Mismatch with the sealed intent fails before stop. Capture success requires all
previously running consumers healthy; upload may overlap only after restart and
does not erase its recovery cursor.

### Restore publication

Restore persists separate immutable manifests for the validated new tree and
the stopped live old tree. The latter comes from a filesystem scan and is not
represented as source-archive evidence. Each manifest durability transaction
contains exactly 1..31 immutable Entry puts plus one progress put, at most 32
operations and 262,144 key/value bytes. A non-final batch has exactly 31
entries. At most 67 commits hold a 2,048-entry manifest. Progress pins role,
point/restore ids, both tree digests, count, cursor, transfer chain, completion,
and prior root revision; entries never rewrite.

The helper creates root-owned mode-`0700`
`.groundplane-restore-<restore-generation-id>` beside the live root under the
authorized parent. It uses one pre-opened parent fd and descriptor-relative
`openat2`/`*at` calls with the full resolution mask. Absolute reopen,
`/proc/self/fd`, fallback walking, copy, move-aside, and delete-then-rename are
invalid.

Construction follows archive order. Directories remain temporary `0:0/0700`.
Files begin `0:0/0600`, receive and fsync exact content, then receive final
owner/mode, file fsync, and parent fsync. Directories finalize afterward in
descending unsigned path order, descendants before ancestors and root last,
with final owner/mode and directory fsync.

Every construction and directory-finalization mutation has an acknowledged
intent containing point/generation, content-manifest digest, before/after
cursor, ordinal, path, kind, final metadata, size, and content digest as
applicable. Completion advances only after required fsyncs. Recovery recognizes
only absent, exact temporary prefix/state, final owner with temporary mode, or
exact final state described by the intent. It removes/replays temporary state or
finishes metadata/fsync. Anything else is ambiguity.

After finalization the helper fsyncs the parent and verifies the new full-tree
digest. An acknowledged exchange intent retains constant old/new full-tree
digests. It proves same filesystem, calls `renameat2(RENAME_EXCHANGE)` exactly
once, fsyncs parent, and verifies live-new. Resume accepts only
live-old/hidden-new, which exchanges once, or live-new/hidden-old, which replays
completion. There is no fallback.

Old-tree deletion orders descendants in descending raw path bytes with root
last. Before each unlink/rmdir the Agent obtains acknowledgement for exact
point/generation, both constant tree digests, deletion cursor, one-based
ordinal, path, kind, and parent. After removal and parent fsync it completes
`volume_replaced_path_cleaned` with the same facts.

With no pending delete, exactly the cursor suffix survives. With a pending
delete, only its exact path may be present or absent; absence permits replay of
only that completion. A gap, completed survivor, changed kind/parent, non-empty
directory, changed live tree, or extra path is ambiguity. Root completion
records the authorized-parent fsync and moves directly to consumer recovery.
A durable cursor equal to entry count and an aggregate tree-cleaned checkpoint
are both invalid.

Restore completes only after every previously running consumer restarts and
passes health. Restart failure resumes Service recovery and never repeats the
exchange.

## PostgreSQL format and restore semantics

Capture is the exact stdout of PostgreSQL 16 `pg_dump` custom format with
compression zero, `--no-owner`, and `--no-acl` for the one provisioned
database. PostgreSQL supplies its database-local snapshot; Groundplane claims
no consistency with another source. The allowlisted helper invokes the client
directly inside the already running managed database container with typed
database and role values, Unix socket, and `--no-password`. No shell, SQL
construction, filename, TCP fallback, credential, or ambient environment is
accepted.

Restore performs a complete `pg_restore --list` validation before mutation. It
then stops and holds every consumer across Environments, terminates connections
only to the selected database, proves zero remaining connections, and applies:

```text
pg_restore --clean --if-exists --no-owner --no-acl --exit-on-error
           --single-transaction --host=/var/run/postgresql
           --username=postgres --no-password --role=<role>
           --dbname=<database>
```

Success additionally requires a fixed direct query proving the current
database name. Failure after the acknowledged apply boundary remains
recovery-required and keeps consumers stopped. After verified apply,
previously running consumers restart in stable-id order through health gates;
restart failure never reapplies the dump.

The format excludes physical layout, WAL, cluster roles, ownership/ACL
restoration, and other databases. Required PostgreSQL 16 extensions and
facilities must already exist.
