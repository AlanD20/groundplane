# ADR 0047: Fix Backup artifact formats and restore publication

- Status: Accepted
- Date: 2026-08-30
- Accepted: 2026-08-30

## Context

ADR 0024 fixes Backup product behavior, source kinds, immutable upload, the
restore overwrite boundary, and durable recovery. ADR 0046 fixes stable Backup
source identity. ADRs 0020 and 0029 fix the filesystem boundary and canonical
Environment Entry primaries. They do not select one byte representation for
`environment-config-v1` and `volume-tar-v1`, one controlled PostgreSQL 16
execution contract for `postgres-custom-v1`, or one crash-recoverable restore
publication procedure.

The paired ADR 0048 owns the sole final Agent execution-plan schema, schema 1:
protobuf fields, queues, execution fences, durable wire acknowledgements,
deduplication, and reconnect sequencing. This ADR owns decoded source bytes,
the semantic Config transcript, strict validation, target-local mutation
invariants, and the facts ADR 0048 must bind. It does not define another
checkpoint vocabulary.

No verified Backup artifact has shipped. Version 1 is replaced cleanly. There
is no permissive version-1 reader, version-2 fallback, compatibility decoder,
or artifact migration.

The prior mutable PostgreSQL workload identity `postgres:16-alpine` is likewise
replaced cleanly. It is removed from adapter defaults, persisted backing-Service
validation, fixtures, renderers, tests, documentation, Compose projections, and
Backup authority. There is no alias, tag fallback, dual-image acceptance,
runtime helper copy, or migration reader. Because no public release exists,
pre-contract backing containers and records are recreated through the canonical
backing-service lifecycle before Backup becomes available; they are never
silently adopted as managed-image authority.

## Decision

### 1. Use one source stream and one optional age wrapper

The decoded source is exactly the canonical Config archive, canonical Volume
archive, or opaque PostgreSQL 16 custom dump defined below. There is no outer
compression. With `encryption: none`, the stored object is the exact source
file. With `encryption: age`, it is the unarmored binary age v1 encoding for
exactly one X25519 recipient. Another stanza, armor, content encoding, archive,
or compressor is invalid.

For source length `S`, stored length is exactly:

```text
chunks(S) = max(1, ceil(S / 65536))
T(S)      = S + 184 + 16 * chunks(S)
```

The pinned encoder contributes a 168-byte one-recipient header, a 16-byte
payload nonce, and one 16-byte authentication tag per 64-KiB payload chunk.
The empty plaintext still has one authenticated chunk. All arithmetic,
offsets, allocations, and encoded lengths are checked as unsigned 64-bit.

```text
S =      0 -> T(S) =    200
S =      1 -> T(S) =    201
S =  65535 -> T(S) =  65735
S =  65536 -> T(S) =  65736
S =  65537 -> T(S) =  65753
S = 131072 -> T(S) = 131288
S = 131073 -> T(S) = 131305
```

The source remains present while a distinct age file is constructed.
Exact-bound producers reserve `S + T(S)` before initial byte credit.
PostgreSQL uses the exclusive incrementally allocated path below. A completed
file requires EOF, `fsync`, exact `fstat` length, and SHA-256 over exactly that
length.

Before target mutation, restore downloads and fsyncs the complete object,
verifies its sealed stored length and SHA-256, decrypts when applicable into a
distinct source file, verifies source length and SHA-256, and strictly
validates the complete source through exact EOF. No network, decrypting, or
partly validated stream feeds a live target.

The universal stored-object ceiling is
`L=5*2^40=5,497,558,138,880` bytes. For encryption none the exact plaintext
maximum is `L`. For age and the fixed formula above, the exact maximum is
`S=5,496,216,289,016`: `ceil(S/65536)=83,865,605` and
`T(S)=S+184+16*83,865,605=L`; `T(S+1)=L+1`. Producers preflight bounded Config
and Volume sources and enforce the limit before each unknown-length PostgreSQL
allocation/write, so no valid evidence can name a stored object above 5 TiB.

### 2. Define canonical USTAR headers and framing

Config and Volume use 512-byte USTAR headers with these offsets and widths:

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
bytes 500 through 511 and every unused byte are zero. Device numbers encode
zero. Regular files use ASCII typeflag `0`, never NUL; directories use `5`;
permitted local PAX headers use `x`.

Numeric fields are unsigned ASCII octal only. Mode, uid, gid, devmajor, and
devminor are seven zero-padded digits plus NUL. Size and mtime are eleven plus
NUL. Checksum is six zero-padded digits, NUL, and one space, computed as the
unsigned sum of the header with its eight checksum bytes treated as spaces.
Base-256 and signed encodings are invalid. Short names/prefixes have only zero
padding; exact-width values fill their fields.

Each payload has the minimum zero padding to the next 512-byte boundary. A
zero-size payload has none. Exactly two zero blocks then EOF terminate the
archive. Missing/additional/non-zero footer blocks, trailing bytes, truncation,
non-zero padding, or checksum mismatch rejects it.

```text
R(x) = x                         when x mod 512 = 0
R(x) = x + (512 - (x mod 512))  otherwise
```

### 3. Make `environment-config-v1` exact and bounded

Logical members are `manifest.json`, then `values/<entry-id>` in ascending
unsigned UTF-8 bytes of canonical stable Entry id. There are no other members
and no PAX headers. Every header has empty prefix/linkname/uname/gname, uid and
gid zero, mtime zero, zero devices, and typeflag `0`. Manifest mode is `0444`;
plain value mode is `0444`; secret value mode is `0600`.

`manifest.json` is RFC 8785 JCS with no BOM or terminal LF. Its root has
exactly `entries,format`; format is `environment-config-v1`. Entries are in id
order and each item has exactly
`exposure,id,metadata,secret,source,value` in JCS order. Accepted nested objects
and their JCS key order are:

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

Unknown, missing, duplicate, or inapplicable fields are invalid. File mode is
decimal `292` when plain and `384` when secret. File uid/gid are explicit
uint32 values. Env Entries have no file fields; file Entries have no env key.
Service ids are non-empty, unique, byte-sorted, and at most 128 per Entry.
References are stable ids. `secret` is a required JSON boolean. The schema-1
capture and restore messages use `optional bool` fields whose presence is
required; it is never inferred from mode, source, or protobuf's default false.
Environment keys are 1..255 bytes in the accepted key grammar. File destination
paths are 1..240 bytes in the accepted materialization grammar.

`value.path`, size, and SHA-256 match the exact value member. Values never enter
JSON. A fact or reusable-secret Entry retains the artifact's typed source
descriptor but selects the captured immutable value generation during restore;
it is not resolved against a current Secret, Attach, Fact, Entry revision, or
desired source during restore. Every env-destination value from every source is
valid UTF-8 without NUL. Literals remain authored UTF-8. File-destination fact
and secret-reference values may be arbitrary bytes. Nothing is normalized,
replacement-decoded, truncated, or coerced between destinations.

Closed bounds are:

```text
0 <= N <= 4096
0 <= V_i <= 262144
sum(V_i) = T <= 1073741824
deterministic direction-selected Config Entry metadata <= 7936 bytes
complete schema-1 EntryHeader outer envelope <= 8118 bytes
sum(deterministic direction-selected Entry metadata bytes) <= 32505856
M <= 67108864
S <= 1142949376
age stored length <= 1143228616
age peak source-plus-stored staging <= 2286177992
```

Marshal bounds are checked after encoding. Zero Entries is a valid full
replacement. With entries in order, value member `i` begins at:

```text
512 + R(M) + sum(k < i, 512 + R(V_k))
```

The first footer block and complete length are:

```text
footer = 512 * (N + 1) + R(M) + sum(i, R(V_i))
S      = footer + 1024
```

The maximum holds because the per-value and manifest maxima are 512-byte
multiples: `1073741824 + 67108864 + 512*(4096+1) + 1024 =
1142949376`. At that source maximum, `ceil(S/65536)=17441`, so the exact age
bound is `S+184+16*17441=1143228616`; retaining source while writing age peaks
at `2286177992`. Sender and receiver independently check every formula fact.

The durable schema-1 metadata key is exactly
`/v1/backup/config-metadata/<metadata-id>/<entry-id>`: a 27-byte prefix,
30-byte metadata id, separator, and 29-byte Entry id, totaling 87 bytes.
`metadata-id` is the immutable capture snapshot id for capture or the restore
generation id for restore. `BackupConfigMetadataRow` uses authority digest tag
1, metadata content digest tag 2, uint32 ordinal tag 3, a direction-selected
oneof of capture `BackupConfigEntry` tag 4 or restore
`BackupConfigRestoreEntry` tag 5, and preceding/resulting transfer-chain digests
tags 6/7. At the 7,936-byte metadata limit its exact terms are
`34+34+3+(1+2+7936)+34+34=8078` bytes. Protobuf PutRequest framing is 89 bytes
for field 1 key (`1+1+87`) and 8,081 for field 2 value (`1+2+8078`), totaling
8,170 and leaving 22 bytes below 8,192. The complete EntryHeader uses capture
metadata tag 1 or restore metadata tag 2 and chunk-count tag 3. Its outer
maximum remains 8,118 bytes after task/assignment/step IDs, execution/transfer
digests, record sequence, nested and outer oneof framing.
These bounds replace the earlier unpersistable 256-KiB metadata allowance;
selected value bytes remain separate and retain their 256-KiB bound.

The empty manifest is exactly
`{"entries":[],"format":"environment-config-v1"}`. It has `M=47`, manifest
SHA-256 `b42cc47ac095a7216a02db99c618694c48e69f0e9c230731c6c8f04943c5c37a`,
manifest-header checksum bytes `011706\0 `, `S=2048`, and complete tar SHA-256
`0b726da4797d0420a45848a567ed63cb4798a2f8c688a3340e01f9a4fbc6904f`.

### 4. Bind Config transfer and two-pass restore

ADR 0048 owns schema-1 messages and fences. The semantic transcript is SHA-256
over exactly:

```text
ASCII "groundplane.backup.config-transfer.v1"
0x00
direction                         u32 BE; capture=1, restore=2
manifest_sha256                  32 raw bytes
M                                u64 BE
S                                u64 BE
N                                u32 BE
T                                u64 BE
for ordinal 1..N:
  ordinal                         u32 BE
  P                               u32 BE
  deterministic BackupConfigEntry exactly P bytes for capture, or
  deterministic BackupConfigRestoreEntry exactly P bytes for restore
for ordinal 1..N:
  ordinal                         u32 BE
  chunk_count                     u32 BE
  V_i                             u64 BE
  selected value                  exactly V_i bytes
```

Unknown protobuf fields reject before deterministic marshal. No protobuf
varint envelope, task/assignment/step/fence, record sequence, credit, Start,
EntryEnd, TransferEnd, tar header, padding, or footer enters the transcript.

The transcript therefore contains its authority prefix, all `N` metadata
frames in ordinal order, and then all `N` value frames in ordinal order. Local
restore validation of the complete USTAR source occurs earlier and is separate
from this transcript and transfer order.

Value chunks use canonical 32768-byte segmentation: every non-final is 32768,
the final is 1..32768, and empty has zero chunks, so the maximum is eight. Wire
order in both directions is exactly Start; for nonempty metadata, an immediate
grant of 46 Header records and 393,216 complete serialized bytes; all `N`
EntryHeaders in canonical durability batches, with exactly 46 Headers in every
non-final batch and 1..46 in the final batch. Only after a whole non-final batch
durably commits does the receiver issue one cumulative replacement grant of
exactly 46 records and 393,216 serialized bytes; unused byte slack is not
additive. After the final batch durably commits, the receiver sends
`MetadataAccepted` instead of another metadata grant. For empty metadata it
durably crosses the metadata barrier and sends `MetadataAccepted` directly.
The barrier carries initial value credit; then, for each Entry in ordinal order,
zero through eight ValueChunks followed by EntryEnd without interleaving; then
TransferEnd. Start, EntryHeaders, ValueChunks, EntryEnds, and TransferEnd use
one-based contiguous record sequences. `MetadataAccepted` is a Controller
barrier, not a sequenced transfer record, and enters neither the transcript nor
the record count. Let `C` be the total chunk count. Then:

```text
pre-End record_count = 1 + 2*N + C <= 40961
TransferEnd sequence = record_count + 1
TransferEnd sequence <= 40962
```

Empty transfer is Start sequence 1 and TransferEnd sequence 2 with count 1.
The sender finalizes only after the final EntryEnd validates length, SHA-256,
and chunk count; the receiver compares only after validating end count/sequence.

On capture, one Controller fixed revision computes revision-bearing typed
`BackupConfigEntry` messages, selected
bytes, canonical manifest, `M`, digest, `N`, `T`, and `S`. The Agent writes
values directly at final tar offsets, independently regenerates the manifest,
requires every sealed value and formula fact, fills the reserved manifest and
footer, fsyncs, requires `fstat=S`, and hashes exactly `S` bytes. It creates no
second plaintext value/archive spool.

The Controller persists those typed Entries as the bounded immutable metadata
snapshot whose exact revision/count/bytes/content digest are sealed by ADR 0048.
No Entry is embedded in TaskAssignment authority; all `N` Headers come from
that snapshot during the metadata phase.

The snapshot content digest is transfer-independent and exactly:

```text
SHA256(ASCII "groundplane.backup.config-metadata-content.v1" || 0x00 ||
       u32be(N) ||
       for ordinal 1..N:
         u32be(P) || deterministic BackupConfigRestoreEntry[P])
```

Capture canonically projects each revision-bearing `BackupConfigEntry` to the
artifact's `BackupConfigRestoreEntry`; restore parses that byte-identical
projection. There is exactly one `u32` length prefix per metadata message.
Authority, transfer id/direction, record sequence, credit, ack, and
transfer-chain state are excluded. Capture authority and restore authority carry
the same round-tripped digest before ADR 0048 derives the separate
direction-selected authority-bound transfer chain, so no digest cycle exists.

Before the schema-1 artifact completion authority exists, capture reconnect
removes every partial Config source/stored file, transcript state, and credit
balance and restarts at record sequence 1 with zero credit, then waits for the
new Start-scoped metadata grant. It never adopts a
partial member cursor or verified byte prefix. Once that completion authority
exists, stage recovery follows section 10 and never retransfers or re-encrypts
the acknowledged final.

Config restore is exactly two bounded passes over the same immutable source.
It never constructs or accepts a capture-side `BackupConfigEntry`. The restore
metadata record is `BackupConfigRestoreEntry`: exact authenticated stable Entry
id tag 1, parsed destination metadata tag 2, parsed exposure tag 3, artifact
desired-source descriptor tag 4, presence-required optional secret tag 5, and
selected-value size/SHA-256 tag 6. The desired-source oneof is literal tag 1,
secret-ref authored key tag 2, or fact Attach id/optional grant Attach id/fact
tag 3. It contains no revision, generation, resolved Secret id/value, fact
value, current-source evidence, or lookup result. Every field is derived from
the authenticated canonical manifest and member evidence. The Controller may
validate dependency existence, kind, scope, and classification, but MUST NOT use
that lookup to replace the authenticated selected bytes.
Its deterministic encoding has the same 7,936-byte ceiling, aggregate ceiling,
durable-row proof, and 8,118-byte complete Header ceiling as capture metadata.

The two passes are:

1. Pass one parses through exact EOF and validates every header, order, path,
   JCS byte, shape, cap, member size/hash, padding, footer, and source digest.
   It derives `N,T,M,S` and manifest digest while retaining only bounded typed
   manifest data and one 32-KiB value buffer. It changes no target.
2. After the Controller durably accepts that complete content authority, pass
   two opens at offset zero, revalidates framing, emits typed restore
   Entries/values in
   archive order, and produces the exact restore transcript. The Controller
   verifies it and all selected values before staging an Entry generation.

Restore reconnect before durable `MetadataAccepted` removes receiver metadata,
the incomplete value buffer, transcript state, and credit balance, then
restarts pass two at Entry 1, record sequence 1, with zero credit and waits for
new metadata credit. On
`MetadataAccepted`, the Controller retains the bounded typed metadata,
destination/exposure publication holds (never desired-source-resolution holds), and preallocated generation ids. Each later EntryEnd
atomically protects that Entry's hidden immutable generation and advances the
durable Entry ordinal and transcript hash-chain. Reconnect after
`MetadataAccepted` discards only the incomplete current value buffer and
starts the value phase at the first uncommitted Entry ordinal. All old-session
credit is dead; Resume must match the durable cursor/hash/ack and receive a new
explicit value-credit grant before the next chunk. It never replays
an EntryHeader and never retransmits, rewrites, or re-encrypts a committed
Entry's value or hidden generation. After the
schema-1 transfer-completed authority exists, no transfer record is replayed
and Controller publication resumes only from its durable cursors. No Entry
primary or materialization changes during pass one or incomplete pass two.

### 5. Publish Config through canonical Entry primaries

Restore fully replaces the Environment Entry set. Artifact Entry ids remain
primary ids; omitted Entries are deleted and present Entries are upserted.
There is no Environment-wide active-generation read model.

If this paired ADR is accepted, this canonical-primary/read-hiding procedure
cleanly supersedes ADR 0024's Environment-wide active Entry-generation switch.
Until acceptance, ADR 0024 remains authoritative; implementations MUST NOT mix
the two publication models.

Before staging, one read-hidden Controller fence pins point, target and
coordination revisions, manifest digest, ordered Entry ids, retry-stable restore
generation, all preallocated `cfg_` generations, exact next positive render
generation, dependency revisions/holds, phase, and bounded-batch cursors. A
fixed-revision Service-id-to-current-name map is pinned for exposure conversion.

Every exposure Service, fact/grant Attach, resolved reusable Secret, and
surviving external Entry consumer is validated and held before staging.
Missing/cross-scope dependencies, Secret kind mismatch, omitted consumed Entry,
or incompatible immutable type, destination, secrecy, ownership, or owner
fails before mutation. Ordinary reads and conflicting writes see busy/state
conflict while read-hidden.

After complete transfer, immutable value generations are written/verified in
bounded transactions. Canonical `/v1/records/entries/<entry-id>` primaries and
owner indexes roll forward in byte-sorted batches. Each transaction compares
fence, coordination, dependency, and cursor state and advances atomically.
After the first primary batch, every recovery rolls forward and never exposes a
mixed set or reallocates ids.

One final transaction publishes the preallocated render generation and removes
read hiding. The Agent converges exactly it. Post-publication failure resumes
materialization, never republishes. After exact acknowledgement, bounded
cleanup removes only superseded unreferenced generations/staging, releases
holds after their last dependent batch, and removes the fence last.

### 6. Make `volume-tar-v1` a closed projection

Authority pins stable Volume/Environment ids and revisions, the immutable
projection coordination root, Compose artifact id/digest/revision, render
generation, Compose volume key, generated Docker Volume name, and exact derived
authorized directory. ADR 0048 carries those eight projection facts verbatim in
the required `BackupVolumeProjectionAuthority`; none is reconstructed or
guessed from runtime state. The coordination root is the ModRevision of the
immutable `EnvironmentBlueprintSeal` at
`/v1/records/environment-blueprint-revisions/<environment-id>/<desired-revision-id>/root`.
The historical `EnvironmentComposeProjection`, its embedded `ComposeArtifact`,
and the seal are one projection authority: artifact revision equals that root
ModRevision, and the projection and seal carry the same render generation. The
step's consumer references and sole common Service fact set additionally pin
every mounting Service, Service and Compose revision, and exact prior runtime
intent. Slugs, caller paths, and supplied directories are invalid. Projection,
ownership, mount set, device, mount id, and topology are revalidated before
every capture/mutation boundary.

Archive order is root `.` then descendants in ascending unsigned raw path
bytes. Paths are at most 4095 bytes. There are at most 2048 logical entries
including root. Root is the sole exception permitting the complete path `.`.
Descendants have `/` separators and no leading `/` or `./`, empty/`.`/`..`
component, or trailing slash. Each path occurs once.

```text
DIRECTORY = 1
REGULAR   = 2
```

USTAR typeflags stay `5`/`0`; numeric kinds are used by manifests, digests,
and intents. Directories have size zero. Both kinds preserve uint32 uid/gid
and low twelve mode bits. Files preserve length, bytes, and SHA-256. Mtime is
zero. Capture rejects links, multi-link files, devices, sockets, FIFOs, magic
links, sparse semantics, flags, ACLs, capabilities, labels, xattrs, nested
mounts, and filesystem transitions. Traversal is descriptor-relative with
`RESOLVE_BENEATH|RESOLVE_NO_SYMLINKS|RESOLVE_NO_MAGICLINKS|RESOLVE_NO_XDEV`
plus a same-device bind-mount topology check.

Plain USTAR is used when path and numerics fit. Path splitting chooses the
rightmost `/` with non-empty prefix <=155 and name <=100 bytes; no-prefix name
is <=100. Otherwise exactly one local PAX `x` header immediately precedes the
member. Allowed keys, in order when needed, are `path,uid,gid,size`, once each.
Numeric values are canonical unsigned decimal; path is exact UTF-8.

PAX records are `<length> <key>=<value>\n`; length has no leading zero,
includes its digits, and is the unique byte-length fixed point. Header name is
`PaxHeaders/<ordinal>` and path-overridden payload name is
`PaxPayload/<ordinal>`, with zero-based ordinal as twelve decimal digits. PAX
header mode/uid/gid/mtime are zero, prefix empty, and size exact. An overridden
numeric in the next header is zero; others stay canonical. Non-UTF-8 paths are
accepted only in plain USTAR. Global PAX, GNU, sparse, vendor, xattr, ACL,
unknown, chained, or unconsumed extensions reject.

Tree digest input is:

```text
ASCII "groundplane.volume-tree.v1"; 0x00; entry count u64 BE
for each entry in archive order:
  path length u32 BE; raw path bytes
  kind byte (DIRECTORY=1, REGULAR=2)
  mode u32 BE; uid u32 BE; gid u32 BE; size u64 BE
  content SHA-256 32 raw bytes
```

Directories contribute size zero and 32 zero digest bytes. The digest is not
an archive member.

### 7. Bound Volume manifests and disk

The immutable Volume content manifest digest is independent of transfer,
authority, direction, role, and replay. For canonical entries `E[1..N]`, where
each is unknown-field-free deterministic protobuf and `P_i=len(E[i])`, it is:

```text
SHA256(ASCII "groundplane.backup.volume-manifest-content.v1" || 0x00 ||
       u32be(N) ||
       for i=1..N: u32be(P_i) || E[i])
```

The separate transfer member framing is identically:

```text
SHA256(ASCII "groundplane.agent.v1.volume-manifest-transfer-chain" || 0x00 ||
       previous_chain || u64be(record_sequence) || u32be(first_ordinal) ||
       u32be(batch_count) ||
       for each Batch entry i: u32be(P_i) || E[i])
```

Both formulas contain exactly one `u32be(P_i)` length prefix per deterministic
Entry and no protobuf wrapper or second length. The transfer seed, defined in
ADR 0048, binds authority, transfer id, direction, role, content/full-tree
digests, entry count, and the role-specific source-evidence discriminator.

Ordinals are contiguous from one, root is first, and paths retain canonical
unsigned-byte order, so capture and restore of the same manifest MUST reproduce
the same digest. ADR 0048 separately defines a transfer chain that binds
authority, transfer, direction, role, source-evidence presence, record sequence,
and replay. Neither digest substitutes for the other.

Restore persists validated new and stopped-live old manifests as separate
immutable sets before construction. Each entry is one etcd key/value whose
combined encoded key-plus-marshaled-value is <=8192 bytes. ADR 0048's dedicated
bulk manifest transfer carries exactly 31 entries in every non-final canonical
wire Batch and 1..31 in the final Batch. For `N` entries,
`b=ceil(N/31)`, Batch sequences are `2..b+1`, and End is `b+2`, hence 3..69.
Each wire Batch is exactly one durable transaction containing those 1..31 immutable
Entry puts plus exactly one manifest-root/progress put: at most 32 operations
and 262,144 key/value bytes before framing. The progress value embeds content
manifest digest, transfer sequence, prior acknowledgement, cursor, and transfer
chain, so the transaction adds no generic checkpoint-dedupe or resume Put. The
Controller durably commits it before issuing the cumulative ack and never
combines or splits a wire Batch. Old/new never mix. A 2,048-entry manifest takes
at most `ceil(2048/31)=67` manifest-entry durable commits.

The root/progress value pins role, point/restore ids, count, full-tree digest,
next ordinal, completion, and prior root revision. Each transaction compares
that revision and new entry-key absence. Completed entries never rewrite.

Before capture, exact tar bytes plus 67108864 bytes must fit Agent staging.
Before restore construction, total regular-file bytes plus 67108864 bytes and
enough inodes must be available on the target filesystem. Overflow, quota,
allocation, inode, or query failure rejects before output/mutation. Headroom is
not payload and remains outside planned writes.

### 8. Publish Volume by one exchange

The Controller pins mounting Services, revisions, and exact prior runtime
intents and orders host effects in Service-id byte order. The Agent alone
executes stop/inspect operations on the host and returns checkpointed proof for
Controller acknowledgement. Only prior-running intent is later restarted;
desired intents do not change. The helper creates root-owned mode-`0700` hidden sibling
`.groundplane-restore-<restore-generation-id>` under the authorized parent.
All operations use one pre-opened parent fd, the complete resolution mask, and
relative `openat2`/`*at`; device/mount/topology is revalidated before each
batch/exchange. Absolute reopening, `/proc/self/fd`, fallback walking, copy,
move-aside, and delete-then-rename are forbidden.

Construction follows archive order. Directories remain temporary `0:0/0700`.
Files are created `0:0/0600`, receive/fsync exact content, then final `fchown`,
`fchmod`, file fsync, and parent fsync. Directories finalize afterward in
descending unsigned path order, descendants before ancestors and `.` last,
with final `fchown`, `fchmod`, and directory fsync.

Before each construction mutation, Controller acknowledges this exact durable
intent:

```text
point_id; restore_generation_id; new_tree_content_manifest_sha256
construction_cursor_before; archive_ordinal (zero-based); logical_path bytes
kind; mode; uid; gid; size; content_sha256
```

Directory size/digest are zero. Completion clears this intent and advances
only after required fsyncs. Recovery accepts pending file as absent,
`0:0/0600` with expected content prefix, final uid/gid with `0600` and complete
content, or exact final metadata/content. Temporary state is removed and
replayed; complete states replay remaining metadata/fsync. Pending directory is
absent or exact temporary `0:0/0700` without later descendants. Completed
prefix and future non-pending state must match; anything else is ambiguity.

Before each directory mutation, Controller acknowledges:

```text
point_id; restore_generation_id; new_tree_content_manifest_sha256
finalization_cursor_before; finalization_ordinal (zero-based)
logical_path bytes; final_uid; final_gid; final_mode
```

Completion advances only after fsync. Pending state may be temporary
`0:0/0700`, final uid/gid with `0700`, or exact final metadata; recovery
replays the fixed metadata/fsync sequence. Other state is ambiguity.

After finalization the helper fsyncs parent and verifies new full-tree digest.
A durable exchange intent retains old/new full-tree digests. The helper proves
same filesystem, calls `renameat2(RENAME_EXCHANGE)` once, fsyncs parent, and
verifies live-new. Resume accepts only live-old/hidden-new (exchange once) or
live-new/hidden-old (lost completion, never exchange again). No fallback exists.

The old hidden tree is deleted in descending descendant-path bytes with root
`.` explicitly last. Let this be `d[0..n-1]`, `d[n-1]=.`. Before each removal,
Controller acknowledges:

```text
point_id; restore_generation_id
old_full_tree_sha256; new_full_tree_sha256
deletion_cursor_before=k; deletion_ordinal=k+1 (one-based)
logical_path=d[k] bytes; kind; parent_logical_path bytes (empty for root)
```

The full-tree digests stay constant; no remaining-tree digest exists. Without
pending, exactly `d[k..n-1]` survive. With pending, only `d[k]` may be present
or absent. Present is verified, removed, and parent-fsynced. Absent means only
this exact mutation may have completed, so the same parent is fsynced and the
same completion replayed. Any gap, completed survivor, changed kind/parent,
non-empty directory, changed live tree, or extra path is ambiguity.

Non-final completion clears pending and increments `k`. Root completion clears
pending, records authorized-parent fsync, and transitions directly to consumer
restart. Durable `k=n` is forbidden. There is no aggregate replaced-tree-
cleaned checkpoint, completion, or transition; final per-path deletion is the
sole cleanup boundary. Prior-running consumers then restart in Service-id byte
order through health gates; other prior intents remain unchanged.

### 9. Make `postgres-custom-v1` a controlled PostgreSQL 16 contract

The source is exact opaque stdout of PostgreSQL 16 `pg_dump` custom format,
compression disabled. Every PostgreSQL child runs only inside the existing
running Groundplane-managed PostgreSQL workload container. Before every exec,
the Agent validates the pinned `postgres16` adapter contract, Attach and Service
revisions, Compose artifact and render generation, Service key, immutable
container id, required Groundplane labels, and exact managed OCI release
authority through Moby.

No project, Environment, Blueprint, operator setting, Controller configuration,
or Agent configuration may select or override the PostgreSQL repository, tag,
digest, helper binary, platform, or adapter contract version.

#### Managed PostgreSQL workload image

The PostgreSQL workload image is one first-party derived OCI index for exactly
the two supported host platforms:

```text
linux/amd64
linux/arm64
```

The index contains exactly one runnable child manifest per supported platform.
Each platform's authenticated upstream parent, child/config/layer descriptors,
and derived helper/config/layer digests are retained in the platform-specific
release record. Source documentation never predicts a post-push managed-index,
child, config, or layer digest. The upstream runnable parents are the
platform-specific children of the PostgreSQL `16.15-alpine3.24` OCI index; the
authenticated ARM64 descriptor decides whether its variant is `v8` or `null`.

Its Docker Library source is exactly:

```text
repository:       https://github.com/docker-library/postgres.git
commit:           9d15534160ade17f2b6c455a39ee967c49b1937d
directory:        16/alpine3.24
PostgreSQL:       16.15
source SHA-256:   c1575341fa7bd40f5274ea465b34390f4dc64cdd0770af327005caaeb9f6b7ed
```

The derived image adds only the fixed helper, its private fixed client gate, its
state directory, and the two Groundplane contract labels defined below. It MUST NOT set or replace
`ENTRYPOINT`, `CMD`, `USER`, `WORKDIR`, `ENV`, `STOPSIGNAL`, exposed ports, or
volumes.

The derived image therefore retains and release verification requires:

```text
Entrypoint:  ["docker-entrypoint.sh"]
Cmd:         ["postgres"]
User:        unset
WorkingDir:  /
StopSignal:  SIGINT
PG_MAJOR:    16
PG_VERSION:  16.15
PGDATA:      /var/lib/postgresql/data
```

The complete OCI Config.Env array is retained byte-for-byte and in order from
the fixed upstream config digest. The derived Dockerfile declares no `ENV`.
Release verification compares the derived array to the upstream array exactly,
not as a map or selected subset.

Because OCI User is unset, normal container startup begins as uid 0. The
inherited entrypoint performs its existing `gosu postgres` privilege drop before
starting PostgreSQL. This startup behavior is distinct from every helper Docker
Exec, which selects exact numeric user and group `0:0`. The helper remains the
root supervisor; only its PostgreSQL client child is launched as exact numeric
user and group `70:70` through the private release-pinned gate.

The image contains:

```text
/usr/local/libexec
  uid:   0
  gid:   0
  mode:  0755

/usr/local/libexec/groundplane-postgres16-helper
  uid:   0
  gid:   0
  mode:  0555
  os:    linux
  arch:  amd64 or arm64, matching the selected host platform
  ELF:   ELF64, little-endian, x86-64 or AArch64, matching that platform
  link:  static

/usr/local/libexec/groundplane-postgres16-client-gate
  uid:   0
  gid:   0
  mode:  0500
  os:    linux
  arch:  amd64 or arm64, matching the selected host platform
  ELF:   ELF64, little-endian, x86-64 or AArch64, matching that platform
  link:  static

/run/groundplane-postgres16
  uid:   0
  gid:   0
  mode:  0700
```

The amd64 and arm64 helper and gate builds have the same semantics, protocol,
and security behavior; only ELF machine, syscall constants, and
platform-specific release digests vary.

Helper state files within `/run/groundplane-postgres16` are `root:root`, mode
`0600`, regular, single-link files. PostgreSQL server and client processes at
numeric `70:70` cannot traverse the directory or read, write, link, unlink,
rename, replace, chmod, or chown its files.

The helper, private client gate, and state directory are image contents of the already managed
PostgreSQL workload container. They are not present in the Groundplane Agent
image, are not copied or mounted into a running workload, and are not supplied
by a helper image or helper container. No helper-specific bind mount, volume,
tmpfs, Docker socket, network, TCP fallback, or Docker copy is added.

The client gate is not a public helper and has no Docker Exec operation. The
root helper is its only caller. It accepts only the closed inherited control
descriptors and fixed launch-intent encoding defined below; it has no generic
command, argv, environment, credential, file, SQL, or diagnostic interface.

#### Immutable managed-image release authority

The managed PostgreSQL image uses one cycle-free two-level release authority.

For both levels, `JCS(x)` is the RFC 8785 canonical JSON encoding of `x`, and:

```text
D(domain, bytes) = SHA-256(ASCII(domain) || 0x00 || bytes)
```

The exact level-one digest domain is:

```text
groundplane.postgres16.managed-image-build-contract.v1
```

The exact level-two digest domain is:

```text
groundplane.postgres16.managed-image-release-record.v1
```

##### Level one: pre-push build contract

The helper and private client-gate binaries are built first. Their exact byte
lengths and SHA-256 digests are then available before the managed OCI image is
built.

The level-one object has exactly this closed schema:

```text
ManagedPostgres16BuildContract = {
  "adapter_contract_version": 1,
  "adapter_key": "postgres:16",
  "container_security": {
    "cap_add": [
      "CHOWN",
      "DAC_OVERRIDE",
      "FOWNER",
      "KILL",
      "SETGID",
      "SETPCAP",
      "SETUID",
      "SYS_PTRACE"
    ],
    "cap_drop": ["ALL"],
    "no_new_privileges": true,
    "postmaster_cap_ambient": 0,
    "postmaster_cap_bounding": [
      "CHOWN",
      "DAC_OVERRIDE",
      "FOWNER",
      "KILL",
      "SETGID",
      "SETPCAP",
      "SETUID",
      "SYS_PTRACE"
    ],
    "postmaster_cap_effective": 0,
    "postmaster_cap_inheritable": 0,
    "postmaster_cap_permitted": 0,
    "postmaster_egid": 70,
    "postmaster_euid": 70,
    "postmaster_fsgid": 70,
    "postmaster_fsuid": 70,
    "postmaster_groups": [],
    "postmaster_gid": 70,
    "postmaster_no_new_privs": true,
    "postmaster_saved_gid": 70,
    "postmaster_saved_uid": 70,
    "postmaster_uid": 70,
    "security_opt": ["no-new-privileges:true"]
  },
  "client_gate": {
    "elf_class": "ELF64",
    "elf_data": "little-endian",
    "elf_machine": {
      "linux/amd64": "EM_X86_64",
      "linux/arm64": "EM_AARCH64"
    },
    "gid": 0,
    "linkage": "static",
    "mode": 320,
    "os": "linux",
    "path": "/usr/local/libexec/groundplane-postgres16-client-gate",
    "sha256": HEX64,
    "size_bytes": UINT64_POSITIVE,
    "uid": 0
  },
  "helper": {
    "directory_gid": 0,
    "directory_mode": 493,
    "directory_path": "/usr/local/libexec",
    "directory_uid": 0,
    "elf_class": "ELF64",
    "elf_data": "little-endian",
    "elf_machine": {
      "linux/amd64": "EM_X86_64",
      "linux/arm64": "EM_AARCH64"
    },
    "gid": 0,
    "linkage": "static",
    "mode": 365,
    "os": "linux",
    "path": "/usr/local/libexec/groundplane-postgres16-helper",
    "sha256": HEX64,
    "size_bytes": UINT64_POSITIVE,
    "uid": 0
  },
  "image_labels": {
    "adapter_contract_version_key":
      "com.groundplane.postgres16.adapter-contract-version",
    "contract_sha256_key":
      "com.groundplane.postgres16.contract-sha256"
  },
  "launch_profile_sha256": HEX64,
  "platforms": [
    {"os": "linux", "architecture": "amd64", "variant": null},
    {"os": "linux", "architecture": "arm64", "variant": "v8" | null}
  ],
  "process": {
    "child_cap_ambient": 0,
    "child_cap_bounding": 0,
    "child_cap_effective": 0,
    "child_cap_inheritable": 0,
    "child_cap_permitted": 0,
    "child_egid": 70,
    "child_euid": 70,
    "child_fsgid": 70,
    "child_fsuid": 70,
    "child_gid": 70,
    "child_nofile_limit": 64,
    "child_no_new_privs": true,
    "child_saved_gid": 70,
    "child_saved_uid": 70,
    "child_seccomp_mode": 2,
    "child_supplementary_groups": [],
    "child_uid": 70,
    "client_paths": {
      "pg_dump": "/usr/local/bin/pg_dump",
      "pg_restore": "/usr/local/bin/pg_restore",
      "psql": "/usr/local/bin/psql"
    },
    "environment": [
      "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "HOME=/nonexistent",
      "LC_ALL=C",
      "TZ=UTC"
    ],
    "fd_profile_version": 1,
    "gate_protocol_version": 1,
    "gate_seccomp_sha256": HEX64,
    "gate_single_thread_profile": {
      "linux/amd64": "linux-amd64-freestanding-single-thread-v1",
      "linux/arm64": "linux-arm64-freestanding-single-thread-v1"
    },
    "helper_exec_gid": 0,
    "helper_exec_uid": 0,
    "helper_egid": 0,
    "helper_euid": 0,
    "helper_fsgid": 0,
    "helper_fsuid": 0,
    "helper_gid": 0,
    "helper_saved_gid": 0,
    "helper_saved_uid": 0,
    "helper_supplementary_groups": [],
    "helper_uid": 0,
    "launch_profile": "groundplane.postgres16.client-launch.v1",
    "state_schema": 1
  },
  "probes": {
    "helper_environment": [
      "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
      "HOME=/nonexistent",
      "LC_ALL=C",
      "TZ=UTC"
    ],
    "helper_protocol_version": 1,
    "pg_dump_major": 16,
    "pg_restore_major": 16,
    "psql_major": 16,
    "server_major": 16
  },
  "runtime": {
    "cmd": ["postgres"],
    "entrypoint": ["docker-entrypoint.sh"],
    "environment": UPSTREAM_CONFIG_ENV,
    "pg_major": "16",
    "pg_version": "16.15",
    "pgdata": "/var/lib/postgresql/data",
    "startup_uid": 0,
    "stop_signal": "SIGINT",
    "user": null,
    "working_dir": "/"
  },
  "schema": 1,
  "state": {
    "file_mode": 384,
    "gid": 0,
    "mode": 448,
    "mount_kind": "none",
    "path": "/run/groundplane-postgres16",
    "storage": "container-writable-layer",
    "uid": 0
  },
  "upstream": {
    "config_digest":
      "sha256:c05eced0bdb41ea9b95a656472a6aa4d50cad0d8a2e33d14eb1c53fd6204f2ae",
    "index_digest":
      "sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685",
    "release": "16.15-alpine3.24",
    "repository": "docker.io/library/postgres",
    "runnable_child_digest":
      "sha256:738d1359df5aa0b6d50a9071e989c49fdd39152a2a805c6ff131bf5e2243e0b3",
    "source_commit":
      "9d15534160ade17f2b6c455a39ee967c49b1937d",
    "source_directory": "16/alpine3.24",
    "source_postgresql_sha256":
      "c1575341fa7bd40f5274ea465b34390f4dc64cdd0770af327005caaeb9f6b7ed"
  }
}
```

`HEX64` is exactly 64 lowercase hexadecimal bytes. `UINT64_POSITIVE` is a
canonical positive JSON integer within unsigned 64-bit range.
`UPSTREAM_CONFIG_ENV` is the complete ordered OCI Config.Env string array read
from the fixed upstream config digest above. It is a derived value from fixed
authenticated source bytes, not a release placeholder or operator input.

`launch_profile_sha256` is exactly 64 lowercase hexadecimal bytes and is
computed before the build-contract digest as:

```text
D("groundplane.postgres16.client-launch-profile.v1",
  JCS(ManagedPostgres16BuildContract["process"]))
```

The helper and client gate embed that exact digest. Release verification
requires both to report it and independently reconstructs it from the closed
`process` object. It contains no gate/helper byte digest or managed image digest
and therefore creates no digest cycle.

`gate_seccomp_sha256` is raw SHA-256 over the exact loaded classic-BPF
`sock_filter` array, each instruction serialized as little-endian `u16 code`,
`u8 jt`, `u8 jf`, and little-endian `u32 k` with no header or padding. Release
automation reconstructs each platform's array from the locked syscall constants
for `linux/amd64` and `linux/arm64`, checks the filter semantics and digest, and
verifies that libc `fork`/`vfork` and raw `clone`/`clone3` each fail with `EPERM`
while the selected PostgreSQL client can exec and operate. The digest is part of
the `process` object and therefore of `launch_profile_sha256` and the level-one
contract.

The level-one digest is exactly:

```text
contract_sha256 =
  D("groundplane.postgres16.managed-image-build-contract.v1",
    JCS(ManagedPostgres16BuildContract))
```

`contract_sha256` is represented outside the level-one object as exactly 64
lowercase hexadecimal bytes. The level-one hash preimage contains no
`contract_sha256`, managed repository, managed index digest, or managed child
digest, so no image-digest cycle exists.

The derived image contains exactly these added labels:

```text
com.groundplane.postgres16.adapter-contract-version=1
com.groundplane.postgres16.contract-sha256=<contract_sha256>
```

The label value `<contract_sha256>` is the already-computed level-one output. It
is not part of its own preimage.

##### Level two: post-push release record

After the derived image is pushed as one OCI index, release automation supplies
the first-party repository and obtains the registry-reported managed index plus
exactly one runnable child, config, and added-layer digest set for each of
`linux/amd64` and `linux/arm64`. These values are release inputs or outputs,
never guessed source constants and never operator configuration.

The level-two object has exactly this closed schema:

```text
ManagedPostgres16ReleaseRecord = {
  "contract_sha256": HEX64,
  "managed_index_digest": OCI_SHA256,
  "managed_repository": OCI_REPOSITORY,
  "platforms": [
    {
      "os": "linux",
      "architecture": "amd64",
      "variant": null,
      "child_digest": OCI_SHA256,
      "config_digest": OCI_SHA256,
      "added_layer_digests": [OCI_SHA256]
    },
    {
      "os": "linux",
      "architecture": "arm64",
      "variant": "v8" | null,
      "child_digest": OCI_SHA256,
      "config_digest": OCI_SHA256,
      "added_layer_digests": [OCI_SHA256]
    }
  ],
  "schema": 1
}
```

`OCI_SHA256` is exactly `sha256:` followed by 64 lowercase hexadecimal bytes.
`OCI_REPOSITORY` is one canonical lowercase OCI repository without a tag,
digest, whitespace, URL scheme, query, or fragment.

The release-populated fields are:

```text
managed_repository:
  RELEASE INPUT supplied by first-party release automation

managed_index_digest:
  RELEASE OUTPUT reported by the registry after push

platforms:
  RELEASE OUTPUT obtained by resolving the pushed managed index: exactly one
  runnable child, config, and added-layer digest set for each of
  `linux/amd64` and `linux/arm64`
```

No fixed first-party repository or managed post-push digest is asserted by this
ADR.

The level-two digest is exactly:

```text
managed_release_sha256 =
  D("groundplane.postgres16.managed-image-release-record.v1",
    JCS(ManagedPostgres16ReleaseRecord))
```

`managed_release_sha256` is exactly 64 lowercase hexadecimal bytes in release
metadata. It is not a field of `ManagedPostgres16ReleaseRecord` and therefore
creates no digest cycle. Controller and Agent release artifacts embed the same
value. Agent authentication carries its decoded 32 bytes in the sole
`postgres16_managed_release_sha256` field at Authenticate tag 5, as defined by
ADR 0048.

Release verification independently fetches the managed index and requires:

- exactly one runnable child for each of `linux/amd64` and `linux/arm64`;
- each child digest equals its platform-specific release-record digest;
- each child config and added-layer set equals its platform-specific
  release-record digests;
- each child config retains the level-one runtime values exactly;
- managed container HostConfig has `Privileged=false`, exact `CapDrop=[ALL]`,
  exact ordered
  `CapAdd=[CHOWN,DAC_OVERRIDE,FOWNER,KILL,SETGID,SETPCAP,SETUID,SYS_PTRACE]`, and
  exact `SecurityOpt=[no-new-privileges:true]`, with no added capability or
  alternate security option;
- the complete Config.Env array equals the fixed upstream Config.Env array
  byte-for-byte and in order;
- the two Groundplane labels have the exact keys and values above;
- `/usr/local/libexec` has the required type, uid, gid, and mode;
- the helper has the required path, type, uid, gid, mode, byte length, SHA-256,
  static linkage, and ELF identity;
- the private client gate has the required path, type, uid, gid, mode, byte
  length, SHA-256, static linkage, ELF identity, and no setuid bit, setgid bit,
  file capabilities, or writable path component;
- gate READY proves one task, `pid==pgid`, all four uid/gid values zero, empty
  groups, zero inheritable/ambient sets, exact eight-capability permitted/
  effective/bounding sets, `NoNewPrivs: 1`,
  exact parent identity, and exact fd 0..4 set before release;
- `/run/groundplane-postgres16` has the required type, uid, gid, and mode and is
  not covered by an OCI volume, bind mount, or tmpfs;
- a helper probe entered by exact Docker Exec user/group `0:0` proves the helper
  real/effective/saved/filesystem uid and gid are zero, its state is `root:root`
  `0600`, it has no supplementary groups, and its gated child has
  real/effective/saved/filesystem uid and gid
  `70:70`, no supplementary groups, zero inheritable/permitted/effective/ambient
  capabilities, and `NoNewPrivs: 1`;
- the three client paths are regular root-owned non-writable files without
  setuid, setgid, or file capabilities, byte-identical to the fixed upstream
  runnable child, and gate READY plus PROFILE_APPLIED evidence matches the
  closed fd and security profiles;
- the initialized live postmaster has real/effective/saved/filesystem uid and
  gid all 70, no supplementary groups, zero inheritable/permitted/effective/
  ambient capability sets, the exact eight-capability container bounding set,
  and `NoNewPrivs: 1`;
- every executable reachable by uid 70 in the final rootfs lacks setuid, setgid
  and `security.capability`, every path component is non-writable by uid 70, and
  postmaster cannot reacquire the bounded startup capabilities through uid/gid
  change or exec;
- `pg_dump`, `pg_restore`, and `psql` report major 16 under the helper's exact
  environment; and
- a temporary initialized PostgreSQL instance passes the exact helper probes
  and server-major proof.

Only after all verification succeeds may release automation publish the
level-two record.

Both Controller and Agent release artifacts embed the byte-identical level-one
object, `contract_sha256`, level-two object, and `managed_release_sha256`.
The Controller must not assign PostgreSQL Backup work unless the authenticated
Agent supplied the exact decoded 32-byte `managed_release_sha256`. The same
release record controls the image, helper, recovery, and exit table. There is
no generic adapter ledger, second PostgreSQL digest, or alternate equality
path.

#### Managed image projection and sealing

The `postgres16` adapter resolves its workload image solely from the embedded
level-two release record. Its exact public image projection is:

```text
managed_repository + "@" + managed_index_digest
```

The adapter exposes no image parameter, default tag, repository option, digest
option, platform option, helper option, or contract-version option.

Canonical Compose contains exactly that OCI digest reference. Durable Service
desired state, the immutable Compose artifact, the required-label digest, and
Backup task authority seal that same identity.

For the managed PostgreSQL Service:

```text
BackupServiceFact.repository_digest =
  raw 32 bytes decoded from managed_index_digest
```

Before every helper exec, the Agent uses Moby to require:

- the exact immutable Service and container identity;
- the exact canonical managed repository and index digest;
- exactly one expected native release child for each of `linux/amd64` and
  `linux/arm64`;
- the exact level-one `contract_sha256` image label;
- adapter contract version `1`;
- the exact required Groundplane container-label count and digest;
- the sealed Service and Compose revisions; and
- the current immutable container id.

No mutable tag participates in desired state, a plan, an observation, recovery,
or acceptance evidence.

Every Docker Exec `Cmd` has exact prefix:

```text
/usr/bin/env
-i
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
HOME=/nonexistent
LC_ALL=C
TZ=UTC
/usr/local/libexec/groundplane-postgres16-helper
```

The suffix begins `run,1,<nonce>,<hard-deadline-unix-nano>`; nonce is 64
lowercase hex characters for 32 random bytes and integers are canonical
unsigned decimal. Closed remaining suffixes are:

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

#### Shared caller/helper protocol ownership

`internal/common/postgres16protocol` is the sole shared owner of the helper
machine vocabulary:

- protocol version;
- fixed helper path;
- fixed absolute PostgreSQL client paths;
- public operation names;
- exact Docker Exec prefix and suffix construction;
- exact helper `0:0` and child security profile;
- the version-1 gate intent, release, fd-profile, and durable-state vocabularies;
- nonce and deadline encoding;
- database, role, size, and SHA-256 argument encoding;
- stdin, stdout, and stderr policies; and
- stable numeric helper exit-code meanings.

The shared leaf declares explicit numeric constants, never `iota`, for this
complete normal helper-exit table:

| Exit | Name | Exact meaning |
| --- | --- | --- |
| 0 | `success` | For `run`, the selected child operation's exact semantic and stream proof completed, the fixed PostgreSQL child exited zero and was reaped once by its original parent, and helper state was durably removed. For `stop`, the requested termination or retirement was exactly proven and state was durably removed through one of two branches: original-parent normal stop or already-reaped cleanup retains the exact terminal child status, uses the sole Wait4 when applicable, and durably records `phase=reaped`; non-parent or boot-change recovery durably records `recovery_retired` with exact reason `unavailable_not_parent` or `unavailable_boot_changed` and never claims Wait4 or `reaped`. Stop success never requires child exit zero and makes no workload-success claim. |
| 1 | `request_invalid` | Public argv was malformed, extra, missing, reordered, noncanonical, wrong-versioned, or carried an invalid nonce, deadline, operation, database, role, size, or SHA-256 value. |
| 2 | `environment_invalid` | After caller identity passed, the helper executable, exact four-entry environment, `/` working directory, or operation-selected fd/stream envelope was not the closed contract, before any ambiguous release or execution. |
| 3 | `recovery_required` | Durable helper state, process identity, release consumption, client admission, terminal status, wait/reap, state retirement, or mutation outcome is absent, ambiguous, inconsistent, or cannot be durably retired; this also covers every non-successful `restore-apply` result after its apply-start acknowledgement or durable `gate_released`. |
| 4 | `launch_failed` | The fixed gate/client launch or admission failed in a completely classified way, all created lifetimes and state were durably retired, no PostgreSQL client execution is possible, and the operation's retry authority was not spent. |
| 5 | `deadline_exceeded` | A valid absolute deadline expired, TERM/KILL and the sole reap completed, state was durably retired, and neither execution ambiguity nor the spent `restore-apply` rule requires `recovery_required`. |
| 6 | `child_failed` | The admitted fixed PostgreSQL child completed a normal nonzero exit after the expected streams terminated, the original parent reaped it once, and state was durably retired; a spent `restore-apply` instead returns `recovery_required`. |
| 7 | `caller_identity_invalid` | Before parsing any operation or touching state, one or more real/effective/saved/filesystem uid or gid values was not zero, or supplementary groups were nonempty. |
| 8 | `io_failed` | A helper-owned stdin/stdout/stderr, gate-status, release-pipe, or artifact-forwarding read, write, EOF, or close failed while time remained, with complete retirement and no more-specific integrity, limit, proof, or recovery classification. |
| 9 | `input_integrity_failed` | Repeat-safe `restore-list` input was early, extra, short, wrong-size, wrong-SHA-256, or lacked the required extra EOF read, and the child/state were completely retired; the same condition after the `restore-apply` destructive boundary returns `recovery_required`. |
| 10 | `stream_limit_exceeded` | A helper-enforced stdout/stderr byte ceiling or checked unsigned-64 drain/count bound was exceeded, with complete retirement; a spent `restore-apply` returns `recovery_required`. |
| 11 | `proof_mismatch` | A complete bounded, zero-exit child result failed the operation's exact client-major, server-major, terminate, zero-connection, or database-name proof. |
| 12 | `internal_failure` | A helper implementation invariant or owned resource failure not assigned above occurred before ambiguity, and every created lifetime and durable state was completely retired. |

Normal helper exits outside `0..12` are forbidden. In particular, `126` and
`127` are never helper meanings, and `128..255` are never used to encode a
signal. A signal death or other non-normal Docker Exec termination is not
remapped by the helper. The Agent treats any forbidden normal exit, signal-like
exit, or missing terminal proof as a helper-contract violation and
`recovery_required`; it never classifies such a value by subtracting 128.

Entry classification order is caller identity, environment, then request.
Malformed or noncanonical deadline syntax is `request_invalid`; a canonical
deadline that is not future at entry or expires during work is
`deadline_exceeded` only when complete retirement is proven. After state or a
child can exist, `recovery_required` overrides deadline and every stage-specific
code whenever identity, release, execution, terminal, wait/reap, retirement, or
spent-mutation authority is uncertain. Otherwise deadline overrides a
simultaneous stage I/O failure. A complete gate FATAL is `launch_failed` only
when its sequence/stage/errno/EOF and terminal state are consistent, complete
retirement is proven, and retry authority remains; an incomplete or
inconsistent gate-status stream is `recovery_required`.

Codes `0..6` retain the compatible meanings of the superseded scaffold,
narrowed by the accepted gate, stream, and retirement proof. The new revision's
old `7=unsupported` and `8=internal_bootstrap` meanings do not survive: this
contract has no unsupported public mode, and the private client gate reports
only its closed READY/PROFILE_APPLIED/FATAL status frames. Exact
`managed_release_sha256` equality selects one release and one exit table for
each execution. No old helper remains executable. Activation fails closed if
legacy state exists, and no execution accepts a compatibility decoder or dual
table.

It is a side-effect-free shared leaf and owns no Docker client, process launch,
filesystem state, or PostgreSQL execution.

`cmd/postgres16-helper` remains the only public Docker Exec helper.
`cmd/postgres16-client-gate` is the private non-public launcher described above;
it is reachable only from the helper's closed raw-process boundary.
`internal/postgres16helper` remains the sole helper state machine and privileged
supervisor and owns one closed helper-side client builder. The builder maps one
validated public operation to exactly one fixed client path, argv, replacement
environment, three-fd policy, and security profile; it accepts no caller-chosen
executable, raw argv, environment, credentials, descriptors, or SQL.
Before parsing an operation or touching state, the public helper requires all
real/effective/saved/filesystem uid and gid values zero and no supplementary
groups. A direct uid-70 invocation fails with the stable caller-identity exit
code and cannot invoke the gate.

The narrow raw `os/exec` and Linux process-control exception for
`internal/postgres16helper` already recorded in `docs/standards.md` remains the
sole process boundary for this helper. It launches and supervises only the
private fixed client gate and the resulting uid-70 PostgreSQL client. It is not
broadened to any other package or operation. The generic shared Runner is
unchanged and is not used to implement this privileged gate. The Moby caller
lives under `internal/infra/docker` and consumes the shared protocol leaf. The
Agent orchestration runtime consumes that infrastructure interface.

No caller duplicates a helper path, operation token, environment member,
argument order, exit code, or stream policy. No caller imports
`internal/postgres16helper`.

#### Exact Docker Exec contract

The only second public operation is:

```text
stop,1,<64-lowercase-hex-nonce>,<canonical-cleanup-deadline-unix-nano>
```

There is no collect, shell, generic executable, generic argument, generic SQL,
filename, password, ambient-environment, or diagnostic mode.

The semantic execution policy is closed: the helper command is the fixed prefix
plus closed suffix; it runs as exact numeric `0:0` in `/`, receives no
Moby-injected environment, receives no added privilege or TTY, and has separate
attached stdout and stderr semantics. Capture and every proof operation have no stdin.
Only `restore-list` and `restore-apply` receive attached stdin; each streams the
retained host source from offset zero and closes the write side only after exact
EOF.

ADR 0048 is the sole owner of the exact Moby ExecCreate and attached-start
request fields and values, including `DetachKeys`, `ConsoleSize`, every attach,
stdin, stdout, stderr, privilege, TTY, user, environment, and working-directory
field, the one permitted `ContainerExecAttach` / Docker `POST /exec/{id}/start`
call for an acknowledged exec, and Docker raw-stream demultiplexing through
`stdcopy.StdCopy`. ADR 0048 also owns descriptor closure and the exact
attach/start ordering around the acknowledged dump-start and apply-start
barriers. This ADR defines no Moby request struct, default, second start path,
or raw framing parser. There is no duplicate Moby mechanics authority in ADR
0047; its ownership is limited to the closed helper command, PostgreSQL
semantics, and the semantic stream policies below.

The `/usr/bin/env -i` prefix clears the inherited container environment before
the helper starts. The helper receives exactly PATH, HOME, LC_ALL, and TZ in
that order. For every PostgreSQL child, the closed helper-side builder replaces
rather than extends the environment with those same four entries in that order,
uses the operation's fixed absolute executable path, and passes the closed argv
directly without a shell or PATH lookup. No credential, password, secret, or
ambient Agent or container environment reaches the child.

Dump stdout is the sole artifact stream. Dump stderr is drained and discarded
with a 32-KiB ceiling. Probe and proof modes validate their closed outputs and
emit no result payload.
Each `probe-pg-dump`, `probe-pg-restore`, and `probe-psql` invocation has an
independent hard limit of exactly 32,768 bytes for stdout and exactly 32,768
bytes for stderr; neither stream's unused allowance transfers to the other.
Restore-list stdout is fully drained and discarded in
O(1) retained memory with checked unsigned-64 counting and no size ceiling; its
stderr alone has a 32-KiB ceiling. Restore-apply stdout and stderr each have an
independent 32-KiB ceiling.

No discarded diagnostic byte is logged, persisted, returned in Task evidence,
or reused as protocol input. Every mode has a fresh nonce and re-attestation;
the helper owns its absolute deadline, TERM/KILL sequence, wait, and reap.

Helper maps modes one-to-one to these fixed child argv/SQL:

```text
/usr/local/bin/pg_dump --version
/usr/local/bin/pg_restore --version
/usr/local/bin/psql --version

/usr/local/bin/pg_dump --format=custom --compress=0 --no-owner --no-acl
  --host=/var/run/postgresql --username=postgres --no-password
  --role=<role> --dbname=<database>

/usr/local/bin/pg_restore --list --no-password

/usr/local/bin/psql --no-psqlrc --quiet --tuples-only --no-align --set=ON_ERROR_STOP=1
  --host=/var/run/postgresql --username=postgres --no-password
  --dbname=<database>
  --command=SELECT pg_catalog.current_setting('server_version_num')::integer / 10000;

/usr/local/bin/psql --no-psqlrc --quiet --tuples-only --no-align --set=ON_ERROR_STOP=1
  --host=/var/run/postgresql --username=postgres --no-password
  --dbname=<database>
  --command=SELECT pg_catalog.coalesce(pg_catalog.bool_and(pg_catalog.pg_terminate_backend(a.pid)), true) FROM pg_catalog.pg_stat_activity AS a WHERE a.datname = pg_catalog.current_database() AND a.pid <> pg_catalog.pg_backend_pid();

/usr/local/bin/psql --no-psqlrc --quiet --tuples-only --no-align --set=ON_ERROR_STOP=1
  --host=/var/run/postgresql --username=postgres --no-password
  --dbname=<database>
  --command=SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity AS a WHERE a.datname = pg_catalog.current_database() AND a.pid <> pg_catalog.pg_backend_pid();

/usr/local/bin/pg_restore --clean --if-exists --no-owner --no-acl --exit-on-error
  --single-transaction --host=/var/run/postgresql --username=postgres
  --no-password --role=<role> --dbname=<database>

/usr/local/bin/psql --no-psqlrc --quiet --tuples-only --no-align --set=ON_ERROR_STOP=1
  --host=/var/run/postgresql --username=postgres --no-password
  --dbname=<database> --command=SELECT pg_catalog.current_database();
```

Helper validates probes/client major 16, server `16\n`, terminate `t\n`, zero
connections `0\n`, and post-restore `<database>\n`, and emits no result payload.
The `server-major` mode binds its sealed `<database>` suffix argument to the
fixed child's `--dbname=<database>` argv element; it never infers a default.
Every connection command has `--no-password`. Shells, ambient env, `.pgpass`,
dynamic SQL, filenames, and passwords are forbidden.

#### Gated client creation and live exec confirmation

The helper-side builder produces one immutable version-1 launch intent before
any process exists. It contains the state schema and launch/fd-profile versions;
nonce, operation and absolute deadline; supervisor pid/start ticks/boot id and
numeric identity; fixed gate path/digest; fixed client path and verified image
file identity; complete argv and ordered replacement environment; exact
stdin/stdout/stderr policy; child process-group policy; and every required
credential, capability, and `no_new_privs` value. The parent durably writes and
fsyncs that intent as `phase_created` before spawning. The state contains no
password, secret, ambient environment, or generic executable field.

The private gate is a release-pinned freestanding static executable for the
selected native `linux/amd64` or `linux/arm64` platform, with no language
runtime or thread library. Release and runtime probes require
exactly one `/proc/<pid>/task` entry from entry through exec. Its only argv is the closed internal form
`gate,1,<nonce>,<supervisor-pid>,<supervisor-start-ticks>,<boot-id>,<operation>,<closed-operation-arguments>`.
It independently rebuilds the same launch intent and rejects a digest mismatch.
Its inherited descriptors are exactly fd 0, 1 and 2 according to the selected
mode, fd 3 as the one-byte release pipe, and fd 4 as the bounded status pipe;
fds 3 and 4 are `CLOEXEC`. Its one exact ordered algorithm is the numbered
FATAL-stage order below; no implementation may move a stage across another.
Before acknowledging readiness it calls `setpgid(0,0)` and requires its
dedicated process-group id to equal its pid, lowers both soft and hard
`RLIMIT_NOFILE` to exactly 64, calls
`prctl(PR_SET_PDEATHSIG,SIGKILL)`, rechecks that current parent pid, parent start
ticks, and boot id equal the recorded supervisor identity, independently
rebuilds and matches the launch intent, and emits READY, in that order. The
parent verifies the limit from `/proc/<pid>/limits`.

The gate is the sole creator of the dedicated pgid through that one
`setpgid(0,0)` call; neither parent nor recovery calls `setpgid`. The root parent
is its sole supervisor. It opens the pidfd immediately from the positive spawn
pid and authenticates the ready gate's pid, pidfd, pgid, start ticks,
boot id, executable, argv, root identity, and intent digest, then durably writes
`phase=gate_durable`. The same attestation requires `/proc/<gate-pid>/fd` to
contain exactly 0..4: the operation-selected 0/1/2 targets and the exact release
and status pipes at 3/4, with `FD_CLOEXEC` set on 3/4 and no fd >=5. It next
durably writes `phase=gate_released` and fsyncs the
state and directory before writing the sole release byte. `gate_released` means
release authority is durable; it does not assert that the byte was consumed or
that exec occurred. The gate cannot exec a PostgreSQL client before both durable
gate states exist and the release byte is received.

After READY the gate reads exactly the release byte, then rechecks the parent
identity. While `CAP_SETPCAP` is still effective it reads `cap_last_cap`, rejects a value above 63, and applies
`PR_CAPBSET_DROP` to every supported capability from zero through that value;
it requires `CapBnd` to become zero. It then clears supplementary groups;
establishes real/effective/saved/filesystem gid `70`; establishes
real/effective/saved/filesystem uid `70`; clears inheritable, permitted,
effective and ambient capabilities; sets `PR_SET_NO_NEW_PRIVS=1`; re-arms
`PR_SET_PDEATHSIG,SIGKILL` because Linux credential changes may clear it; and
rechecks the parent identity again. It verifies the client is the release-pinned
regular root-owned path with no group/other write, setuid, setgid, or file
capabilities. Any failed syscall or mismatch exits without exec.

The gate installs the release-pinned version-1 seccomp BPF whose canonical bytes
and SHA-256 are in the build contract. On `AUDIT_ARCH_X86_64` or
`AUDIT_ARCH_AARCH64`, matching the selected native platform, it returns `EPERM`
for `fork`, `vfork`, `clone`, and `clone3` and allows all other syscalls; wrong
architecture kills the process. It calls
`close_range(3, UINT_MAX, CLOSE_RANGE_UNSHARE|CLOSE_RANGE_CLOEXEC)` and makes no
later fd-creating syscall. It verifies 0/1/2 with `fstat`/`fcntl`, verifies 3/4
exist with `FD_CLOEXEC`, and requires `fcntl(F_GETFD)` to return `EBADF` for
every fd 5 through the fixed `RLIMIT_NOFILE-1`; this creates no descriptor. It
reports the resulting canonical 0..4 fd-set digest on fd 4, then directly
`execve`s the fixed client path with closed argv/environment. The filter and
`no_new_privs` survive exec, and successful exec atomically closes fd 4.

PROFILE_APPLIED is the sole deterministic pre-exec security/fd-profile proof.
Fd-4 EOF after that frame proves only that the gate's `execve` closed the
CLOEXEC status descriptor; EOF alone is not successful client admission and
makes no statement about instructions already executed.

After emitting PROFILE_APPLIED, the attested single-threaded gate has exactly
one normal control-flow edge: fixed `execve` of the already authenticated client
path with the closed argv and environment. It has no return, fd-4 close, or
normal `_exit` edge after PROFILE_APPLIED. If `execve` fails, fd 4 is still open:
the gate emits sequence-3 FATAL stage 22 with that errno, closes fd 4, and exits
nonzero. A complete FATAL is durably recorded before terminal retirement; a
missing, partial, inconsistent, or post-FATAL status stream fails closed.

After valid PROFILE_APPLIED followed by EOF and no FATAL, the original parent
uses its already-open pidfd for one non-reaping
`waitid(P_PIDFD,pidfd,&si,WEXITED|WNOHANG|WNOWAIT)` observation. Exactly two
client-classification branches exist:

- zero `si_pid` means the lifetime is still nonterminal. The sealed gate cannot
  be alive after fd-4 EOF except as the exec'd fixed client, so the parent
  durably records `child_durable` with `exec_evidence=1`;
- `CLD_EXITED` means a normal terminal lifetime. Because the gate has no normal
  post-PROFILE_APPLIED exit and exec failure must produce FATAL, the no-FATAL
  lifetime is the fixed exec'd client even when it exited before observation.
  The parent durably records `child_durable` with `exec_evidence=2`; the sole
  later Wait4 raw status must reproduce that normal exit before it can support
  any operation result.

An alive lifetime may be inspected through its existing pidfd and matching
`/proc` entry for current executable evidence, but that observation is purely
supplemental: it is neither required nor an initial, pre-instruction, or
security-boundary attestation. The parent never SIGSTOPs the client for exec
classification. `CLD_KILLED`, `CLD_DUMPED`, any other wait result/error, a
missing or inconsistent FATAL, or any sealed-gate invariant failure is
ambiguous gate/client failure and returns `recovery_required`; a mutation apply
can never infer success or replay from it. Operation success additionally
requires its exact I/O, artifact/readback, terminal-zero, sole-Wait4, and
ExecInspect conditions. No signal targets a negative pid or pgid.

The exact child fd policy is:

| Operations | fd 0 | fd 1 | fd 2 |
| --- | --- | --- | --- |
| `probe-*`, `server-major`, `terminate-db-connections`, `assert-zero-db-connections`, `post-restore-verify` | `1`, read-only `/dev/null` | `3`, dedicated pipe to validating root parent | `5`, dedicated pipe to 32-KiB-capped root-parent drain |
| `dump` | `1`, read-only `/dev/null` | `4`, dedicated pipe copied only to helper artifact stdout | `5`, dedicated pipe to 32-KiB-capped root-parent drain |
| `restore-list` | `2`, dedicated pipe fed by root parent's exact-size/hash/EOF input validator | `6`, dedicated pipe to O(1) root-parent drain/count | `5`, dedicated pipe to 32-KiB-capped root-parent drain |
| `restore-apply` | `2`, dedicated pipe fed by root parent's exact-size/hash/EOF input validator | `5`, dedicated pipe to independent 32-KiB-capped root-parent drain | `5`, dedicated pipe to independent 32-KiB-capped root-parent drain |

The parent owns both ends needed for Docker stream policy, all byte accounting,
deadline signaling, pidfd leader control, terminal observation and the one
parent reap. Pgid is authenticated identity only: neither normal nor recovery
code passes a negative pid to `kill`, `wait`, or any signal syscall. Every TERM,
KILL, STOP and CONT uses `pidfd_send_signal` on the authenticated positive
leader pidfd.

All ordinary running/stopped/continued/terminal observations use
`waitid(P_PIDFD,pidfd,...,WNOHANG|WNOWAIT)` with only the relevant
`WSTOPPED`, `WCONTINUED`, or `WEXITED` bit and never reap. The original root
parent may reap only after a positive terminal `WEXITED|WNOWAIT` observation. It
then calls `Wait4(exact_positive_pid,&status,0,&rusage)` until one successful
return, retrying only `EINTR`; that one success is the sole reaping wait and its
raw status is durably recorded before `reaped`. `ECHILD`, a different pid, a
second successful wait, or a blocking wait without prior terminal observation
is `recovery_required`. A recovery invocation that is not the parent never
calls `Wait4`. No uid-70 client receives a journal, state-directory, gate,
pidfd, or unrelated helper descriptor.

#### Helper state-directory lifecycle

`/run/groundplane-postgres16` is part of the managed image filesystem. It is not
an OCI volume, bind mount, tmpfs, Agent mount, or host staging path. Its inode,
root uid, root gid, and mode `0700` are revalidated descriptor-relatively before
every state access. The helper never creates, chmods, chowns, replaces, or
repairs the directory.

The directory and nonce state survive Agent reconnect, Agent restart,
Controller restart, and stop/start of the same PostgreSQL container because
they remain in that container's writable filesystem.

A normal root helper supervisor creates only one exclusive nonce-named
`root:root` mode-`0600` state file, durably advances it, fsyncs it and its parent
at every required boundary, and removes it only after normal reap or
authenticated recovery retirement. The root helper parent alone owns journal,
recovery, signaling, wait, reap, retirement, and state removal. The PostgreSQL
child never opens the state directory or inherits a state descriptor.

Every state file and launch intent uses the following canonical version-1 byte
grammar. `u8`, `u32` and `u64` are unsigned big-endian integers of exactly 1, 4
and 8 bytes. `bool` is one byte and only `0` or `1`. `digest` is exactly 32 raw
SHA-256 bytes. `boot_id` is the 16 raw bytes decoded from the kernel's canonical
lowercase UUID. `blob` and UTF-8 `text` are `u32(length)||bytes`; text rejects
NUL and invalid UTF-8. `vector<T>` is `u32(count)` followed by exactly that many
canonical `T` values. Records contain fields exactly once in the listed order;
there are no tags, padding, alignment, optional-field defaults, trailing bytes,
or unknown fields.

`FileIdentityV1` fields are, in order:

```text
path:text, st_dev:u64, st_ino:u64, size_bytes:u64, sha256:digest,
uid:u32, gid:u32, mode:u32, regular:bool,
setuid_absent:bool, setgid_absent:bool, file_capabilities_absent:bool
```

`ProcessIdentityV1` fields are, in order:

```text
pid:u32, ppid:u32, pgid:u32, start_ticks:u64, boot_id:boot_id,
ruid:u32, euid:u32, suid:u32, fsuid:u32,
rgid:u32, egid:u32, sgid:u32, fsgid:u32,
groups:vector<u32>, cap_inh:u64, cap_prm:u64, cap_eff:u64,
cap_bnd:u64, cap_amb:u64, no_new_privs:bool, seccomp_mode:u8,
thread_count:u32, executable:FileIdentityV1,
argv_sha256:digest, environment_sha256:digest, fd_set_sha256:digest
```

Groups are ascending and unique. A pid/ppid/pgid is positive, start ticks are
positive, and `pgid==pid` for gate/client identity. `seccomp_mode` is `0` before
the gate filter and exactly `2` after it. File mode is `st_mode&07777`.
Executable paths are exact constants. File SHA-256 is over exactly
`size_bytes`; argv, environment and fd-set digests use the domains
`groundplane.postgres16.argv.v1`, `.environment.v1`, and `.fd-set.v1` over a
`vector<blob>` encoded by this grammar. The fd-set vector orders numeric fds and
each item is `fd:u32,flags:u32,target:text,st_dev:u64,st_ino:u64`.

`SecurityProfileV1` fields are exactly `ruid:u32,euid:u32,suid:u32,fsuid:u32,
rgid:u32,egid:u32,sgid:u32,fsgid:u32`, then `groups:vector<u32>`,
`cap_last_cap:u32`, and the five `u64` capability
sets in `inh,prm,eff,bnd,amb` order, `no_new_privs:bool`,
`seccomp_sha256:digest`, and `fork_syscalls_denied:bool`. The child profile
requires every uid/gid value 70, no groups, `cap_last_cap<=63`, all five sets
zero, both booleans true, and the release-record seccomp digest.

`FdProfileV1` is `profile_version:u32` followed by `fd0:u8,fd1:u8,fd2:u8`.
Fd values are `1=read_only_dev_null`, `2=validated_restore_input_pipe`,
`3=validated_result_pipe`, `4=artifact_output_pipe`, `5=bounded_diagnostic_pipe`,
and `6=discard_count_pipe`. Only the operation-to-fd combinations in the table
below are valid.

`ClientLaunchIntentV1` fields are, in order:

```text
magic: exact ASCII "groundplane.postgres16.client-launch.v1\x00",
schema:u32=1, nonce:32 raw bytes, operation:u8, deadline_unix_nano:u64,
supervisor:ProcessIdentityV1, gate_file:FileIdentityV1,
client_file:FileIdentityV1, argv:vector<blob>, environment:vector<blob>,
fd_profile:FdProfileV1, child_security:SecurityProfileV1,
nofile_limit:u32,
launch_profile_sha256:digest, gate_seccomp_sha256:digest
```

Operation values are `1=probe_pg_dump`, `2=probe_pg_restore`, `3=probe_psql`,
`4=server_major`, `5=dump`, `6=restore_list`,
`7=terminate_db_connections`, `8=assert_zero_db_connections`,
`9=restore_apply`, and `10=post_restore_verify`; zero and 11..255 reject. The
nonce is nonzero. Deadline is positive. Argv has 1..32 items, each 1..4,096
bytes, no NUL, and at most 16,384 aggregate bytes. Environment has exactly the
four ordered nonempty entries in the build contract, each at most 256 bytes.
`nofile_limit` is exactly 64.
The complete intent is at most 32,768 bytes. Its digest is
`D("groundplane.postgres16.client-launch-intent.v1", canonical_intent_bytes)`.

Fd 3 accepts exactly byte `0x01` followed by EOF. EOF before that byte, another
byte, an additional byte, or a read/error other than interrupt retry is fatal.
The parent writes and fsync-confirms `phase=gate_released` before writing this
byte, closes fd 3 immediately afterward, and never reopens or reuses it.

Fd 4 carries `u32(payload_length)||GateStatusV1`, with payload length 1..4,096.
`GateStatusV1` fields are
`magic:text` equal to `groundplane.postgres16.gate-status.v1`, `schema:u32=1`,
`sequence:u32`, `kind:u8`, `nonce:32 raw bytes`, `intent_sha256:digest`, followed
by the kind record. Kind 1 `READY` is sequence 1 and carries
`gate:ProcessIdentityV1`. Kind 2 `PROFILE_APPLIED` is sequence 2 and carries
`profile:SecurityProfileV1,fd_set_sha256:digest`. Kind 3 `FATAL` carries
`stage:u8,errno:u32` and is sequence 1 for every failure before READY, sequence
2 after READY and before PROFILE_APPLIED, or sequence 3 after PROFILE_APPLIED.
Fatal stage values in the sole gate algorithm order are exactly `1=setpgid`,
`2=setrlimit`, `3=initial_pdeathsig`, `4=initial_parent_identity`,
`5=intent_rebuild`, `6=ready_frame`, `7=release_read`,
`8=post_release_parent_identity`, `9=cap_last_cap`, `10=bounding_drop`,
`11=groups`, `12=gids`, `13=uids`, `14=capability_clear`,
`15=no_new_privs`, `16=pdeathsig_rearm`, `17=final_parent_identity`,
`18=client_file`, `19=seccomp`, `20=close_range_or_fd_set`,
`21=profile_frame`, and `22=execve`; zero and 23..255 reject. Stages 1..6 use
FATAL sequence 1, stages 7..21 use sequence 2, and stage 22 uses sequence 3.
If a READY or PROFILE_APPLIED write fails after emitting any partial frame, the
gate closes fd 4 and exits rather than appending FATAL to malformed bytes; the
receiver rejects the partial frame. `ready_frame` or `profile_frame` FATAL is
therefore emitted only when no byte of the corresponding frame was written.

The first frame is exactly READY sequence 1 or pre-READY FATAL sequence 1.
PROFILE_APPLIED occurs exactly once after READY/release and before exec. FATAL
is final and must be followed by EOF. EOF after READY but before
PROFILE_APPLIED is failure. EOF immediately after PROFILE_APPLIED is necessary
but not sufficient for client admission; admission additionally requires
exactly one of the two fd-4 EOF plus non-reaping waitid classification branches
above. Live `/proc` executable evidence is supplemental only. Duplicate,
skipped, oversized, truncated,
out-of-order, wrong-nonce/digest, extra-after-final, or noncanonical frames fail
closed. The gate writes no other byte to fd 4, stdout, or stderr.

Every retained status/profile digest has one exact preimage. Let
`frame_bytes=u32(payload_length)||canonical_GateStatusV1_payload`, including the
four-byte length prefix exactly as written on fd 4. Then:

```text
ready_frame_sha256 =
  D("groundplane.postgres16.gate-ready-frame.v1", frame_bytes)

profile_frame_sha256 =
  D("groundplane.postgres16.gate-profile-applied-frame.v1", frame_bytes)

fatal_frame_sha256 =
  D("groundplane.postgres16.gate-fatal-frame.v1", frame_bytes)

applied_profile_sha256 =
  D("groundplane.postgres16.gate-applied-profile.v1",
    canonical_SecurityProfileV1 || fd_set_sha256)
```

Each frame digest uses only the matching frame kind; kind/domain mismatch
rejects. `applied_profile_sha256` uses the exact SecurityProfileV1 subrecord
bytes carried in PROFILE_APPLIED followed by its 32 raw fd-set digest, with no
length prefix or outer status fields. Durable state, recovery, release probes,
and golden tests use only these formulas; hashing a payload alone, a subrecord
as a frame, or an unframed/full-frame alternative is invalid.

`HelperStateV1` fields are, in order:

```text
magic: exact ASCII "groundplane.postgres16.helper-state.v1\x00",
schema:u32=1, state_sequence:u64, phase:u8, progress_phase:u8,
preceding_state_sha256:digest, launch_intent:blob,
launch_intent_sha256:digest, supervisor:ProcessIdentityV1,
gate_ready_present:bool, [gate_ready:ProcessIdentityV1,ready_frame_sha256:digest],
gate_release_present:bool, [gate_release_authority_sha256:digest],
gate_profile_present:bool, [gate_profile:SecurityProfileV1,
  gate_profile_fd_set_sha256:digest,profile_frame_sha256:digest],
gate_fatal_present:bool, [gate_fatal_stage:u8,gate_fatal_errno:u32,
  fatal_frame_sha256:digest],
child_present:bool, [child_pid:u32,child_pgid:u32,child_start_ticks:u64,
  child_boot_id:boot_id,expected_executable:FileIdentityV1,
  expected_argv_sha256:digest,expected_environment_sha256:digest,
  applied_profile_sha256:digest,exec_fd4_eof:bool,exec_evidence:u8],
io_present:bool, [stdin_bytes:u64,stdin_sha256:digest,stdin_eof:bool,
  stdout_bytes:u64,stdout_sha256:digest,stdout_eof:bool,
  stderr_bytes:u64,stderr_sha256:digest,stderr_eof:bool],
terminal_present:bool, [terminal_kind:u8,exit_code:u32,signal:u32,
  core_dumped:bool,raw_wait_status:u32,wait4_reaped:bool,
  terminal_evidence_sha256:digest],
record_sha256:digest
```

Bracketed fields exist exactly when their preceding presence byte is one.
`record_sha256` is
`D("groundplane.postgres16.helper-state-record.v1", all preceding state bytes)`.
Sequence starts at one and increments by exactly one per durable replacement;
the first preceding digest is 32 zero bytes and every later value equals the
prior record digest. The embedded intent bytes must be canonical and reproduce
the stored intent digest. A complete state record is at most 65,536 bytes.
`gate_release_authority_sha256` is cycle-free
`D("groundplane.postgres16.gate-release.v1",
launch_intent_sha256||ready_frame_sha256)`.
When `gate_profile_present=true`, the retained `gate_profile` and
`gate_profile_fd_set_sha256` are exactly the PROFILE_APPLIED
`SecurityProfileV1` and `fd_set_sha256` subrecords. Canonical reconstruction of
that complete sequence-2 PROFILE_APPLIED frame using the retained nonce and
intent digest MUST reproduce `profile_frame_sha256`; applying the defined
`groundplane.postgres16.gate-applied-profile.v1` domain to
`canonical(gate_profile)||gate_profile_fd_set_sha256` MUST reproduce the
child's `applied_profile_sha256` whenever `child_present=true`. Missing input or
either digest mismatch makes the state noncanonical and returns
`recovery_required`.

Terminal kind values are exactly `1=exited`, `2=signaled`, `3=gate_fatal`,
`4=unavailable_not_parent`, and `5=unavailable_boot_changed`. For
`phase=terminal`, the original parent's non-reaping waitid observation permits
exactly these encodings:

- `exited` requires `CLD_EXITED`, `exit_code=0..255`, `signal=0`, and
  `core_dumped=false`;
- `signaled` requires `exit_code=0`, `signal=1..64`, and either `CLD_KILLED`
  with `core_dumped=false` or `CLD_DUMPED` with `core_dumped=true`;
- `gate_fatal` requires a complete final FATAL frame and EOF plus its retained
  stage, errno, and frame digest. A normally exited gate requires `CLD_EXITED`,
  `exit_code=1..255`, `signal=0`, and `core_dumped=false`; a signaled gate
  requires `exit_code=0`, `signal=1..64`, and the same exact
  `CLD_KILLED`/false or `CLD_DUMPED`/true mapping. These status fields describe
  only the gate lifetime and never claim a PostgreSQL client exit; and
- unavailable kinds are invalid at `terminal` or `reaped` and are encoded only
  by the `recovery_retired` rule below.

Every `terminal` encoding has `wait4_reaped=false` and
`raw_wait_status=0`. No other kind/code/signal/core combination is valid.
`terminal_evidence_sha256` binds the
canonical waitid/gate/I/O evidence available at that phase; it does not claim
restore success. Apply success is the external conjunction of exact state/I/O,
the sole Wait4 exit zero, and matching successful ExecInspect and only then
authorizes the separate fixed verification.
The terminal-evidence preimage is, in order,
`launch_intent_sha256 || progress_phase:u8 || ready/profile/fatal frame digests
or 32 zero bytes when absent || canonical I/O subrecord or u32(0) when absent ||
terminal_kind:u8 || exit_code:u32 || signal:u32 || core_dumped:bool ||
raw_wait_status:u32 || wait4_reaped:bool`, under domain
`groundplane.postgres16.terminal-evidence.v1`.
At `terminal`, original-parent exit/signal facts come from non-reaping waitid,
`wait4_reaped=false`, and `raw_wait_status=0`. The transition to `reaped`
changes only raw wait status, `wait4_reaped`, and terminal-evidence digest after
the sole successful `Wait4` naming that exact positive child pid. At `reaped`,
`wait4_reaped=true`; `WIFEXITED`/`WEXITSTATUS` or
`WIFSIGNALED`/`WTERMSIG`/`WCOREDUMP` decoded from the exact returned
`raw_wait_status` MUST reproduce the prior waitid branch and every retained
exit/signal/core fact. Raw status zero is valid only for `exited` with exit code
zero; in particular a normally exited `gate_fatal` has nonzero raw status. The
Wait4 result for `gate_fatal` remains the gate lifetime's status and never a
PostgreSQL client status. Any wrong pid, second successful Wait4, decode
mismatch, or incompatible prior waitid branch is `recovery_required`.

All vectors have at most 64 items except argv's bound above; group vectors have
at most 64 ascending ids. Every text/blob is at most 16,384 bytes unless a
smaller bound is stated. Length arithmetic is checked in `u64` before allocation.
An I/O digest is 32 zero bytes when that stream policy discards or only validates
the stream; the helper does not compute or retain a diagnostic-content hash.
Only sealed restore input and dump artifact output carry their actual SHA-256.
Pidfd numbers are process-local descriptors and are never encoded. State stores
the complete pid/start-ticks/boot/file identity needed to authenticate a newly
opened pidfd; a reopened pidfd is used only after every stored fact matches.

Phase values are `1=phase_created`, `2=gate_durable`, `3=gate_released`,
`4=profile_applied`, `5=child_durable`, `6=io_complete`, `7=terminal`,
`8=reaped`, and `9=recovery_retired`. `progress_phase` is 1..6 and equals the
highest completed nonterminal phase. Presence is exact:

| progress phase | gate ready | gate release | gate profile | child | I/O |
| --- | --- | --- | --- | --- | --- |
| `phase_created` | absent | absent | absent | absent | absent |
| `gate_durable` | present | absent | absent | absent | absent |
| `gate_released` | present | present | absent | absent | absent |
| `profile_applied` | present | present | present | absent | absent |
| `child_durable` | present | present | present | present | absent |
| `io_complete` | present | present | present | present | present |

`child_present=true` requires `exec_fd4_eof=true`, no gate FATAL, and one closed
classification value: `exec_evidence=1` is the original parent's nonterminal
pidfd waitid observation immediately after valid PROFILE_APPLIED/EOF;
`exec_evidence=2` is its `CLD_EXITED` observation of the sealed gate lifetime
after valid PROFILE_APPLIED/EOF. No other value is valid. Evidence 2 requires
the later sole Wait4 to be a matching normal exit. EOF alone, signal/core,
missing or inconsistent FATAL, wait ambiguity, or a gate invariant failure
keeps `child_present=false` and cannot authorize I/O, apply success, or replay;
a mutation operation remains `recovery_required`.

For nonterminal phase 1..6, `phase==progress_phase` and terminal is absent.
`terminal`, `reaped`, and `recovery_retired` retain the exact high-water
`progress_phase` and corresponding presence row and require terminal present.
Gate-fatal fields are present exactly for terminal kind `gate_fatal` and absent
otherwise.
`reaped` requires `wait4_reaped=true` and an exact exit/signal wait status from
the original parent. `recovery_retired` requires `wait4_reaped=false`,
`raw_wait_status=0`, and terminal kind `unavailable_not_parent` or
`unavailable_boot_changed`; it also requires `exit_code=0`, `signal=0`, and
`core_dumped=false` and never carries any exit/signal claim.

The only forward progress edges are
`created->gate_durable->gate_released->profile_applied->child_durable->io_complete`.
The original parent may move any reached phase to `terminal`, then only to
`reaped`, then unlink. A different root recovery invocation may move any reached
phase only to `recovery_retired` after the phase-specific positive-lifetime
proof below, then unlink. Same-phase replacement is allowed only for strictly
increasing bounded I/O counters; no other field may change. Each replacement is
temp-write, file-fsync, atomic
rename, parent-fsync. Invalid field, presence, bound, digest, sequence, phase or
transition retains the file and returns `recovery_required`.

A file's `0600` mode cannot protect its directory entry from another process
that owns and can write the containing directory. Therefore the rejected
same-UID design, in which helper state and PostgreSQL both ran as uid 70, could
not prevent a server process or extension from renaming or unlinking the state.
There is no chmod-only repair for that design; root ownership of the `0700`
directory is the required isolation boundary.

One zero-old-state activation barrier inspects every managed PostgreSQL
container and requires the attested root-owned state directory to be empty
before true Ready, positive capacity, or ordinary Backup admission under the
new release.
Legacy uid-70 ownership, an unknown schema/profile/version, or any existing
state file blocks activation and requires the canonical container lifecycle to
prove no unresolved task and recreate the container from the new digest. The
barrier never chowns, deletes, migrates, adopts, or interprets old state.

The barrier does not block Agent authentication, false Ready with zero
capacity, staging inventory, Controller dispositions, `stop`, or recovery
traffic. The clean pre-release replacement has no old executable helper or
registry entry. Only proved empty old state permits container recreation and
activation of the new managed release digest. If old state is found, the new
pair reports false Ready and recovery-required. It does not execute, translate,
or delete that state.

A PostgreSQL workload container with unresolved helper state MUST NOT be
removed, recreated, exchanged, or upgraded. Groundplane first completes the
exact live-supervision or `stop` recovery-retirement procedure.

Identity ambiguity, deadline expiry, an externally missing state directory, an
externally removed container, or state loss before positive terminal proof
produces `recovery_required`. It never authorizes replay, container replacement,
discard, or inference of success.

Ordinary container removal after terminal-receipt cleanup removes the empty
state directory with the container and creates no separate host cleanup
obligation.

#### Capture producer-start barrier

Capture performs one dump under `ExclusiveUnknown`, never an estimate/counting
dump/dry run. Agent creates and retains the exclusive stage, attests/probes,
ExecCreates an unstarted exec, sends a Controller-acknowledged producer-start
intent, receives its exact acknowledgement, reattests the same container/exec,
then and only then attaches/starts once and continuously streams stdout.

The intent binds exactly:

```text
point_id; execution_nonce (32 raw bytes); container_id; exec_id
repository_digest; expected_labels_sha256 (32 raw bytes)
adapter_contract_version=1; database; role; max_plaintext_bytes
```

ADR 0048 owns its schema-1 wire name. No helper process/stdout byte may exist
before ack. Dump stdout is artifact data bounded by sealed maximum and exact
incremental allocation; stderr is drain/discard capped at 32 KiB. Each wholly
unwritten range is first fallocated with `FALLOC_FL_KEEP_SIZE`, then completely
written/hash-counted. Allocation/short-write/overflow/process/limit/fsync/final
size or hash failure closes and removes partials.

Before ack, unstarted exec/empty stage may be replaced with new nonce. After
ack, start right is one-shot. Reconnect never starts or reattaches that exec;
Docker cannot prove never-started and lost hijack cannot recover stdout.
Running exec is authenticated/stopped/reaped; other states are inspected and
retired. Stage is discarded and a new Controller disposition/intent is needed.
No bytes/archive/exit/size/hash are salvageable unless the acknowledged attempt
maintained one continuous stdout-to-fd stream through helper reap, terminal
ExecInspect success, stage fsync/fstat/hash. Otherwise output is removed even
if independently valid.

#### Restore list and apply

Restore opens the retained decoded source twice through independent readers at
offset zero: one list, then one apply. ExecAttach writes bounded 32-KiB chunks
and CloseWrite only at exact EOF. Helper hashes/counts forwarded bytes, requires
expected size/SHA and one extra EOF read, then closes child stdin. Early/extra
bytes, short forwarding, child close, or mismatch fails. No in-container file.

Restore-list stdout is fully drained/discarded in O(1) memory with checked u64
count and no size rejection; stderr alone has hard 32-KiB cap. Complete input
and child exit zero are required before mutation. Apply stdout/stderr each have
independent 32-KiB drain/discard caps. Diagnostics are never logged/persisted.

Restore durably holds the owning Attach, surviving grants, all consuming and
fact-exposed Services (including other Environments), revisions, and prior
runtime intents; locks affected Environments in id order; then the Controller
orders and acknowledges while the Agent stops prior-running Services in
Service-id order and runs termination and zero-connection proof.

Apply ExecCreate remains unstarted until Controller acknowledges an ADR-0048-
owned apply-start intent binding nonce, container/exec, repository/labels
digests, source size/SHA, database/role, target/dependency revisions, and holds.
Agent reattests then uses the fresh second reader. This is destructive boundary.

Crash before ack is safe redo. After ack, never blindly start/reapply. Positive
terminal proof is exact input size/hash/EOF, child exit zero/reap, and same-
container ExecInspect not-running/exit-zero; then only fixed verification may
run. Once the apply-start acknowledgement exists or the helper has durably
entered `gate_released`, the attempt is spent: zero forwarded bytes, pre-exec
failure, boot change, unknown release consumption, missing wait status, or any
other non-proven successful terminal is `recovery_required` and never permits a
new apply intent. Retain
artifact/holds/locks/stopped consumers for explicit recovery. After fixed
verification, restart prior-running consumers in Service-id order through
health gates. Other intents remain. Restart failure never reapplies dump.

### 10. Reconcile acknowledged staging before Ready

An Agent stage has one or two final files in closed role order. A final whose
schema-1 completion acknowledgement may have committed is never silently
deleted/reused after restart. Before Ready, Agent validates each complete stage
plan and accepts exactly one Controller disposition per recovered stage:

- `ResumePrepared` binds exact roles, lengths, SHA-256 values, assignment-resume
  digest, and remaining-growth mode; Agent verifies the complete stage.
- `DiscardRecovered` removes the entire stage and fsyncs its parent.

Missing/extra/duplicate/mismatched disposition blocks mutation and Ready.
Controller may Discard only terminal, cancelled, superseded, unknown, or
explicitly abandoned authority. Active irreversible mutation must Resume or
remain `recovery_required`, never infer discard. Partials never reaching a
possibly acknowledged final completion are removed and never promoted by local
size, digest, format, or helper status. Volume exchanged trees recover through
section 8 intents instead.

ADR 0048's shared Backup Task authority carries the sole Environment and Service
fact set. PostgreSQL and Volume format authorities contain only stable Service
ID references into it; they never duplicate consumer names, Service/Compose
revisions, prior runtime intents, label digests, or repository digests. Config
exposure ids also resolve through that one set. Any missing or conflicting
reference rejects before host effects.

### 11. Fidelity and decision boundaries

Config and accepted Volume logical trees are byte-reproducible. PostgreSQL
logical dumps are not promised reproducible for equivalent databases. Volume
fidelity is only path bytes, kind, file bytes, uid/gid, and low twelve mode
bits; unsupported times/inodes/allocation/sparse/links/ACL/xattr/capability/
label/flag/device/socket/FIFO/mount semantics reject. PostgreSQL excludes
physical layout, WAL, cluster roles, ownership/ACL restoration, and other
databases; required PG16 extensions/facilities must exist.

This adds no product action, endpoint, command, source, scheduler, or retention
rule. ADR 0048 must carry these exact facts in sole schema 1; this ADR does not
duplicate field numbers, queue grammar, or checkpoint names.

### PostgreSQL helper recovery retirement

The managed PostgreSQL container's helper state uses the closed `phase` vocabulary
`phase_created`, `gate_durable`, `gate_released`, `profile_applied`,
`child_durable`, `io_complete`, `terminal`, `reaped`, and
`recovery_retired`. A normal live root supervisor remains the child parent: after a
terminal pidfd/proc observation it performs exactly one `Wait4`, durably records
the exact wait status and `reaped`, fsyncs the state, unlinks it, and fsyncs the
parent directory.

Recovery by a different root helper invocation has a separate closed branch. If the
stored supervisor lifetime is proven absent or terminal, the recovery invocation
is not the child parent and MUST NOT claim a normal exit status or `reaped`:

- Pidfd-first authentication is universal for every recovered supervisor,
  gate, post-exec client, or scan candidate. A `/proc` enumeration may produce
  only a numeric candidate; recovery calls `pidfd_open(candidate_pid, 0)` before
  treating any `/proc` field as authentication. With that pidfd open, recovery
  calls `ppoll` on exactly that pidfd with `events=POLLIN` and zero timeout,
  requires return zero and no `revents`, reads and revalidates every applicable
  stored fact against one `/proc` snapshot, then repeats the identical
  zero-timeout pidfd `ppoll` and again requires return zero and no `revents`.
  The facts are exact pid, ppid and parent identity,
  pgid, start ticks, boot id, all real/effective/saved/filesystem uid/gid values,
  supplementary groups, capability sets, `no_new_privs`, seccomp state,
  executable path/device/inode/digest/mode, closed argv/environment/fd profile,
  and the recorded security-profile digest. Pidfd `POLLIN` means terminal; a
  failed `pidfd_open`, nonzero/unexpected `ppoll` result or `revents`, disappeared
  `/proc` entry, or any mismatch is never signal authority and enters the
  bounded phase-specific absence/reuse proof or remains ambiguous. No recovery
  path authenticates from `/proc` first. Only a pidfd that remains nonterminal
  across the complete matching snapshot may be passed to `pidfd_send_signal`.
  Non-parent recovery never calls waitid or Wait4; those are reserved to the
  original parent;
- `phase_created` never retires from lack of a recorded child. Recovery first
  proves the recorded supervisor lifetime absent, proves unchanged boot
  identity, and completes two bounded stable numeric `/proc` enumerations. An
  exact candidate requires real/effective/saved/filesystem uid and gid zero,
  exact current ppid plus that parent's pid/start ticks/boot identity (or an
  authenticated reparenting after the recorded parent disappeared), gate
  `st_dev`, `st_ino`, byte digest and mode `0500`, nonce, and the complete closed
  gate argv. Each numeric candidate is opened by pidfd and authenticated under
  the universal rule before it may be terminated; an
  ambiguous root candidate retains state. A uid-70 process with copied argv is
  positively excluded by credentials and executable identity and cannot execute
  the root-only gate;
- `gate_durable`, `gate_released`, and `profile_applied` first open the stored
  gate pid by pidfd, then authenticate its start ticks, boot id, pgid,
  executable/argv or post-exec client identity under the universal rule before
  any signal. `gate_released` never retires merely because release consumption or
  exec is unknown; the exact stored lifetime must be positively terminal or
  absent/reused with mismatching start ticks under the same boot;
- for `child_durable` or a later nonterminal phase, first open each stored
  positive pid by pidfd, then authenticate the stored boot identity, original
  supervisor uid/gid `0:0`, and child uid/gid `70:70`, empty
  supplementary groups, start time, absolute executable, closed argv, and
  process group under the universal rule before signaling;
- signal only the authenticated positive leader with `pidfd_send_signal`; use
  the operation's finite absolute deadline and a finite post-`SIGKILL` deadline,
  and wait only by deadline-bounded `ppoll` on that pidfd until `POLLIN` proves
  termination. Recovery never substitutes waitid, Wait4, a blocking wait, or
  `/proc` disappearance for that pidfd terminal observation;
- after the exact child lifetime is gone, durably replace the state with
  `phase=recovery_retired` and
  `terminal_status=unavailable_not_parent`, fsync it, then unlink it and fsync the
  parent directory. The stop operation may then report cleanup success; and
- any identity ambiguity or deadline expiry retains the state and returns
  `recovery_required`.

A stored boot id different from the current boot id positively proves that the
old supervisor/gate/client lifetime cannot still exist. Recovery may record
`recovery_retired` with `terminal_kind=unavailable_boot_changed` and retire the
process state without scanning or signaling. It never fabricates a wait status
or operation success. For `restore-apply` after its acknowledged start or gate
release, boot change leaves the Task, artifact, locks, holds and stopped
consumers in `recovery_required`; it does not authorize apply replay or success.

`unavailable_not_parent` and `unavailable_boot_changed` are valid only with
`recovery_retired`; neither is an exit code, signal status, or synonym for
`reaped`. This retirement branch applies
to every closed helper operation when its original supervisor is absent:
`probe-pg-dump`, `probe-pg-restore`, `probe-psql`, `server-major`, `dump`,
`restore-list`, `terminate-db-connections`, `assert-zero-db-connections`,
`restore-apply`, and `post-restore-verify`, including any of their
`profile_applied`, `child_durable`, or later stored states. It does not weaken the exact
parent-owned `Wait4` path while the original supervisor exists.

Fault-injection proof covers process death immediately before and after the
`phase_created` fsync, gate spawn return, ready identity read, `gate_durable`
fsync, `gate_released` fsync, release-byte write, PROFILE_APPLIED frame,
`profile_applied` fsync, fd-4 exec EOF, alive or fast-normal-terminal sealed-gate
exec classification and every ambiguous signal/core/FATAL branch,
`child_durable` fsync, every input/output durability
boundary, `io_complete`, terminal observation, `terminal`, `Wait4`, `reaped`,
state unlink and parent fsync. Every recovery path proves either the exact live
lifetime it controls or positive absence; it never turns a spawn-to-durable gap
into success, replay, or unauthenticated retirement.

Container root is deliberately bounded to this already managed, digest-attested
PostgreSQL container. `Privileged=false`, protected-mount rejection, exact image
and helper digests, and the no-copy/no-helper-container rules remain unchanged.
The Agent's Docker authority, Docker daemon and host root, and container root
are explicit trusted computing-base members; PostgreSQL uid 70 is not trusted
with supervisor state. Host root or container root can still alter that state,
so this decision does not claim protection from those trust-boundary actors.

This cleanly replaces every helper-as-`postgres` and uid-70 state contract.
There is no alternate `User=postgres`, rootless-helper fallback, dual state
owner, ownership migration, or adoption of old state. Delivery rebuilds and
republishes the managed image, level-one contract, level-two release record,
Controller, and Agent together; prior contract/release digests and any
pre-contract container or helper state are first drained only by their matching
old generic AdapterRevision/helper as the activation barrier requires. After proved empty state,
containers are recreated through the canonical clean bootstrap lifecycle and
the new generic AdapterRevision activates; anything not drained remains rejected rather than
being adopted by the new helper.

## Relationship to prior decisions and acceptance

Accepted together on 2026-08-30, ADRs 0047 and 0048 cleanly supersede only ADR
0024's incomplete artifact framing and canonical formats, upload/Head/point-
commit checkpoint detail, Config active-generation publication, and Agent
staging/recovery mechanics. ADR 0048 additionally supersedes the Backup-only
terminal-receipt clauses in ADRs 0024 and 0035 with one generic Agent-task
receipt. ADR 0024's product sources, scheduling, immutable run/retry identity,
retention ownership, operator behavior, and deadlines remain in force. ADRs
0020, 0022, 0029, and 0046 remain constraints. There is no compatibility
contract.

Acceptance grants implementation authority only. It claims no protobuf,
generated-protocol, persistence, Controller, Agent, test, deployment, managed-
image release, live R2, or C16 acceptance gate has passed.

The paired schema also closes Connector endpoint/region bounds at a 2,048-byte
canonical URL and a region of 1 through 64 bytes, each exactly in ASCII range
`0x21..0x7e`, while preserving the accepted ADR 0005 and ADR 0045 Connector
contracts.

The ordered mirror set for ADR 0047 and ADR 0048 is identical:

1. `docs/mvp.md`
2. `docs/api-cli.md`
3. `docs/blueprint.md`
4. `docs/architecture.md`
5. `docs/capabilities.md`
6. `docs/status.md`
7. `console/src/lib/store.tsx`
8. `console/src/lib/types.ts`
9. `console/src/lib/mock-data.ts`
10. `console/src/components/common/service-form-body.tsx`
11. ADR 0005
12. ADR 0010
13. ADR 0020
14. ADR 0022
15. ADR 0024
16. ADR 0029
17. ADR 0035
18. ADR 0045
19. ADR 0047
20. ADR 0048

## Consequences

- Config/Volume have one representation and strict readers.
- Config restore is bounded two-pass canonical-primary roll-forward.
- Volume is capped at 2048 entries with old/new immutable manifests and exact
  construction/finalization/deletion intents.
- No aggregate cleanup checkpoint can disagree with final root deletion.
- PostgreSQL uses one release-record-pinned dual-platform workload OCI index,
  with exactly one native runnable child for each of `linux/amd64` and
  `linux/arm64`, derived from the authenticated PostgreSQL 16.15 Alpine source,
  with the fixed helper and private client gate baked into that database image rather than the Agent image or a helper
  container, an acknowledged dump start, and a non-blindly-replayable apply
  boundary.
- The mutable `postgres:16-alpine` identity is removed cleanly and has no
  compatibility path.
- The two-level build contract and post-push release record bind helper bytes,
  runtime configuration, upstream provenance, managed repository, managed
  digests, adapter projection, Compose, and Agent compatibility without a digest
  cycle or operator-controlled image input.
- Acknowledged staging survives restart and blocks Ready pending disposition.
- Artifact version 1 and Agent schema 1 each have one meaning.

## Rejected alternatives

Permissive tar readers, Config shadow active generations, copy-over Volume
restore, aggregate cleanup completion, shell/TCP/filename PostgreSQL execution,
and unacknowledged staging salvage are rejected because each creates a second
representation, a non-atomic publication boundary, or authority inferred from
local state.
