# Backup Agent execution and recovery

Read this document for execution authority, checkpointing, bounded transfers,
staging recovery, terminal delivery, and crash/reconnect behavior. The
[schema-1 wire contract](wire-contract.md) preserves exact accepted messages
that are absent from the checked-in protobuf. This document owns the ordering,
digest, transaction, and recovery invariants that a message catalog cannot
express. It does not preserve an alternate pre-release wire shape.

## Channel and schema

The Controller serves the Agent bidirectional gRPC channel only on the
machine-bootstrap Unix socket. It does not listen on TCP. Every stream supplies
the bootstrap token; socket ownership/mode protects transport and the token
authenticates the Agent process. TLS is not used on this local channel.

There is one final MVP execution-plan schema: schema 1. Authenticate and every
ExecutionPlan require value 1. Omitted zero and all other values reject before
recovery or work. There is no negotiation, fallback, translation, dual writer,
store conversion, or compatibility decoder for incomplete pre-release shapes.
Unknown protobuf fields are recursively rejected before durable or external
mutation, as are unset oneofs, invalid enums, noncanonical ordering, and every
length/count violation.

PostgreSQL capability authentication carries exactly the 32 decoded bytes of
the managed release digest defined in [the PostgreSQL contract](postgresql.md).
It is required even before PostgreSQL Backup runtime is complete. Controller
and Agent compare exact bytes; an empty, placeholder, mutable image reference,
or second digest path is invalid.

The Agent samples one nonzero 16-byte CSPRNG `process_generation` before its
first Authenticate and reuses it for every reconnect in that process. Restart
samples another. This is distinct from durable positive `AgentGeneration` and
from positive Controller-owned `assignment_generation`; none is derived from or
substituted for another.

`AgentConfig.labels` is the sorted unique repeated key/value representation,
not the incomplete map representation. Legacy schema-2 readiness identifiers
do not survive. Retired tags/names remain reserved, including the old relative
`timeout_seconds` assignment field and the incomplete Backup checkpoint fields.
The separate [outer-message allocation holds](wire-contract.md#outer-message-allocation)
are released only for their named schema-1 fields when those fields are implemented.

## Assignment authority

Every Agent has a durable monotonic assignment sequence. Claim compares the
current value, writes its exact successor to the sequence and every assignment
copy, and allocates assignment atomically; failed claims consume nothing. The
first is 1 and exhaustion at `MaxInt64` fails closed. New Tasks and retries
consume a value. Redispatch, reconnect, and process restart reuse the exact
assignment id and generation. Credential rotation, disconnect, Task pruning,
and ordinary cleanup never reset it. Final Agent removal deletes the sequence
only after fixed-revision proof of no active assignment, indexes, timeout rows,
or non-clean terminal delivery.

The plan hash is deterministic schema-1 ExecutionPlan bytes with its hash field
cleared. Resume is outside the plan hash. Controller reads authority and resume
together at their exact revisions.

One Backup Task authority has exactly one Project, Environment, absolute
deadline, assignment fence, and sorted common Service-fact set, plus 1..12
ordered unique step authorities. Step order equals the sealed plan. Each step
has a stable step id, 32-byte execution id, digest, deadline, sorted Service-id
references, and exactly one capture, restore, or prune authority.

The common Service set is the only Service fact map. Each row pins stable id,
current name, Service/Compose revisions, prior runtime intent, exact required
label count/digest, and repository digest. Steps, Config exposure, PostgreSQL,
and Volume contain references only; duplicate or conflicting Service facts are
invalid.

All mutable resources are stable-id facts with exact Controller modification
revision and content digest. The sealed authority includes every Project,
Environment, source/target, Service, Volume, Entry snapshot, Secret/fact/grant
hold, Connector, Compose projection, retention policy, and object identity
needed by the work. The Agent never resolves a mutable label during execution.

Connector authority pins canonical endpoint (1..2,048 bytes), region (1..64
printable non-space ASCII bytes), addressing style, bucket, prefix, object key,
and two credential-slot identities/revisions. Object key is opaque and at most
1,024 bytes. A successful object identity uses present VersionId, even if its
value is empty, or otherwise non-empty ETag; each is at most 1,024 bytes.

Backup payloads never contain generic parameters, shell strings, pipelines,
redirection, arbitrary executable paths, credentials, age identities, Entry
values, or artifact bytes. Task/Event/public records also omit private object
identity and provider diagnostics.

## Digests and checkpoint fences

All protocol digests use SHA-256. The shared primitive is:

```text
D(domain, body) = SHA-256(ASCII(domain) || 0x00 || body)
B(x)            = u32be(len(x)) || x
E(message)      = deterministic protobuf after recursive unknown-field
                  rejection, with only its resulting self-digest cleared
P(message)      = u32be(len(E(message))) || E(message)
```

Integers are unsigned big-endian at their declared width, booleans are one byte
0/1, and enums are u32 big-endian. No native integer, JSON number, textual
length, protobuf outer wrapper, map, or implicit default enters a digest unless
the named formula says so. Config and Volume member formulas each add exactly
one explicit `u32be(len(E))`; they never wrap `P(E)` in another length.

Normative digest domains and bodies are:

| Evidence | Domain and body |
| --- | --- |
| plan step | `groundplane.agent.v1.execution-step`; `plan_hash || P(step)` |
| Backup authority | `groundplane.agent.v1.backup-authority`; `P(authority)` |
| Agent terminal result | `groundplane.agent.v1.task-terminal-result`; `P(TaskAck)` in the sole final shape, including execution epoch, recovery-record digest, and assignment generation |
| terminal Task | `groundplane.task.terminal-record.v1`; `B(canonical terminal Task primary bytes)` |
| terminal receipt | `groundplane.agent.v1.task-terminal-receipt`; `P(receipt record)` |
| Config metadata content | `groundplane.backup.config-metadata-content.v1`; count then each artifact projection as `u32be(len(E_i)) || E_i` |
| Config metadata transfer seed | `groundplane.agent.v1.config-metadata-transfer-chain`; authority digest, transfer id, direction, and content authority |
| Config metadata member | same chain domain; prior chain, ordinal, and one length-prefixed direction-selected Entry |
| Config value seed/member | `groundplane.agent.v1.config-value-chain`; final metadata chain, then prior chain, ordinal, chunk count, value size, and bytes |
| Volume content manifest | `groundplane.backup.volume-manifest-content.v1`; count then each length-prefixed canonical Entry |
| Volume transfer seed/member | `groundplane.agent.v1.volume-manifest-transfer-chain`; authority/transfer/direction/role/content and tree digests/count/source-evidence, then prior chain/record sequence/batch bounds/length-prefixed Entries |
| Volume construction/finalization | `groundplane.agent.v1.volume-construction-chain` and `.volume-finalization-chain`; authority plus fixed manifest/final chain, then length-prefixed intent and completion |
| Service progress | `groundplane.agent.v1.service-progress-chain`; authority, operation kind, count, then length-prefixed progress records |
| Config materialization | `groundplane.agent.v1.config-materialization`; generation/render identity followed by stable-Entry ordered Entry/Service/destination/value and observation evidence |
| container labels | `groundplane.agent.v1.container-labels`; sorted unique count then `B(key)||B(value)` |
| PostgreSQL verification | `groundplane.agent.v1.postgres-restore-verification`; point/container/artifact/database/role/source and observation evidence |
| runtime observation | `groundplane.agent.v1.runtime-observation`; Service/container/repository/labels/runtime phase |
| Config credit ack | `groundplane.agent.v1.config-credit-ack`; one explicit length-prefixed Credit encoding |
| S3 metadata | `groundplane.agent.v1.s3-object-metadata`; sorted unique count then `B(key)||B(value)` |

Content digests are independent of session authority. Config capture projects
revision-bearing metadata to the exact revision-free artifact restore form;
capture and restore therefore reproduce one metadata-content digest. The
separate transfer chains then bind direction, session, replay, and authority.
Volume likewise separates role-independent manifest content from its role-bound
transfer chain. Neither digest can substitute for the other.

Checkpoint sequence is positive, one-based, contiguous `uint64`. A fresh
resume can name sequence zero with no predecessor; later resume carries the
immediately preceding nonzero checkpoint and fence. Every request repeats task,
assignment, step, execution id, shared authority digest, sequence, predecessor
fence, and exactly one typed payload.

One Controller transaction compares authority, exact locks/holds, current
resume, predecessor, and operation cursor. It atomically performs the domain
transition, creates immutable completed dedupe, advances resume, and performs
required publication. The ack fence is the authority digest plus the completed
dedupe key's exact modification revision. The Agent cannot perform the next
irreversible effect before receiving it.

Lost-ack dedupe is checked before stale-fence rejection. Exact duplicate returns
the stored result/fence. Changed duplicate, sequence reuse, gap, missing
predecessor, or fence mismatch rejects without work. Lifecycle state or a
diagnostic TaskEvent is never checkpoint authority.

Config primary roll-forward and materialization cleanup are the bounded
exception. Their initial checkpoint creates one pending claim containing fixed
membership, hashes, holds, preallocated ids, cursor, and prior resume. Bounded
transactions advance only that claim. The final transaction publishes/cleans,
creates completed dedupe and ack fence, advances resume, and removes the claim.
Replay while pending resumes the claim rather than taking the generic completed
path.

## Absolute deadlines

Backup capture, restore, and prune Tasks persist one absolute deadline exactly
21,600 seconds after creation. It is carried unchanged as the assignment
forward deadline. Retry, reconnect, process restart, or adoption never extends
it. Each step deadline is persisted, digest-bound, and no later than the Task;
prune steps are exactly 1,800 seconds.

For an API taking seconds, remaining time is floor of the positive nanosecond
difference divided by one second, clamped to zero. Zero means do not start. A
monotonic timer correlated with the absolute deadline still stops at the
deadline, so rounding may shorten but never lengthen work.

## Secret delivery

There are exactly four Backup secret purposes: S3 access key, S3 secret key,
current age identity, and operator-supplied old identity. Config values use the
typed Config transfer and are not secret slots.

Capture and prune receive exactly the two S3 credential slots. Capture encrypts
with the public recipient and prune never decrypts, so neither receives a
private identity. Current-era restore receives current identity; old-era
restore receives only the request identity.

Every header, chunk, and end repeats task, assignment, step, and purpose.
Credentials are 1..262,144 bytes in 1..8 canonical 32-KiB chunks. Each age
identity is 1..4,096 bytes in exactly one chunk. Header declares nonzero total
and exact count; sequence is one-based; non-final credential chunks are exactly
32,768 bytes; End repeats count.

Slots are fenced by `(task_id, assignment_id, step_id, purpose)`. The Agent
accepts only the active assignment and clears each chunk after consumption, the
slot after successful consume, and all slots on completion, failure, timeout,
abort, cancellation, stream loss, or reconnect before redispatch. Durable
direct or Secret-backed credentials can be resolved on valid redispatch. A
request-only old identity cannot, which is why that restore is non-retryable.

Connector Secret references are late-bound keys. Every delivery resolves the
consumer Environment's Project first and platform fallback second. There is no
Secret-to-Connector reverse index; deleting a Secret does not scan or block on
Connector references. Later resolution either selects fallback or fails closed.

## Bounded transport and scheduling

gRPC compression is disabled. Both sender and receiver check the complete outer
`proto.Size`, excluding only the five-byte gRPC frame header, before enqueue and
after decode. Global maxima are 16,777,216 bytes Agent-to-Controller and
5,242,880 Controller-to-Agent. A deterministic ExecutionPlan is at most
4,194,304 bytes; a complete assignment is at most 5,242,880. Backup authority
is at most 522,752 bytes and resume at most 65,536.

Backup-related variant maxima are:

| Variant | Bytes |
| --- | ---: |
| any Agent message | 16,777,216 |
| Authenticate | 1,609 |
| Ready | 256 |
| TaskEvent | 65,536 |
| ObservedState or TaskAck | 16,777,216 |
| Backup checkpoint request | 16,384 |
| Agent Config or Volume bulk record | 262,144 |
| Agent Config or Volume Credit/Ack | 512 |
| staging inventory | 4,259 |
| staging recovery ack | 72 |
| Agent terminal/retirement record | 512 |
| any Controller message | 5,242,880 |
| TaskAssignment | 5,242,880 |
| TaskAbort | 512 |
| ConfigUpdate | 65,536 |
| Shutdown | 16 |
| Materialization transfer | 65,536 |
| checkpoint ack | 512 |
| secret-slot record | 65,536 class cap plus its smaller exact per-record cap |
| Controller Config or Volume bulk record | 262,144 |
| Controller Config or Volume Credit/Ack | 512 |
| staging recovery plan | 5,861 |
| Controller terminal/recovery/retirement ack | 512 |

At most 32 Tasks are assigned/nonterminal on one connection. One task has at
most four active Config/Volume credit-controlled bulk queues, one
Materialization inline transfer, and four active secret slots multiplexed
through one inline queue. Each bulk queue is at most 8 records, 1,048,576
serialized bytes, and 262,144 content bytes. Materialization is at most 34
records, 1,054,256 serialized bytes, and 1,048,576 content bytes. All four
secret slots total at most 26 records, 592,764 serialized bytes, and 532,480
content bytes and are produced incrementally.

Queue admission is also exact:

| Queue class | Records | Serialized bytes | Content bytes |
| --- | ---: | ---: | ---: |
| connection control | 32 | 262,144 | none |
| assignment | 1 | 5,242,880 | none |
| checkpoint request/ack | 64 | 1,048,576 | none |
| TaskAck/terminal Agent records | 32 | 16,777,216 | none |
| terminal Controller acknowledgements | 32 | 16,777,216 | none |
| ObservedState | 1 | 16,777,216 | none |
| TaskEvent | 128 | 1,048,576 | none |
| each Config/Volume bulk transfer | 8 | 1,048,576 | 262,144 |
| per-assignment Materialization inline | 8 | 262,144 | 262,144 |
| per-assignment secret inline | 8 | 262,144 | 262,144 |

There is one Recv goroutine and one Send goroutine. Recv validates and attempts
nonblocking admission to a distinct bounded queue; it never waits on a worker,
drops, coalesces, or overwrites. A full inbound queue closes with
`RESOURCE_EXHAUSTED`. A full Task outbound queue fails that Task; a full
connection-control queue closes the stream. Producers enqueue and never call
Send directly.

The writer uses a work-conserving cyclic scan in this order:

```text
control, assignment, checkpoint, terminal, observed, event,
materialization-inline, secret-inline, bulk
```

The next scan begins after the class that emitted; at most one record emits per
class per scan. Inline and bulk classes independently rotate by Task and active
transfer. Bulk is eligible only when both record and complete-serialized-byte
credit admit the next item. Start and Resume have their specified first-record
exceptions. Inline transfers receive no bulk credit and cannot borrow another
queue's caps.

Observed-state projects sort by stable id and are never split. Multiple
envelopes share one nonempty 16-byte snapshot id, contiguous zero-based batch
ordinals, and final flag only on the last. A Project too large to fit alone
fails observation. Controller publishes only a complete contiguous sequence.

## Config transfer

Config capture flows Controller to Agent; restore flows Agent to Controller.
One transfer id is derived from authority digest and direction and remains
stable across reconnect. Metadata is all Entries first, followed by all values.
The semantic transcript and archive formulas live in [artifact formats](artifacts.md#environment-config-format).

Start is record 1 and is the only fresh no-credit record. For nonempty metadata,
receiver grants exactly 46 Headers and 393,216 complete serialized bytes.
Every non-final durability batch is exactly 46 Headers; final is 1..46. Only
after a batch commits does the receiver replace the window with the same grant,
or send `MetadataAccepted` after the final batch. Empty metadata crosses the
barrier immediately. Metadata credit is replacement, not additive, and never
transfers to values.

Metadata rows contain authority/content digests, ordinal, exactly one capture
or restore metadata message, and preceding/resulting transfer-chain digests.
The key is exactly
`/v1/backup/config-metadata/<snapshot-id>/<entry-id>` (87 bytes). One row is at
most 8,078 bytes and its etcd PutRequest 8,170. A durability transaction holds
0..46 next rows plus one progress put, at most 94 application operations and
393,216 encoded bytes. Non-final batches use 46. At least 64 MiB etcd quota
headroom is required before each transaction.

`MetadataAccepted` starts a new credit namespace at sequence 1 and grants eight
records plus 262,144 serialized bytes. Values then run in Entry order. Each
value has 0..8 chunks; non-final chunks are exactly 32,768 bytes and the final
nonempty chunk is 1..32,768. Empty value has no chunk. Exactly one EntryEnd
follows and one Entry is in flight. Receiver retains at most one incomplete
262,144-byte value plus bounded hash/cursor state.

Credit sequences are monotonic within one stream phase. Current and immediately
preceding envelopes may replay only byte-identically and never replenish twice;
current+1 is the only new sequence. Disconnect invalidates all outstanding
credit and retained session acks.

Before durable `MetadataAccepted`, reconnect discards partial metadata/sink
state and restarts at Start with zero credit. After the barrier, Resume is the
sole first record without credit. It must match exact authority, transfer,
direction, reconstructed Entry-boundary record sequence, next ordinal, value
chain, and durable ack, then receives a fresh sequence-1 value grant. Committed
Headers and values never replay. An incomplete value is zeroized/discarded and
retransmitted from offset zero.

For `N` Entries and `C` value chunks, pre-End records are `1+2N+C`, at most
40,961, and End is the next record, at most 40,962. Empty is Start 1, End 2.
After transfer completion no transfer record replays. Config publication and
materialization use separate pending checkpoint claims so reconnect resumes
only their current cursor.

## Volume manifest transfer

Volume uses three exact direction/role pairs: captured Agent-to-Controller,
restore-new Controller-to-Agent, and stopped-live-old Agent-to-Controller.
Captured and restore-new carry source archive evidence. Stopped-live-old carries
an explicit no-source marker because it is a filesystem scan.

Start is fresh record 1. The content manifest digest is role-independent; the
transfer chain binds authority, transfer, direction, role, tree digest, count,
source-evidence presence, sequence, and batch contents. Receiver recomputes
both. Each non-final Batch has exactly 31 contiguous canonical Entries; final
has 1..31. With `b=ceil(N/31)`, Batches are sequences `2..b+1`, End is `b+2`,
and End lies in 3..69. A short non-final batch or 32 entries is invalid.

After Start commits, ack sequence and committed sequence are 1, next ordinal is
1, and credit is at most 8 records/262,144 serialized bytes. A Batch consumes
one record and its outer size and commits exactly its Entry puts plus one
progress put before cumulative ack/replacement credit. Ack sequence equals
committed record sequence. Exact duplicate returns the stored ack and consumes
no second credit.

After reconnect, Resume is the sole no-credit first record. Its outer record
sequence repeats the last committed sequence as a nonsemantic assertion and
must match durable next record/ordinal, chain, ack, authority, role, direction,
and transfer id. The receiver replays ack with new bounded credit; acknowledged
Entries are not retransmitted. End completes only after its durable cumulative
ack.

## Staging and startup recovery

Agent staging is bootstrap-owned and unavailable to project containers. One
directory belongs to one recovery key derived from task, step, and point ids.
Final roles are exactly `SOURCE_PLAINTEXT` at `source.final` and
`STORED_OBJECT` at `stored.final`. Unencrypted work has only source, used as
both evidences. Age work may have source or both finals according to the durable
checkpoint. Managed partial names are removed before inventory; a namespace
without a final is removed.

Unknown names, unsafe metadata, duplicate roles, more than two finals, duplicate
assignment staging, or a 33rd recovered stage fails startup closed and remains
for inspection. Config/Volume reserve exact bounds. PostgreSQL uses exclusive
unknown growth: no other staging writer, physically allocated exact ranges
before write, held until final or failure.

Startup order is:

1. Authenticate and send presence-checked false Ready with capacity zero.
2. Reconcile terminal delivery and receive exact aborts for awaiting rows.
3. Serialize registry/terminal/recovery state, stop and join matching workers,
   prevent inventory mutation, and durably classify TaskAck wins or retirement.
4. Finish pending/applied terminal handshakes and cleanup.
5. Scan the quiescent staging root and send sorted Inventory.
6. Apply one immutable kind/state-specific RecoveryPlan. Reversible rows may
   receive explicit Discard; protected or possibly applied rows receive exact
   Resume/adoption/recovery or remain recovery-required.
7. Journal and fsync plan application, replay its stable ack until receipt,
   prove stage/journal absence, then complete eligible retirement handshakes.
8. Send true Ready with zero capacity; Controller reconciles delivery archive
   and proves active delivery empty.
9. Only then send true Ready with real capacity and admit Config/dispatch.

Silence, timeout, terminal state, local validity, or missing authority never
means discard. A Plan may discard only independently proven reversible,
terminal, cancelled, superseded, unknown, or explicitly abandoned rows.
Irreversible mutation without exact Resume remains recovery-required and blocks
Ready.

Before effects, Agent fsyncs a bootstrap-owned applying journal containing
inventory/plan digests, dispositions, and deterministic ack. Effects are
canonical and crash-idempotent only in applying state. After all effects and
parent fsyncs it writes applied before sending. Restart replays the byte-identical
ack without effects. Controller acceptance returns a receipt bound to current
process generation and stable inventory/plan/ack digests; only exact receipt
permits journal removal.

An acknowledged final is never silently deleted or regenerated. Resume pins
roles, lengths, SHA-256, assignment-resume digest, and remaining-growth mode and
the Agent re-verifies it. Discard removes the complete stage and fsyncs its
parent. A possibly acknowledged Config or stored-object final is reused; Volume
exchanged trees recover through their per-mutation intents.

## Upload and point commit

`artifact_prepared` exists only after producer EOF/success, exact bound/length,
format evidence, file and directory fsync, final installation, open-descriptor
`fstat`, and hashes. Pre-checkpoint failure removes partials. Its ack advances
capturing to uploading and is mandatory before Put. After it, reconnect reuses
verified finals and never regenerates or re-encrypts.

After any Put response, including uncertainty, Agent sends
`upload_completed`. A normal response binds its object discriminator; uncertainty
uses an explicit unknown outcome and invents none. Ack is mandatory before
Head. Head adopts only exact target, metadata, evidence, and discriminator. A
mismatch retains finals/object authority for inspection and is not automatically
overwritten or deleted. `upload_verified` ack advances to point commit.

Point commit is Controller-owned and atomically creates the immutable Recovery
Point and object/evidence authority. There is no Agent point-commit checkpoint.
Uncertainty after verified Head retains point-commit orphan authority and
staging until reconciliation. PostgreSQL/Config cleanup occurs only after point
commit. Volume remains nonterminal until consumer recovery. Stored finals stay
recoverable until terminal transaction/ack; disconnect does not eagerly delete
them.

## Restore protection and adoption

Restore artifact validation checkpoints exact object identity, four-part
evidence, finals, and required source-format evidence before any target
mutation. A capture payload is not restore evidence.

Before first mutation an execution may fail and release authority. After it,
the execution is protected: locks, holds, finals, helper nonce, pending claims,
mutation intents, cursors, and resume fence cannot enter generic retry.

Continuation by another execution requires one Controller transaction that
compares exact predecessor authority/fence, proves retained files/holds, creates
adoption identity, transfers identical locks/holds/cursors/pending/resume, and
terminalizes the predecessor as adopted. New assignment binds adoption
revision/digest. It does not redownload verified data, rerun PostgreSQL apply,
republish Config, reconstruct/exchange Volume, or clear a pending deletion.
Uncertain adoption fails closed.

## Terminal delivery

Every terminal assigned Agent Task has one generic immutable receipt at:

```text
/v1/runtime/task-terminal-receipts/{task_id}
```

It replaces the old Backup-only receipt. Native Controller Tasks and a pending
abort without terminal assignment have none. Receipt contains schema, task and
Agent identities, durable Agent generation, assignment id/generation, plan
hash, terminal outcome, and canonical terminal Task digest. It contains no
TaskAck digest, task revision, delivery state, process generation, or self
digest.

The terminal transaction publishes canonical terminal Task and receipt at one
modification revision and reserves one of the exact 96 application operations,
leaving at most 95 for other effects. Agent Ack then CAS-ensures delivery
`pending` with exact Ack digest. Controller timeout/abort instead ensures
`awaiting_ack` without one. A matching late Ack changes only awaiting to
pending. A changed/outcome-inconsistent Ack cannot rewrite Task or receipt.

Active delivery states are exactly `awaiting_ack`, `pending`, and `applied`, at
most 32 rows. Clean is a separate immutable archive. Missing delivery after a
receipt is fixed-revision reconciled before Ready, pruning, or Agent removal.
Unknown terminal transaction outcome is accepted only when one fixed-revision
read proves canonical Task/receipt at the same revision and exact digests.
Anything torn, reconstructed, or mismatched is corruption.

Awaiting sends only exact TaskAbort. Agent stops/joins the exact worker before
inventory. A matching TaskAck winner enters pending. Without one, Agent fsyncs
`retirement_pending`; a conflicting local Ack can enter it only after Controller
binds that exact first rejected Ack digest into a replayed abort under identical
receipt/assignment/plan/terminal/task-revision authority. A second different
digest changes nothing. The conflict-bound Ack is inert evidence retained until
RetiredAck cleanup. Unclassified Ack blocks retirement.

Pending/applied use ReceiptAck/Applied exchanges. Retirement waits for protected
staging recovery. Only exact safe terminal/adopted state and stage absence allow
`retirement_pending` to become durable Retired. Controller validates immutable
Task/receipt and atomically moves active delivery to clean. Replay uses clean
and resends RetiredAck; Agent then removes local Ack journal and marker in that
order.

`terminal_delivery_clean=true` means terminal, retirement-pending, and Retired
stores are empty. Startup processes their at-most-32 rows in Task-id order;
unknown, corrupt, oversized, or a 33rd row fails closed. TTL, age, reconnect,
listing, or inferred absence never evicts them. Applied-to-clean runs in at most
24 rows/96 operations then 8/32 and proves active prefix empty. Retention deletes
only fixed-revision-validated clean+receipt pairs, at most 24 pairs/96
operations. Awaiting/pending/applied receipts persist indefinitely.

Checkpoint cleanup begins after terminal commit, removes at most 47 rows plus
cursor per transaction, and retains membership until complete. It creates no
second replay authority.

## Transition table

| Operation | Authoritative order |
| --- | --- |
| PostgreSQL capture | container observed; dump start acknowledged; artifact prepared; upload completed; Head/upload verified; Controller point commit; source cleanup; terminal |
| Config capture | bounded metadata/value transfer; artifact prepared; upload completed; Head/upload verified; point commit; source cleanup; terminal |
| Volume capture | consumer stop/progress; artifact prepared; restart/health; upload completed; Head/upload verified; point commit; terminal |
| PostgreSQL restore | artifact validated; container observed; list; consumer stop/zero connections; apply start acknowledged; exact terminal proof; fixed verification; restart/health; terminal |
| Config restore | artifact validated; metadata/value progress; transfer pending claim; primary publication; materialization pending claim; verified; terminal |
| Volume restore | artifact validated; stopped; construction; finalization; exchange; repeated delete intent/path cleaned; restart/health; terminal |
| Prune | repeated exact sealed-object deletion; terminal |

There is no active Config-generation checkpoint, Agent point-commit checkpoint,
aggregate Volume tree-cleaned checkpoint, or event-derived transition.
