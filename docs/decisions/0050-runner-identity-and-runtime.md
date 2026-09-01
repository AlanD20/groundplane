# ADR 0050: Runner identity and runtime contract

## Status

Accepted for the MVP.

## Context

ADR 0033 accepts the scarce-resource and isolation boundary for GitHub
self-hosted Runners: one Tenant-or-Project owner, one combined five-Runner
Tenant quota, one dedicated `/29`, one finite host identity slot, one rootless
Docker daemon, replayable Controller Tasks, a transient registration token, and
local-only removal. The durable implementation already records the owner,
labels, provisioning state, latest creation Task, exact host allocation, and a
replaceable runtime observation.

That contract does not yet identify the external GitHub organization or
repository, the mutable Groundplane slug, the GitHub-visible Runner name, or
the immutable Runner image. It also does not close the public lifecycle
projection, observed-state freshness, the in-process handoff of a one-use
registration token to an asynchronous Task, or the production topology that
keeps untrusted GitHub workflow code away from the host Docker daemon and
Groundplane credentials.

The current fixture Console fills those gaps unsafely. It accepts an arbitrary
name and target, creates an immediately online record, displays a command that
mounts `/var/run/docker.sock`, uses `--restart unless-stopped`, passes the
registration token through an environment variable, and claims that a GitHub Runner receives reusable Groundplane Agent
credentials. Those behaviors contradict ADR
0033 and are not part of the product contract.

The MVP remains one host and persistent Runners only. GitHub deregistration,
GitHub token generation, Runner autoscaling, ephemeral pools, placement,
multi-host operation, GitHub Enterprise, and in-place Runner updates remain out
of scope.

## Decision

### Durable identity and desired input

One durable Runner has these desired fields:

| Field | Contract |
| --- | --- |
| `id` | Stable `run_` id allocated before publication. |
| `slug` | Mutable, Tenant-scoped Groundplane label used by CLI and Console. It is not the GitHub-visible Runner name. It uses the shared lowercase ASCII slug grammar below. |
| `owner_kind` | Exactly `tenant` or `project`. |
| `owner_id` | The Tenant id for a direct Runner or Project id for a Project Runner. |
| `tenant_id` | Always present; equal to `owner_id` for a direct Runner and derived from the durable Project for a Project Runner. |
| `github_url` | Immutable canonical GitHub organization or repository URL shaped by `owner_kind`. |
| `labels` | Canonical ordered custom GitHub labels. GitHub default labels are not stored here. |
| `image_ref` | Exact release-selected digest-pinned image embedded in the same Groundplane release manifest as the Controller and frozen for this Runner. It is never configuration or public request input. |

The lifecycle record is separate from the desired primary. It retains the
accepted ADR 0033 fields `provisioning_state`, `create_task_id`, exact host
allocation, and `created_at`, plus a positive unsigned `runtime_epoch`. It is
the sole authority for the immutable rootful Runner `container_id`, exactly 64
lowercase hexadecimal characters, after
container creation; the container id may be absent while provisioning has not
yet created a container. Desired mutations, including slug replacement, never
write the lifecycle record and never advance `runtime_epoch`.

`runtime_epoch` identifies one immutable materialization of the local runtime.
Before any create, retry, or reboot re-attestation may mutate the host, the
Controller transaction allocates the single prospective value
`next_epoch = current_epoch + 1`, stores it in the lifecycle record, and
publishes the Agent Task plan that carries that value. That transaction is the
only increment for the attempt. The Agent uses the same value in every unit,
network, nftables, container, socket, and RuntimeOwnership identity; binding
the container and accepting attestation do not increment it again. Cleanup
also does not increment it. Ordinary lifecycle and observation changes within
the same materialization do not advance it. An abandoned prospective epoch is
never reused. Runtime ownership references the explicit epoch, never an etcd
revision: an etcd transaction cannot encode its own newly assigned commit
revision. Allocation fails closed rather than wrapping when the unsigned
64-bit value is exhausted.

The sole durable RuntimeOwnership record is UTF-8 RFC 8785 canonical JSON,
rejects unknown/duplicate members and every JSON numeric value, and is capped at
65,536 bytes before allocation. Every integer, UID, GID, device, inode, mount
id, handle, port, revision, PID, start tick, generation and counter is a
canonical unsigned base-10 string with no leading zero except `"0"`. Product
identities use their authoritative prefixed grammars: `agt_`, `run_`, `tnt_`,
`task_`, `op_`, and `step_` followed by a strict uppercase 26-character
Crockford ULID; `prj_` uses the same uppercase body where present in scope.
Lowercase canonical UUID is used only where this contract explicitly says
journal, seal, acknowledgement, or boot id. Digests are
`sha256:` followed by 64 lowercase hexadecimal characters. `ownership_nonce`
is 32 random bytes encoded as 64 lowercase hexadecimal characters. Binary
fields are unpadded base64url. Managed names and paths are ASCII and validated
by their resource-specific grammar.

Its exact top-level schema is:

```text
{
  "schema":"groundplane.runtime-ownership/v1",
  "agent_id":<canonical-agent-id>,
  "scope":{"tenant_id":<canonical-tenant-id>,"project_id":<canonical-project-id-or-null>},
  "runner_id":<canonical-runner-id>,
  "runtime_epoch":<uint-string>,
  "materialize_task_id":<canonical-task-id>,
  "ownership_nonce":<64-lowerhex>,
  "materialize_request_digest":<digest>,
  "boot_id":<lowercase-UUID>,
  "resources":[<ResourceOwnership>...],
  "host_control":{"image_digest":<digest>,"transcript_digest":<digest>}
}
```

`resources` is unique by `effect_id` and sorted in canonical materialization
order. One item is exactly
`{effect_id,kind,request_digest,identity}`; `kind` selects one closed Identity
variant. Helpers never appear as resources and must be removed before seal;
their complete transcript contributes through `transcript_digest`.

Closed reusable identity records are:

```text
ProcessIdentity = {pid,start_ticks,exe_device,exe_inode,exe_sha256,
  cgroup_path,pidns,mntns,userns,netns,uid_map_sha256,gid_map_sha256}
MountIdentity = {source_path,source_device,source_inode,source_mount_id,
  target_path,target_device,target_inode,target_mount_id,mount_type,read_only,
  propagation,create_mountpoint,non_recursive}
PathIdentity = {path,device,inode,mount_id,uid,gid,mode,nlink,size,digest,
  access_acl_digest,default_acl_digest,capability_digest}
```

Closed `Identity` variants are:

- `slot {allocation_id,index}`;
- `linux_group {name,gid,claim_digest}`;
- `linux_user {name,uid,primary_gid,home,claim_digest}`;
- `home|rootless_data_root {path,device,inode,mount_id,uid,gid,mode,
  marker_digest,claim_digest}`;
- `subuid|subgid {user,start,count,claim_digest}`;
- `login_linger {user,marker_path,device,inode,mount_id,claim_digest}`;
- `host_claim {target_effect_id,path,device,inode,mount_id,uid,gid,mode,
  nlink,size,digest}`;
- `owned_path {path_kind,path,device,inode,mount_id,uid,gid,mode,nlink,
  size,digest,access_acl_digest,default_acl_digest,capability_digest}`;
- `systemd_user_unit {user,uid,gid,unit_name,fragment_path,fragment_device,
  fragment_inode,fragment_uid,fragment_gid,fragment_mode,fragment_nlink,
  fragment_size,fragment_digest,semantic_digest,claim_digest,systemd_version,
  manager_boot_id,load_state,unit_file_state,need_daemon_reload,transient,
  source_path,drop_in_paths,aliases,effective_semantics,active_state,sub_state,
  main_pid,main_pid_start_ticks,control_group,limit_nofile_soft,
  limit_nofile_hard,limit_nproc_soft,limit_nproc_hard,tasks_max,
  ancestor_pids,effective_pids_max}`;
- `rootful_network_runtime {docker_id,name,driver,create_request_digest,
  normalized_inspect_digest,labels_digest,bridge_name,bridge_ifindex,subnet,
  ip_range,gateway,endpoint_container_id,endpoint_id,endpoint_ipv4,
  endpoint_gateway}`;
- `nft_policy {family,table,generation,source_digest,policy_digest,set,
  chains,rules}`, where `chains` has exactly three typed identities and `rules`
  has exactly twelve handle/comment/expression identities;
- `resolver_view {scope,process,logical_target,mount,file_digest,file_mode,
  file_size}`, where scope is `rootful|rootless`;
- `proxy_runtime {process,unit_effect_id,config_digest,socket_effect_id}`;
- `rootless_daemon_runtime {daemon_id,engine_version,api_version,architecture,
  rootless,rootlesskit_process,dockerd_process,data_root_path,
  daemon_config_digest,resolver_effect_id,raw_socket_effect_id,
  proxy_alias_mount,state_dir_path,state_dir_device,state_dir_inode,
  state_dir_uid,state_dir_gid,state_dir_mode,state_dir_entries_digest}`;
- `runner_container {docker_id,name,image_repodigest,image_id,
  create_request_digest,config_digest,host_config_digest,networking_digest,
  labels_digest,registration_document_digest,process,endpoint,
  resolver_effect_id,mounts}` where `mounts` has exactly four
  `MountIdentity` values;
- `unix_socket {socket_kind,path,device,inode,mount_id,uid,gid,mode,
  listener_process_effect_id}` where socket kind is `raw_docker|proxy`.

`owned_path.path_kind` is closed to the slot marker, `S`, `S/control`, `R`,
`W`, `E`, `T`, the cleanup roots, the two unit fragments, `runtime.env`,
`daemon.json`, proxy config, both resolver sources below `S/control`, and the
registration document. It also includes exact registration-artifact kinds
`runner_dot_runner`, `runner_credentials`, and `runner_credentials_rsaparams`
for `R/.runner`, `R/.credentials`, and `R/.credentials_rsaparams`; they are
mandatory resources in every sealed Ready ownership after configuration and
are eligible for transfer only under `replace_runtime`. A claim is represented only by `host_claim`, never by
`owned_path`. The claim path is
`S/control/claims/<K>.json`, where `K` is lowercase hex SHA-256 of
`"groundplane.host-claim-path.v1" || NUL || UTF8(target_effect_id)`. Claim
bytes are no-LF RFC 8785 bytes exactly
`{schema:"groundplane.host-claim/v1",runner_id,runtime_epoch,
ownership_nonce,target_effect_id,intent_nonce,request_digest}`. The file is a
root-owned regular one-link `0444` file with no ACL, default ACL, file
capability, symlink, hardlink, or mount crossing.

A valid sealed record has at most 128 resources and 512 managed paths, exactly
two unit resources, two socket resources and two resolver views, and at most
one rootful network runtime, Runner container, proxy runtime, rootless daemon
runtime, and nft policy. The nft policy has exactly three chains and twelve
rules and the Runner has exactly four mounts. Release generation serializes a
maximum fixture and fails if the canonical record exceeds 65,536 bytes.

The lifecycle container id
 and `runner_container.docker_id` are byte-identical.
Controller seal atomically binds the lifecycle id and these exact bytes at the
already allocated epoch. Replay accepts only byte-identical ownership. Cleanup
freezes the exact raw ownership bytes/digest/revision and can act only on the
recorded identities; names alone never authorize a mutation. Controller deletes ownership only after journaled exact absence proof and durable
cleanup receipt. The sole exception is old replace_runtime ownership, which may
be tombstoned without absence of the seven transferred PathIdentity resources,
and with its exact paired retained `host_claim` resources replaced rather than
merely absent, only in the same atomic transaction that consumes exact
ReconcileAuthorization and transfer and accepts byte-identical E+1 path
ownership plus the exact replacement claims. Every other resource must be
absent. Failed cleanup retains ownership and all claims. No earlier narrow
field list, 16,384-byte cap, JSON-number identity, or broad compatibility schema
exists.

Every schema-one Runner persistence decoder otherwise retains the 64 KiB
predecode bound, canonical UTC RFC3339Nano `Z` timestamps, UTF-8/no-BOM,
no-trailing-data and RFC 8785 byte-round-trip rules. JSON null, booleans,
strings, arrays and objects are allowed as stated; floats, exponent spellings,
negative zero, NaN and infinity are forbidden.

The exact allocation remains internal and contains `slot`, `host_uid`,
`subuid_start`, `subuid_count`, `subgid_start`, `subgid_count`, and
`network_cidr`. None of the image, container, network, UID, subordinate-id, or
local-path fields is returned by the public Runner API.

There is no operator-supplied GitHub Runner name. The internal id is exactly
`run_` plus a strict uppercase 26-character Crockford ULID. The GitHub-visible
name is derived everywhere as `gp-` plus the ASCII-lowercase transform of that
same uppercase tail. Every observed external name must be byte-equal to that
derivation. It is not independently persisted, indexed, renamed, or accepted
in a request. This makes the external name deterministic, bounded,
collision-free, and recoverable from durable identity.

The shared Runner slug grammar is 1 to 63 lowercase ASCII bytes containing
only `a-z`, `0-9`, and hyphen. A slug begins and ends with an alphanumeric byte
and contains no consecutive hyphens. Slash, NUL, Unicode, uppercase,
underscore, and every other byte are rejected rather than normalized.

`slug` is the mutable human label required by the repository-wide identity
rule. It uses the shared slug validator, is unique across every direct and
Project-owned Runner charged to the same Tenant, and is indexed independently
of ownership. Create requires it. `PATCH /runners/{id}` changes only `slug`
under the Tenant-scoped uniqueness fence. Slug changes never re-register the
Runner, change `github_url`, change the derived GitHub name, or start a Task.
PATCH is rejected with `state.conflict` while deletion or any other Runner
mutation owns the active-operation fence.

Slug PATCH requires `Idempotency-Key` even though it is synchronous. Its atomic
transaction changes the slug/index and stores a replay target containing the
exact successful HTTP status, content type, and serialized Runner response
bytes. Same key and byte-equivalent canonical `{slug}` input returns those
stored bytes without reading or reserializing the current Runner and without
creating a Task. Same key with a different canonical body is
`idempotency.conflict`. The replay target is retained for the shared protected
mutation replay window and remains replayable after a later slug edit or
Runner removal.

### Write-before-mutation ownership journal

The Agent owns one revision-chain directory per active Runner operation:

```text
/var/lib/groundplane/agent/runner-ownership/v1/<runner-id>/<runtime-epoch>/<task-id>.<ownership-nonce>/
```

The directory is `0700`; immutable 20-digit revision JSON files are `0600` and
`HEAD` contains `{revision,digest}`. Revision zero has `revision="0"` and
`predecessor_sha256=null`; every later revision increments by one and binds
SHA-256 of the exact prior bytes. Each revision is RFC 8785 canonical JSON,
rejects unknown/duplicate members and JSON numbers, and is at most 65,536
bytes. One operation has at most 128 effects and 32,768 revisions.

Commit uses a same-directory exclusive temporary, write plus `fdatasync`,
rename without replacement, and directory `fsync`; only then does it
atomically replace and fsync `HEAD`. Recovery ignores stale HEAD and temps and
selects the longest unique contiguous predecessor chain. A fork, malformed
committed revision, bad predecessor, or a gap followed by a committed later
revision terminalizes `journal_corrupt` without mutation.

One revision is exactly:

```text
{
 "schema":"groundplane.runner-journal/v1",
 "journal_id":<internal-lowercase-UUID>,
 "runner_id":<RunnerID>,"runtime_epoch":<uint-string>,"task_id":<TaskID>,
 "ownership_nonce":<64-lowerhex>,"operation":"materialize"|"cleanup",
 "revision":<uint-string>,"predecessor_sha256":<digest-or-null>,
 "created_at":<RFC3339Nano-Z>,"updated_at":<RFC3339Nano-Z>,
 "origin_boot_id":<lowercase-UUID>,"phase":<Phase>,
 "payload":{"authorization":<Authorization>,"requested":<RequestedInputs>,
   "budgets":{"operation":<Budget>,"abort":<Budget>},
   "effects":[<Effect>...],"seal":<Seal-or-null>,
   "receipt":<DeleteReceipt-or-null>,"abort_receipt":<AbortReceiptState-or-null>,
   "terminal":<Terminal-or-null>}
}
```

`Authorization` is exactly
`{controller_task_id,task_request_digest,authority_generation,confirmed_at,
valid_until,confirmation_digest}`. Generation and all counters are uint
strings; times are RFC3339Nano `Z`. It contains no bearer token. A mutating
call is legal only over a live authenticated Controller channel whose current
confirmation matches these bytes, is unexpired, and was persisted in the
issued revision.

`RequestedInputs` is exactly
`{scope,agent_id,runner_id,runtime_epoch,operation,cleanup_disposition,
transfer_authorization_digest,transfer_record_digest,resource_requests}`. `cleanup_disposition` is null iff materialize and otherwise
`delete_runner|replace_runtime`. One ResourceRequest is exactly
`{sequence,effect_id,kind,operation,protocol_request_b64,protocol_wire_sha256,
request_digest,intent_nonce,expected_name,rollback_template_b64,
rollback_template_digest}`. Operation is `ensure|remove|observe|transfer_retain|transfer_adopt`; transfer operations are legal only for the seven exact transfer effects.
`protocol_wire_sha256` is optional full-wire corruption evidence.
`request_digest` is the semantic request digest; for HostControl it is exactly
`sha256:` plus lowercase hex field 15 after deterministic-wire and self-hash
validation. A rollback template is null only when the forward effect cannot
create durable state. Otherwise it is a predeclared closed HostControl, Docker,
or slot removal request whose only substitutions are tagged fields from that
effect's later bound Identity. The Task digest covers all RequestedInputs,
including rollback templates. No input is reconstructed from host state.

Transfer digest presence is closed: ordinary materialize and `delete_runner`
cleanup require both transfer digests null; `replace_runtime` cleanup requires
`transfer_authorization_digest` nonnull and `transfer_record_digest` null; its
E+1 materialize requires both nonnull. Every other combination rejects before
mutation.

`BudgetKind` is exactly `docker_mutation|slot_mutation|helper_launch|
host_target_mutation`. A `Budget` is exactly
`{limit_ms,spent_ms,reservation}` where reservation is null or
`{dispatch_id,kind,maximum_ms}`. Operation limit is `900000`; abort limit is
`600000` and begins at zero. Before a mutating Docker/slot/host call or helper
launch, the Agent persists issued state/counter plus reservation and requires
`spent_ms+maximum_ms<=limit_ms`. On return it adds ceil monotonic elapsed
milliseconds capped at maximum and clears reservation in the outcome revision;
a crash charges the full reservation. Already-available direct rootful Docker
List/Inspect/Wait reads use fixed process-local count/time bounds, create no
reservation or poll revision, consume no active budget, grant no authority, and
cannot trigger a mutation. Time without authenticated authority consumes zero
and permits no mutation or helper/observer launch. Reconnect may advance
authority generation but never resets requests, counters, or spent budget.


#### Effects, rollback, and transitions

Every Effect repeats immutable
`{sequence,effect_id,kind,operation,request_digest,intent_nonce}`, equals its
ResourceRequest, and includes `active_leg:"forward"|"rollback"|"none"` plus
`rollback:{state,resolved_request_b64,resolved_request_digest,
remove_dispatches,absence_evidence_digest}`. Rollback starts `pending` for a
durable forward effect but is dormant while `active_leg=forward`; dormant
pending rollback is excluded from the one-active-effect invariant. At most one
phase-selected leg is active. Normal completion sets `active_leg=none`. Abort
activates exactly one rollback leg at a time in canonical cleanup order and
returns it to none before selecting the next.


- SlotEffect adds state, reserve/release dispatches and nullable SlotIdentity.
  Ensure is `planned -> reserve_issued -> bound -> complete`; cleanup initializes
  bound and uses `bound -> release_issued -> released -> complete`.
- NetworkEffect adds create/remove dispatches and nullable NetworkIdentity.
  Materialize is `planned -> create_issued -> id_bound -> complete`; cleanup is
  `id_bound -> remove_issued -> removed -> complete`. It has no Start.
- ContainerEffect adds create/start/stop/remove dispatches and nullable
  RunnerContainerIdentity. Materialize is
  `planned -> create_issued -> id_bound -> start_issued -> running -> complete`.
  Cleanup is `running -> stop_issued -> stopped -> remove_issued -> removed ->
  complete`, or begins removal from sealed `id_bound|stopped`.
- HostEffect adds state, target/probe dispatches, claim digest, nullable bound,
  `helper_current`, `helpers_completed`, and `helper_transcript_digest`. Ensure
  is `planned -> mutation_issued -> desired_bound -> complete`; removal begins
  at sealed desired and uses
  `desired_bound -> mutation_issued -> absence_proven -> complete`.

A HelperAttempt is exactly
`{ordinal,role,helper_nonce,expected_name,request_digest,create_digest,state,
create_dispatches,attach_dispatches,start_dispatches,frame_dispatches,
wait_dispatches,remove_dispatches,container_id,response_digest,exit_code}`.
Normal graph is
`prepared -> create_issued -> id_bound -> attach_issued -> start_issued ->
frame_issued -> response_bound -> wait_issued -> terminal_bound ->
remove_issued -> removed`. Pre-frame retirement uses distinct states and always
has `response_digest=null`:

```text
id_bound|attach_issued|start_issued -> preframe_retire_issued
preframe_retire_issued -> remove_issued -> removed
start_issued -> preframe_retire_issued -> wait_issued ->
  preframe_terminal_bound -> remove_issued -> removed
```

The direct removal branch requires full-ID Inspect Created, Running=false,
Pid=0. The Wait branch handles Running or Exited after Start; its
`preframe_terminal_bound` requires Running=false/Pid=0 and no response, whereas
normal `terminal_bound` requires the already-bound valid response digest.
Created-at-start never passes through response-requiring terminal_bound. No
other edge exists. Bounds per attempt remain Create3, Attach2, Start2, Frame1,
Wait streams2, Remove3 and at most17 durable revisions.


Mutators are at most 90 seconds, observers 30 seconds, and final UID proof 120
seconds.

The Agent persists `attach_issued` before Attach. A definitive pre-hijack HTTP
failure may retry; hijack ambiguity retires the attempt because a connection
is process-local, never a durable identity. Start is legal only while the same
Agent process holds the live attach and `start_issued` is durable. Recovery of
a Created helper at attach/start removes it and creates a fresh attempt. A
Running/Exited helper at attach is conflict; at start it is allowed to reach
the 15-second frame deadline and is then retired without observer because
`frame_issued` was never durable.

After Start, the same revision persists `frame_issued`, HostEffect
`mutation_issued`, and target-dispatch increment before writing any request
byte. From there until a valid response is bound the target result is unknown:
never reattach or resend. Wait/Inspect, remove and prove 404, then run a separate
journaled observer. A response is validated and synced before `response_bound`.
Wait is issued once before its stream; one stream retry is allowed, followed by
at most twenty full-ID Inspect polls at least one second apart. Terminal bind
requires `Running=false`, `Pid=0`, exit zero, and the already bound response.
Nonzero exit or missing/invalid response takes unknown-result observation.

Docker attach uses `Tty=false`; Docker stdcopy framing is outside the
application record and is decoded before stdout/stderr interpretation. The
application record is exactly `uint32be(length) || deterministic protobuf`,
where length is 1..65,536 and counts only protobuf bytes. The Agent writes
exactly one request record to helper stdin then CloseWrite; the helper requires
EOF immediately after it. The helper writes exactly one response record to
stdout then EOF; the Agent rejects zero length, oversize, incomplete or trailing
bytes, a second record, or stdout bytes outside that record. The helper must
receive the complete request within 15 seconds. Invalid or incomplete input
mutates nothing, emits no response, and exits 64. Every valid operation result,
including CONFLICT or FAILED, emits exactly one valid typed response and exits
zero. Docker stderr is diagnostic-only, never an application record, and is
capped at 8,192 bytes. Each HostEffect has at most two mutation and two observation attempts on the forward leg and the same limits on rollback: at most four per leg and eight lifetime,
strictly alternating after a missing mutation result. Observer loss may consume
the second observer. The per-effect transcript empty seed is
`sha256:SHA256("groundplane.helper-transcript.v1" || NUL || "empty")`. When an
attempt reaches removed, fold raw prior digest bytes and canonical attempt bytes
as `SHA256("groundplane.helper-transcript.v1" || NUL || prior_raw || NUL ||
attempt_bytes)`, increment completed, and clear current. RuntimeOwnership
host-control transcript starts
`SHA256("groundplane.host-control-transcript.v1" || NUL || "empty")` and folds
numeric-sequence-sorted effects as
`SHA256(domain || NUL || prior_raw || NUL || effect_id || NUL ||
effect_transcript_raw)`. Empty effects participate with the empty seed.

Every host target has a separate preceding claim.ensure HostEffect. Cleanup
removes target then claim. Group, user without home/subid, home, subuid, subgid,
rootless data root, linger, each unit, and nft policy are separate effects.
There is no compound account effect.

Materialize top phases are
`prepared -> materializing -> ownership_ready -> seal_submit_issued -> sealed ->
ownership_commit_issued -> ownership_committed -> task_ack_issued -> complete`.
Cleanup phases are
`prepared -> cleaning -> receipt_ready -> receipt_submit_issued ->
receipt_acked -> ownership_delete_issued -> ownership_deleted ->
task_ack_issued -> complete`.

Abort reuses each original Effect rollback in canonical cleanup order; it never
synthesizes a new Effect. Its top graph is
`prepared|materializing -> abort_requested -> abort_resolving ->
abort_cleaning -> abort_receipt_ready -> abort_ack_issued -> abort_acked ->
terminal`. A forward effect never issued transitions rollback
`pending -> absence_proven -> complete` using evidence of its durable forward
state. A bound effect uses `pending -> bound`; an issued unknown uses
`pending -> resolving -> absence_proven|bound|conflict`; bound then uses
`bound -> removal_issued -> absence_proven -> complete`. Resolved removal bytes
and digest are persisted before `removal_issued`. Abort never redispatches a
forward Create, Start, or ensure.

During `abort_resolving`, Docker and slot issued states use exact 0/1/>1
label/ledger discovery. A host mutation retires any exact helper, then observes:
prestate means absence, desired state plus exact claim binds, and partial or
foreign state conflicts. A discovered helper is bound and retired before target
observation. Only after every issued state resolves may abort clean. Failure
edges from resolving/cleaning enter terminal with
`ownership_conflict|ambiguous_discovery|helper_result_invalid|
dispatch_budget_exhausted|deadline_exhausted|
boot_changed_preseal_cleanup_conflict`, retain journal/claims, and emit no
success receipt.

#### Unknown results, dispatch bounds, seal, and abort receipt

Every Network, Runner, helper, proxy-created container, volume, and network
Create persists its exact expected name, nonce, labels, request/create/image
and normalized configuration digests before dispatch. Discovery filters every
reserved label, inspects full IDs, and uses 0/1/>1: zero permits the resource's
byte-identical retry if name is free; one adopts only exact name/labels/config;
more or mismatch conflicts. Names never authorize adoption. Lost Start, Stop,
Remove and slot writes use full bound identity and exact inspect/ledger rules;
no same-name replacement is touched.

Fixed dispatch bounds are Network Create 2/Remove 2/no Start; Runner Create 3,
Start 2, Stop 2, Remove 3; Slot reserve/release 2; helper bounds above; seal,
receipt, abort ACK, and Task ACK 3. Polling never authorizes mutation.

After all effects and helper removals, the Agent computes RuntimeOwnership once,
syncs `ownership_ready`, persists submit-issued, and uses Controller seal
idempotency `(runner_id,runtime_epoch,task_id,ownership_digest)` with 0/1/>1
semantics. It binds the response before atomically publishing the identical
local record, ACKs only after commit, then renames the journal directory to
`.gc.<journal_id>`, fsyncs, deletes the renamed tree, and fsyncs. Cleanup
revision zero copies the exact seal. Receipt idempotency is
`(runner_id,runtime_epoch,cleanup_task_id,ownership_digest,
removed_effects_digest)`. Controller receipt precedes local ownership tombstone,
delete, Task ACK, and journal GC. Incomplete cleanup emits no success receipt.

The canonical no-LF materialize abort receipt is at most 16,384 bytes and is
exactly:

```text
{"schema":"groundplane.runner-materialize-abort-receipt/v1",
 "runner_id":RunnerID,"task_id":TaskID,"operation_id":OperationID,
 "runtime_epoch":uint-string,"ownership_nonce":64lowerhex,
 "plan_sha256":digest,"reason":AbortReason,
 "bound_resource_set_sha256":digest,"removed_effects_sha256":digest,
 "retained_effect_ids":[effect-id...],"completed_at":RFC3339Nano-Z,
 "receipt_sha256":digest}
```

AbortReason is exactly `boot_changed|task_cancelled|
materialize_deadline_exhausted|materialize_dispatch_exhausted|
runtime_start_failed|registration_token_required|seal_rejected`. The bound
digest is domain `groundplane.abort-bound.v1` over sequence-sorted
`[{sequence,effect_id,kind,request_digest,identity}]`; the removed digest uses
`groundplane.abort-removed.v1` over
`[{sequence,effect_id,resolved_request_digest,absence_evidence_digest}]`.
Self-digest domain is `groundplane.abort-receipt.v1` over the object with
`receipt_sha256:null`. `retained_effect_ids` must be empty to submit.

`absence_evidence_digest` is
`sha256:SHA256("groundplane.abort-absence-evidence.v1" || NUL || canonical
evidence bytes)`. Evidence is exactly one tagged union:

```text
journal_never_issued {kind,forward_state,forward_revision,predecessor_digest}
docker_label_discovery_zero {kind,resource_kind,label_query_digest,
  expected_name,expected_name_free_evidence_digest,create_request_digest,
  discovery_started_at,discovery_ended_at,poll_count,result_count:"0"}
docker_full_id_404 {kind,resource_kind,docker_id,immutable_identity_digest,
  inspect_status:"404",observed_at}
slot_ledger_absent {kind,allocation_owner_digest,intent_nonce,
  request_digest,ledger_revision,observed_at}
host_observer_absent {kind,observer_response_digest,target_request_digest,
  claim_presence:"absent"|"exact_removed",observed_at}
helper_full_id_404 {kind,container_id,helper_create_digest,observed_at}
```

Unknown/extra members reject; all identities equal journal-bound values. `resource_kind` is `network|runner_container|helper_container`; the
label query digest covers the full persisted selector, expected-name-free proof
uses the same bounded Docker inventory, interval is positive and <=10 seconds,
and poll_count is 1..20. This is the only absence evidence for create_issued
without a returned full ID.
`retained_effect_ids` and every bound/removed digest input array are strictly
numeric-sequence ordered and unique. Abort ACK recovery queries Controller by
exact `(task_id,runtime_epoch,receipt_sha256)`: zero redispatches byte-identical
ACK within max3, exactly one response must match ACK id/receipt/response digest
and is adopted, and more than one or mismatch terminalizes conflict. ACK name or
Task state alone never authorizes adoption.


Payload abort state is null or
`{receipt_b64,receipt_digest,ack_dispatches,controller_ack_id,
controller_response_digest}`. Controller ACK idempotency is
`(task_id,runtime_epoch,receipt_sha256)`. After ACK the Agent persists
`abort_acked`, then terminal receipt evidence. Only ACKed success is GC-eligible;
unACKed or conflict terminal journals are retained. Terminal codes additionally
include each AbortReason, `boot_changed_preseal_cleaned`, and the corruption,
conflict, identity, dispatch, deadline, runtime, token, seal, and receipt codes
above. Phase is terminal iff Terminal is non-null.

#### Restart, reboot, and bounded storage

Startup locks the Runner and resolves its unique chain. Same-boot restart
observes every issued state before retry. Without current authenticated
Controller authority the Agent performs no external mutation, including
cleanup or observer launch; it may validate and persist bounded local/direct
Docker observations only.

Boot change pre-seal enters abort and waits for current authority. Boot change
during cleanup resumes only sealed/bound removals after authority. A sealed boot
mismatch takes `ready -> reconciling` and never restarts the old epoch.

Controller first publishes canonical `ReconcileAuthorizationV1` exactly
`{schema:"groundplane.runner-reconcile-authorization/v1",runner_id,
old_runtime_epoch,old_ownership_digest,old_boot_id,new_boot_id,cleanup_task_id,
cleanup_request_digest,retained_paths,new_materialize_task_id,
new_runtime_epoch,new_ownership_nonce,registration_invalidation_receipt_digest,
controller_authorization_id,retained_claims,claim_replacements,
authorization_digest}`. `retained_paths` is
path-kind sorted and exactly seven `{path_kind,identity:PathIdentity}` entries
copied byte-identically from old sealed ownership: S, S/control, R,
`S/control/registration-v1.json`, `R/.runner`, `R/.credentials`, and
`R/.credentials_rsaparams`. `retained_claims` is target-effect-id sorted and
contains every old sealed `host_claim` whose target is one of those seven paths,
and no other claim. `claim_replacements` has the same order/cardinality and each
entry is exactly `{path_kind,old_claim_identity,new_target_effect_id,
new_intent_nonce,new_request_digest,new_claim_digest}`; `new_claim_digest` is
the digest of the canonical E+1 claim bytes. No other path or claim transfers.
Authorization digest is
`sha256:SHA256("groundplane.runner-reconcile-authorization.v1" || NUL ||
RFC8785(the same object with authorization_digest:null))`. Current authenticated
Controller binds the exact bytes before host mutation.

RequestedInputs uses the exact transfer-digest presence rules above.
ResourceRequest operation adds `transfer_retain` and `transfer_adopt`. A
TransferEffect is exactly
`{sequence,effect_id,kind:"transfer_path",operation,request_digest,intent_nonce,
active_leg,state,bound_path_identity,rollback}` where rollback is exactly
`{state:"not_applicable",resolved_request_b64:null,
resolved_request_digest:null,remove_dispatches:"0",
absence_evidence_digest:null}`. Cleanup graph is
`desired_bound -> transfer_retain_issued -> retained_bound -> complete`; E+1
graph is `planned -> transfer_adopt_issued -> identity_bound -> adopted ->
complete`. It launches no helper, accepts only a byte-identical sealed
PathIdentity from the transfer record, and never reopens a pathname. These seven
are the only disposition-specific non-removal effects. Cleanup removes W/E/T,
data root, units, sockets, processes, Network/nft and every nonretained claim,
but does not remove R or its retained ancestors/leaves/claims.

Retained claims are not silently adopted because their bytes bind the old
epoch. After the transfer record is durable, E+1 RequestedInputs predeclare, in
target-effect-id order, one ordinary `claim.remove` HostEffect for each exact
old retained claim followed immediately by one ordinary `claim.ensure`
HostEffect for its authorized replacement bytes. The transfer record owns the
retained path throughout the absence window; issued/bound helper rules apply,
old-claim absence is proven before ensure, and the new full `host_claim`
Identity is journal-bound before the next effect. No claim pathname is reopened
for adoption. E+1 RuntimeOwnership contains every new claim identity and no old
claim identity.

Old RuntimeOwnership remains authoritative through replace_runtime cleanup.
After cleanup receipt ACK, Controller atomically seals transfer object exactly
`{schema:"groundplane.runtime-ownership-transfer/v1",runner_id,
old_runtime_epoch,old_ownership_digest,new_runtime_epoch,
new_materialize_task_id,new_ownership_nonce,reconcile_authorization_digest,
retained_paths,retained_claims,claim_replacements,cleanup_receipt_digest,
transfer_id,transfer_digest}` and marks old
ownership `transfer_pending`; it does not delete it. Transfer digest is
`sha256:SHA256("groundplane.runtime-ownership-transfer.v1" || NUL || RFC8785(the
same object with transfer_digest:null))`. Old ownership plus transfer is the
sole interim authority and binds every retained inode, old claim, and authorized
replacement-claim request.

Only then may E+1 materialize. Exact registration plus Controller invalidation
proof uses a fresh independently one-consume config token and no GitHub
configure. Missing/mismatched proof terminalizes `registration_token_required`
and retains old ownership/transfer until an explicit token-bearing plan cleans
or supersedes it. Controller accepts E+1 seal only if its seven PathIdentity
resources are byte-identical to the consumed transfer. In that single Controller
transaction it consumes ReconcileAuthorization and transfer, accepts E+1
ownership, and tombstones/deletes old ownership. This is the sole exception to
absence-before-old-ownership-delete; the transaction requires every old retained
claim absent and its corresponding E+1 replacement claim byte-identical to
`claim_replacements`, while every nontransferred old resource still
requires exact absence. Crash state is exactly old ownership, old+transfer, or
new ownership, never an unowned path. There is no in-place reseal or old
PID/socket/container replay.



Admission serializes revision zero plus maximum forms of all identities,
current helper, receipt, seal and terminal and rejects before mutation if a
revision exceeds65,536. One attempt has ten durable state transitions plus at
most two extra Create, one Attach, one Start, one Wait-stream and two Remove
retries: 17 revisions. Direct Inspect polls create no revisions. Four helpers
per leg/eight lifetime plus six outer transitions gives
`W_host=8*17+6=142`. Every other effect is lower; `128*142=18,176`. Reserve256
gives18,432<32,768, leaving14,336 safety revisions. Each revision proves
`current_revision + worst_remaining + 256 <= 32768`; reserve is unavailable to
forward work. A one-leg cleanup is <=`128*(4*17+6)+256=9,728`. Worst operation
storage is below2GiB and ACKed GC releases it.


### Canonical GitHub locator

`github_url` is the persisted external scope locator. There is no separate
persisted `scope`, `target`, organization, owner, or repository field.

A Tenant-owned Runner requires exactly:

```text
https://github.com/<org>
```

A Project-owned Runner requires exactly:

```text
https://github.com/<org>/<repo>
```

URL validation is ordered so URL-library normalization cannot hide rejected
input. The boundary first rejects every ASCII control character, non-ASCII
byte, backslash, and percent sign; percent escapes are never decoded or
accepted. It then requires HTTPS, host `github.com`, no user information, no
explicit port, no query, and no fragment. Zero or one terminal slash is
allowed; more than one is rejected, and the one allowed terminal slash is
removed before splitting or validating path segments. Empty and repeated
segments and any additional segment are rejected.

The organization segment is 1 to 39 ASCII characters, contains only
alphanumeric characters and hyphens, begins and ends with an alphanumeric
character, and contains no consecutive hyphens. The repository segment is 1 to
100 ASCII characters containing only alphanumeric characters, dots, hyphens,
and underscores. A repository may begin with a dot, but it may not be `.` or
`..`, end with a dot, or end case-insensitively with `.git`. A Tenant owner has
only the organization segment; a Project owner has organization and repository
segments. After those checks, scheme, host, organization, and repository are
lowercased for canonical persistence. Changing the GitHub organization or
repository requires local removal and creation of a new Runner.

The registration token is expected to authorize the supplied URL, but
Groundplane does not call GitHub to preflight it. An authorization or scope
mismatch is a failed creation Task.

### Custom labels

Create accepts zero to 16 custom labels. Each label is 1 to 64 lowercase ASCII
characters matching `^[a-z0-9][a-z0-9._-]{0,63}$`; comma is forbidden. Labels
are case-insensitively unique, sorted bytewise, and stored in that canonical
order. The GitHub default labels `self-hosted`, `linux`, `x64`, and
`arm64` are rejected as custom labels because the Runner application supplies
the appropriate defaults. Groundplane does not pass `--no-default-labels`.

### Immutable Runner image and disabled updates
The MVP Runner platform is exactly `linux/arm64`. A release publishes one
single-platform OCI image index containing exactly one `linux/arm64` child; it
does not publish an `amd64` child and does not describe the single-platform
index as multiarchitecture. The OCI repository is a mandatory explicit release
build input with no default. It is 1 to 255 lowercase ASCII bytes, follows the
OCI Distribution repository-name grammar, and contains no scheme, tag, digest,
query, or fragment. The release build freezes that repository and the exact
child and index digests. There is no `runner.image` startup setting,
environment override, API field, CLI flag, or Console field.

The upstream Runner input is exactly:

| Field | Value |
|---|---|
| version | `2.336.0` |
| source commit | `98aabcd429c4e8402406c56ce2d26387fed3b9ce` |
| platform | `linux/arm64` |
| asset id | `483731306` |
| URL | `https://github.com/actions/runner/releases/download/v2.336.0/actions-runner-linux-arm64-2.336.0.tar.gz` |
| size | `138824064` bytes |
| SHA-256 | `58b758e420b87093fbd4bfddd368074960053e2f1388f01848c82624b90f27d1` |

Fetching accepts either an immediate HTTPS `200`, or exactly one HTTPS `302`
whose `Location` host is `release-assets.githubusercontent.com`, has no user
information, uses no explicit port other than `443`, and does not downgrade
scheme. The redirect target must return `200`; another redirect, another host,
a non-HTTPS URL, a length other than `138824064`, or a digest mismatch fails
the build. The image owns the single controlled entrypoint and the SPKI-pinned Controller pull protocol. The entrypoint passes `--disableupdate`; acceptance proves local `DisableUpdate=true`, advertisement to GitHub, exact server echo, and persisted `.runner` state. This does not claim the Listener independently rejects refresh messages. Failure of those proofs fails creation.

#### Canonical release contract

The release build generates `release/runner-contract-v1.json`; humans do not
hand-edit it. `release/runner-contract-v1.schema.json` is a checked-in JSON
Schema 2020-12 schema that rejects unknown fields and encodes every constant,
grammar, cardinality, and bound below. The generated file is UTF-8 without a
BOM, contains no insignificant whitespace, and is the RFC 8785 canonical form
of this exact top-level object:

```json
{
  "oci_repository": "<explicit-release-input>",
  "runner_contract": {
    "actions_runner": {
      "architecture": "arm64",
      "asset_id": 483731306,
      "os": "linux",
      "redirect": {
        "count": 1,
        "host": "release-assets.githubusercontent.com",
        "status": 302
      },
      "sha256": "58b758e420b87093fbd4bfddd368074960053e2f1388f01848c82624b90f27d1",
      "size": 138824064,
      "source_commit": "98aabcd429c4e8402406c56ce2d26387fed3b9ce",
      "url": "https://github.com/actions/runner/releases/download/v2.336.0/actions-runner-linux-arm64-2.336.0.tar.gz",
      "version": "2.336.0"
    },
    "apt": {
      "packages": [],
      "repositories": [],
      "roots": [],
      "trust_roots": []
    },
    "base_image": {
      "child": {
        "digest": "sha256:95fa486768020359141f1318720f43e7982ef926c792891d984aef9aaf05e7ea",
        "media_type": "application/vnd.oci.image.manifest.v1+json",
        "platform": "linux/arm64/v8",
        "size": 424
      },
      "config": {
        "digest": "sha256:5b8c0c14690ed170da4e663fe0bae0d58efe59661e791296ffab28ed2113b650",
        "media_type": "application/vnd.oci.image.config.v1+json",
        "size": 2067
      },
      "index": {
        "digest": "sha256:33ceb71981b602c1a7443a53469e4dba065f7503eab3078a2d7a57a2ab987517",
        "media_type": "application/vnd.oci.image.index.v1+json",
        "size": 6688
      },
      "official_images": {
        "commit": "390134527c15b762ef9efa178cbb065886773659",
        "release": "noble-20260810",
        "source_commit": "73ecb123318a4fa4b264fae169d4773bc4c9c9c6"
      },
      "repository": "docker.io/library/ubuntu",
      "tag": "24.04"
    },
    "docker": {
      "api_version": "1.52",
      "moby_commit": "fbf3ed25f893e6ce21336f1101590e40a13934f4",
      "moby_signed_tag": "docker-v29.1.3",
      "moby_tag_object": "3ac9309249510376baefa3b747986b5eeee977f9",
      "seccomp": {
        "compact_transport_sha256": "<64-lowercase-hex>",
        "source_sha256": "01536f1d1df938ae611eba20d6349e0de7a99b6ecdee1549427a0b01b8301e28",
        "source_size": 13063
      },
      "server_version": "29.1.3"
    },
    "docker_proxy_policy": {
      "canonical_base64": "<canonical-rfc4648>",
      "path": "release/runner-docker-proxy-policy-v1.json",
      "schema": "groundplane.docker-proxy-policy/v1",
      "sha256": "<64-lowerhex>",
      "size": <positive-json-integer>
    },
    "executables": [],
    "helper_create": {
      "create_dto_schema_sha256": "<64-lowerhex>",
      "inspect_normalizer_sha256": "<64-lowerhex>",
      "proto_sha256": "<64-lowerhex>",
      "vectors_sha256": "<64-lowerhex>"
    },
    "helper_security": [
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-daemon-cleanup-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-daemon-cleanup-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-daemon-cleanup-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-daemon-cleanup-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-etc-write-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-etc-write-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-etc-write-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-etc-write-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-login1-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-login1-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-login1-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-login1-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-nft-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-nft-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-nft-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-nft-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-process-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-process-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-process-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-process-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-read-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-read-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-read-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-read-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-slot-write-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-slot-write-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-slot-write-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-slot-write-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-socket-observe-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-socket-observe-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-socket-observe-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-socket-observe-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}},
      {"apparmor":{"features_path":"<checked-in-apparmor-features-snapshot-path>","features_sha256":"<64-lowerhex>","load_name":"groundplane-hc-user-systemd-v1","parser_path":"/usr/sbin/apparmor_parser","parser_sha256":"<64-lowerhex>","source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/apparmor/groundplane-hc-user-systemd-v1","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>},"name":"groundplane-hc-user-systemd-v1","seccomp":{"canonical_bytes_base64":"<canonical-rfc4648>","canonical_sha256":"<64-lowerhex>","canonical_size":<positive-json-integer>,"source_bytes_base64":"<canonical-rfc4648>","source_path":"release/runner-security-v1/seccomp/groundplane-hc-user-systemd-v1.json","source_sha256":"<64-lowerhex>","source_size":<positive-json-integer>}}
    ],
    "platform": {
      "architecture": "arm64",
      "os": "linux",
      "ubuntu_suite": "noble"
    },
    "probe_environment": {
      "LANG": "C.UTF-8",
      "LC_ALL": "C.UTF-8",
      "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    },
    "probes": [],
    "resolvers": {
      "embedded": {
        "bytes_base64": "bmFtZXNlcnZlciAxMjcuMC4wLjExCm9wdGlvbnMgdGltZW91dDoyIGF0dGVtcHRzOjIgcm90YXRlIG5kb3RzOjAK",
        "sha256": "7b59a25a5f151203d301cc006d86988479e3f2167f0dccaf2e5b79bf6e5cb2cc",
        "size": 66
      },
      "upstream": {
        "bytes_base64": "bmFtZXNlcnZlciAxLjEuMS4xCm5hbWVzZXJ2ZXIgMS4wLjAuMQpvcHRpb25zIHRpbWVvdXQ6MiBhdHRlbXB0czoyIHJvdGF0ZQo=",
        "sha256": "d23853fdca3ed710769a32f524a19165b9e1361f05f1762befc8bec1f481677f",
        "size": 74
      }
    },
    "runtime_artifacts": [
      {"base64":"<canonical-rfc4648>","path":"<checked-in-daemon-json-template-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-docker-user-unit-template-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-helper-create-dto-schema-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-helper-create-golden-vectors-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-helper-inspect-normalizer-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-proxy-config-schema-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-proxy-user-unit-template-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-rootless-launcher-contract-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-runner-container-create-dto-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-runner-container-create-golden-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-runner-host-control-proto-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-runner-host-control-validator-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>},
      {"base64":"<canonical-rfc4648>","path":"<checked-in-runtime-env-template-path>","sha256":"<64-lowerhex>","size":<positive-json-integer>}
    ],
    "schema": "groundplane.runner-contract.v1"
  },
  "runner_contract_sha256": "<64-lowercase-hex>",
  "runner_image": {
    "child_digest": "sha256:<64-lowercase-hex>",
    "index_digest": "sha256:<64-lowercase-hex>",
    "platform": "linux/arm64"
  },
  "schema": "groundplane.runner-release.v1"
}
```

The Ubuntu source lock is registry-integrity plus official metadata only. It
makes no image signature, SLSA, or other provenance claim. The release builder
resolves `docker.io/library/ubuntu:24.04` to the exact index descriptor, proves
the `linux/arm64/v8` descriptor is the exact child above, verifies its exact
config descriptor, and uses that child digest directly in the ARM64
`FROM`; tag resolution and a second index selection during the Runner build
are forbidden.

The Moby source lock verifies signed annotated tag `docker-v29.1.3` at tag
object `3ac9309249510376baefa3b747986b5eeee977f9`, peeled commit
`fbf3ed25f893e6ce21336f1101590e40a13934f4`. The exact 13,063-byte source
`profiles/seccomp/default.json` has the recorded SHA-256. The builder parses it
as JSON with duplicate-member rejection, requires RFC 8785-compatible JSON
types, emits its RFC 8785 canonical compact bytes, and records the resulting
`compact_transport_sha256`. Runtime sends exactly
`SecurityOpt=["seccomp=" + <those compact bytes>]`; it never sends a path,
reformats the profile, or uses Engine `default` as an alias.

Empty arrays above show shape only; a publishable manifest requires every
array to satisfy the non-empty bounds below. Object member order in the file is
the RFC 8785 order shown. `runner_contract_sha256` is lowercase hexadecimal
SHA-256 over this exact byte preimage, with no newline or length prefix:

```text
groundplane.runner-contract.v1<NUL>C
```

`<NUL>` is the single byte `0x00`, and `C` is the RFC 8785 canonical UTF-8 byte
sequence of the `runner_contract` object alone. The digest field, OCI
repository, Runner image child digest, and Runner image index digest are
outside `runner_contract`. The image embeds only
`runner_contract_sha256`; therefore neither that digest nor either image digest
is in its own preimage. The base image digest, authenticated apt closure,
installed executable hashes, probes, Runner archive, and resolver facts are
known before the final image label is written and are inside the preimage.

The exact apt shapes are:

- `trust_roots`: 1 to 8 entries containing exactly `id`, `bytes_base64`,
  `size`, `sha256`, and `fingerprint`; `bytes_base64` uses RFC 4648 standard
  base64 alphabet `A-Z a-z 0-9 + /`, requires canonical `=` padding, contains
  no whitespace, line break, URL-safe character, or ignored byte, and must
  decode then re-encode byte-identically; decoded bytes are 1 to 65,536 bytes
  per entry and at most 262,144 bytes in aggregate, ids are bytewise-sorted and
  unique, `size` is the exact decoded length, SHA-256 is over the decoded bytes,
  and fingerprint is the exact full uppercase hexadecimal OpenPGP primary-key
  fingerprint derived from those same bytes;
- `repositories`: 1 to 4 entries containing exactly `id`, `uri`, `suite`,
  bytewise-sorted unique `components`, `trust_root_id`, `inrelease` with exact
  `url`, `size`, and `sha256`, and `indexes` with exact `component`, `url`,
  `size`, and `sha256`; `trust_root_id` selects exactly one manifest root;
- `roots`: 1 to 64 bytewise-sorted unique requested package names;
- `packages`: 1 to 512 entries containing exactly `name`, `version`,
  `architecture`, `repository_id`, `filename`, `size`, and `sha256`.

Repositories are sorted by `id`, indexes by `(component,url)`, and packages by
`(name,architecture,version,repository_id,filename)`, all as unsigned UTF-8
byte order. Each repository URI and artifact URL is canonical HTTPS with no
userinfo or fragment. The approved trust-root files are checked into release
source, their exact bytes are compiled into the release builder, and the
manifest copies those bytes and their derived fingerprint and SHA-256. A
repository, mirror, redirect, package, `InRelease`, or index may never supply
or replace a trust root. The build authenticates each selected `InRelease`
only against the one `trust_root_id` bound by that repository entry, verifies
every captured Packages index against `InRelease`, verifies every `.deb`
filename, size, and SHA-256 against its authenticated index, computes the
complete transitive closure for `linux/arm64` and Ubuntu `noble`, then proves
an offline install of exactly the listed closure with no additional package or
network access. Package architecture is exactly `arm64` or `all`; duplicate
package identities and an unreachable, missing, extra, or unauthenticated
`.deb` fail the build.

`executables` has 1 to 32 entries sorted by absolute `path`, each containing
exactly `path`, `size`, and `sha256`. It includes `/usr/bin/rootlesskit`,
`/usr/libexec/groundplane/runner-rootless-launch`, the exact
`/usr/bin/dockerd-rootless.sh`, Docker Engine and CLI, slirp4netns, systemd
control, and nftables executables used by the runtime. `probes` has 1 to 32
entries sorted by unique `id`, each containing exactly `id`, `argv`,
`exit_code`, `stdout_sha256`, and `stderr_sha256`; `argv` contains 1 to 64
strings and begins with an exact executable path from `executables`. Probes run
in the assembled filesystem with no inherited environment and exactly
`PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`,
`LANG=C.UTF-8`, and `LC_ALL=C.UTF-8`, as recorded by `probe_environment`. A
missing executable, mismatched byte hash, non-matching exit code, or stdout or
stderr digest mismatch fails the build or installation.

The whole generated file is at most 2 MiB after adding encoded trust roots; the
decoded trust-root aggregate is independently capped at 262,144 bytes and the
canonical manifest is rejected before allocation when it exceeds the 2 MiB
aggregate bound. Identifiers and package fields are
1 to 255 bytes, URLs and absolute paths are 1 to 4096 bytes, and probe arguments
are 0 to 4096 bytes. Strings are valid UTF-8 without NUL or ASCII controls;
fields defined as ASCII reject non-ASCII. Sizes are positive JSON integers and
all integers are at most `9007199254740991`. Bare SHA-256 values are exactly 64
lowercase hexadecimal characters and OCI digests are exactly `sha256:` plus
those 64 characters. Arrays reject duplicate semantic identities. These
bounds, all exact object members, the stated order, and every constant are
enforced before hashing; normalization is forbidden.

The repository is a release input, but the exact child and index digests are
build outputs. After pushing, the builder resolves the index, proves that it
contains exactly the one expected `linux/arm64` child and that both remote
digests equal the generated values, then seals those outputs in the outer
manifest. Publication and installation fail closed until the base image
digest, full authenticated `.deb` closure, executable hashes and probes, and
both Runner image digests are present and verified.

This introduces no Runner-manifest signing key or runtime trust store. The
release builder produces immutable canonical manifest bytes, and those exact
bytes are compiled into the trusted Controller artifact. The compiled bytes
are the only runtime authority. A file installed beside the Controller,
downloaded later, mounted into it, or otherwise supplied externally is never
read as authority and cannot override them. Controller startup validates the
compiled schema, recomputes the domain-separated contract digest, validates
the repository and both image digests, and rejects a missing, malformed,
tag-only, inconsistent, or unavailable embedded image reference.

The native Controller never permits the managed container to replace its
Runner application. A Controller release embeds one exact Runner image digest;
a failed-create retry uses the Runner's original frozen `image_ref`. There is
no Runner update endpoint, command, or Console action in the MVP. Updating an
existing Runner requires remove/create under a Groundplane release that embeds
the intended image.


Within `runner_contract`, exact RFC 8785 key order is `actions_runner`, `apt`,
`base_image`, `docker`, `docker_proxy_policy`, `executables`, `helper_create`,
`helper_security`, `platform`, `probe_environment`, `probes`, `resolvers`,
`runtime_artifacts`, `schema`. The angle-bracket array entries in the display
above are documentation metavariables expanded by release generation, not
literal publishable strings.

`helper_security` has exactly nine ASCII-name-sorted entries named
`groundplane-hc-daemon-cleanup-v1`, `groundplane-hc-etc-write-v1`,
`groundplane-hc-login1-v1`, `groundplane-hc-nft-v1`,
`groundplane-hc-process-v1`, `groundplane-hc-read-v1`,
`groundplane-hc-slot-write-v1`, `groundplane-hc-socket-observe-v1`, and
`groundplane-hc-user-systemd-v1`. An entry is exactly
`{apparmor:{features_path,features_sha256,load_name,parser_path,parser_sha256,
source_bytes_base64,source_path,source_sha256,source_size},name,seccomp:
{canonical_bytes_base64,canonical_sha256,canonical_size,source_bytes_base64,
source_path,source_sha256,source_size}}`.
Each decoded source/canonical blob is 1..262144 bytes and aggregate decoded
helper security is <=2MiB. Digest is over decoded bytes. AppArmor source has
one same-name profile with no includes/tunables/abstractions. Source paths are
`release/runner-security-v1/apparmor/<name>` and
`release/runner-security-v1/seccomp/<name>.json`; install path is
`/etc/apparmor.d/<name>`. Seccomp canonical bytes are duplicate-rejecting
RFC8785 JSON.

`runtime_artifacts` is path-sorted, at most 32 entries, each exactly
`{base64,path,sha256,size}`, 1..65536 bytes, aggregate <=524288. It includes the
two unit templates, runtime.env, daemon.json template, proxy config schema,
rootless launcher contract, Runner Create DTO/golden, HostControl proto/DTO/
normalizer/goldens. `docker_proxy_policy`
is <=262144 bytes. The outer manifest is <=2MiB. Release generation fails if a
checked-in byte, canonical security profile, parser/features digest, policy,
DTO, normalizer, or golden vector is absent or mismatched; prose is not a
substitute for these artifacts. All fields enter `runner_contract_sha256`.
### Entrypoint-initiated one-time token pull

Every registration-token issue, broker-accept, pull, frame, and configure boundary accepts exactly 1..4096 bytes, each byte ASCII `0x21..0x7e`; whitespace, DEL, non-ASCII, NUL, CR and LF reject before broker acceptance. No weaker token grammar exists.


The former Agent-to-entrypoint `/proc`/Unix-socket relay does not exist. The
persistent Agent has no host PID/proc mount and never receives either token.
Host-control helpers are credential-free. After exact container-id intent bind,
the release-pinned entrypoint pulls each token directly from the already
permitted configured Controller IPv4 endpoint.

The phase-scoped master capability is the existing `ownership_nonce`: 32 random
bytes encoded as exactly 64 lowercase hexadecimal ASCII, generated once per
materialization plan and never reused. It is present in the immutable
registration document, plan, journal and exact Runner label. It remains valid
only for the matching active bootstrap phases and becomes inert after final
bootstrap completion, cancellation, expiry, removal or terminalization. It does
not become inert merely because the registration or config-token endpoint was
consumed. Each raw registration/config token is independently consumed once.
The capability is never a reusable Controller credential and is redacted from
HTTP logs/diagnostics. Workflows never receive the registration-document bind.

The entrypoint connects only to
`https://<canonical-IPv4-literal>:<decimal-port>`, directly, using HTTP/1.1 over
TLS and the release/bootstrap-supplied Controller SPKI SHA-256. Hostnames, IPv6,
userinfo, alternate address, proxy, discovery, redirects, path prefix, query,
fragment, compression and chunked encoding are rejected. Every request has
`Connection: close`, exact Content-Length, UTF-8 RFC 8785 body without BOM,
duplicate members or trailing bytes, at most 4,096 aggregate request-header
bytes, 1,024 body bytes and 512 error-body bytes. Connect, TLS handshake,
response-header and body deadlines are independently five seconds. A request is
never retried after the server may have accepted it.

The common pull tuple is exactly:

```json
{"capability":"<64-lowerhex>","registration_document_sha256":"<64-lowerhex>","runner_id":"run_<ulid>","runtime_epoch":"<uint64-decimal-string>","schema":"<endpoint-schema>"}
```

The Controller resolves exactly one active registration intent by Runner id,
epoch, ownership capability and registration-document digest; the intent also
binds Task id, attempt nonce and already-bound rootful container id.

#### Registration token and completion

Registration pull is:

```text
POST /internal/runner-registration/v1/token
Content-Type: application/json
Accept: application/octet-stream
```

Schema is `groundplane.runner-token-pull.v1`. Success is HTTP 200,
`Content-Type: application/octet-stream`, `Cache-Control: no-store`, connection
close, and exact Content-Length `1..4096`; body is opaque token bytes with no
wrapper/newline/base64. The Controller accepts only current active Task/intent,
prospective epoch, document digest and bound container, within the
pre-registration deadline, before cancellation/terminal/cleanup, and when the
registration broker is pending.

Broker states are `pending -> consuming -> consumed` or `pending -> terminal`.
One CAS detaches the owned buffer and makes state irrevocably consumed before
the first response byte. Writer return then clears the owned token and working
buffers. Reset/crash/response loss after CAS remains consumed and is never
retried.

The entrypoint bypasses `config.sh`, `env.sh` and `run.sh`, ignores every
inherited/image environment entry, and passes an explicit ordered `envp`.
Before both configure and run it fails closed if `R/.env` exists; Groundplane
never creates that file. The exact base environment, in byte order with `R` and
`W` replaced by their canonical absolute slot paths, is:

```text
DOCKER_API_VERSION=1.52
DOCKER_BUILDKIT=0
DOCKER_HOST=unix:///var/run/docker.sock
HOME=<R>
LANG=C.UTF-8
LC_ALL=C.UTF-8
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
TMPDIR=<W>/_temp
```

Configure directly executes the pinned `./bin/Runner.Listener configure` argv
in `R` with exactly those eight entries followed by
`ACTIONS_RUNNER_INPUT_TOKEN=<registration-token>` as entry nine. The long-lived
`./bin/Runner.Listener run` receives exactly the eight base entries. No
`DOTNET_*`, other `RUNNER_*`, `USER`, `LOGNAME`, `SHELL`, `HOSTNAME`, `PWD`,
`TERM`, `CI`, `GITHUB_ACTIONS`, proxy, CA or other inherited variable is
present. There is no token argv/file/fd/stdin/durable OCI Env. The entrypoint
reaps the child, proves exact registration files, AgentId and DisableUpdate,
and clears its owned token/environment arenas before posting:

```text
POST /internal/runner-registration/v1/complete
Content-Type: application/json
Accept: application/json
```

The at-most-1,024-byte canonical body is exactly:

```json
{"capability":"<64-lowerhex>","registration_document_sha256":"<64-lowerhex>","registration_files_sha256":"<64-lowerhex>","runner_id":"run_<ulid>","runtime_epoch":"<uint64-decimal-string>","schema":"groundplane.runner-registration-complete.v1"}
```

Only the same active capability after registration-token consumption is
accepted. The Controller records the non-secret completion and makes config
issuance eligible. Success is HTTP 204 connection close and returns no token.

#### Config token and RunnerBootstrapConfigV1

Config-token pull is
`POST /internal/runner-bootstrap/v1/token`, JSON content type and octet-stream
accept, with common tuple schema `groundplane.runner-config-token-pull.v1`.
Success is exactly 43 unpadded base64url ASCII bytes matching
`[A-Za-z0-9_-]{43}`. Its broker has an independent one-consume CAS. Durable
record contains only token SHA-256, Runner/Task/epoch, endpoint, config digest,
issue/expiry times and nullable consumed_at; raw token exists only in bounded
Controller and entrypoint mutable arenas and expires 60 seconds after issue.

It is used once with:

```text
GET /internal/runner-bootstrap/v1/config
Authorization: Bearer <43-byte-token>
Accept: application/json
```

Controller atomically consumes before returning at most 16 KiB canonical
`RunnerBootstrapConfigV1`. That internal object contains exactly schema,
Runner/Tenant identity, runtime epoch string, exact Controller IPv4 endpoint,
SPKI SHA-256, integer `pull_interval_seconds=10`, integer
`max_concurrent_tasks=1`, and canonical sorted unique `github_labels` from
desired state. It contains no Agent channel/token/credential and none of these
fields maps to AgentConfig labels map, capacity or polling. Architecture and
internal Controller/runtime contracts mirror it; there is no public endpoint,
client, CLI or Console action.

Config response loss after consume is not replayed. Only after exact completed
GitHub registration proof may recovery issue one fresh config token; it never
issues a second GitHub token. The entrypoint clears token, Authorization header,
response and digest owned buffers, submits the existing final non-secret commit
proof, then and only then executes `./bin/Runner.Listener run`.

Fixed error bodies contain only code: 400 `request.invalid`, 401
`capability.invalid`, 409 `attempt.state_conflict`, 410 `attempt.gone`, 503
`controller.unavailable` before CAS, or 500 `controller.internal` before CAS.
After CAS no result becomes retryable. Ambiguous transport quiesces the exact
container and inspects registration; exact registration permits config-only
recovery, otherwise `registration_token_required`. Controller restart empties
raw brokers and never reconstructs a token from digest.

Owned mutable arenas use the existing bounded mlock/MADV_DONTDUMP and explicit
clear rules. Claims are limited to those arenas; unavoidable Go/HTTP/TLS,
`os/exec`, kernel, upstream .NET, same-UID, root and Docker copies are not
claimed erased, are never deliberately persisted/logged, and remain bounded by
process lifetime and GitHub's one-hour registration-token expiry. This ADR
supersedes ADR 0033's environment prohibition only for that bounded one-shot
upstream child hop.

### Per-Runner topology and stable identities

The sole executable local topology, account names, slot paths, units, helper
profiles, network/nft policy, rootless proxy, rootful Runner and lifecycle order
are defined in **Integrated runtime and security contract** below. No former
`S/home`, `S/github/actions-runner`, raw-socket Runner bind, one-unit runtime,
three-bind topology, `port-driver=none`, direct-Agent host mutation or
old Runner-id path remains an alternate contract.

Stable names remain exactly `gp_runner_<T>` for account and group,
`gp_runner_<runner-id>` for rootful container and Network, `g<T>` for bridge,
and `gp_<T>` for the Groundplane nft table, where `T` is the first 14 lowercase
unpadded RFC4648-base32 characters of SHA-256 of canonical Runner id. Collision
with another Runner is ownership corruption; no alternate is selected.

### Public Runner representation

Runner list and detail return exactly:

```json
{
  "id": "run_...",
  "slug": "storefront-ci",
  "tenant_id": "tnt_...",
  "project_id": "prj_...",
  "github_url": "https://github.com/acme/storefront",
  "name": "gp-01...",
  "labels": ["qa-workload"],
  "lifecycle": "provisioning",
  "create_task_id": "task_...",
  "remove_task_id": null,
  "online": false,
  "observed_at": null,
  "created_at": "2026-08-24T12:00:00Z"
}
```

`slug` is the mutable Tenant-scoped Groundplane label; `name` remains the
immutable derived GitHub-visible name. `project_id` is omitted for a
Tenant-owned Runner. `remove_task_id` is non-null
only while a deletion tombstone exists. `observed_at` is nullable before the
first inspection. Timestamps are UTC RFC3339. Relative ages are presentation
only. The response does not include allocation, image, container, local path,
credential, or raw inspection details.

### REST API

The Runner operations are:

| Operation | Contract |
| --- | --- |
| `GET /runners?tenant={id}&cursor={cursor}` | Fixed-revision page containing every direct and Project-owned Runner charged to the Tenant quota. |
| `GET /runners?project={id}&cursor={cursor}` | Fixed-revision page containing only Runners directly owned by that Project. |
| `GET /runners/{id}` | Public Runner detail. |
| `POST /runners` | Protected create; accepts the request below and returns `202 {task_id}`. |
| `PATCH /runners/{id}` | Synchronous protected slug replacement; accepts exactly `{slug}` and returns `200` Runner. |
| `POST /runners/{id}/retry` | Protected failed-create retry; accepts only `{registration_token}` and returns `202 {task_id}`. |
| `DELETE /runners/{id}` | Bodyless protected local removal and `202 {task_id}`. |

Exactly one list scope is required. Exactly one of `tenant_id` and `project_id`
is required by create. Create accepts:

```json
{
  "slug": "storefront-ci",
  "tenant_id": "tnt_...",
  "github_url": "https://github.com/acme",
  "labels": ["qa-workload"],
  "registration_token": "<write-only>"
}
```

or the same shape with `project_id` and a repository URL. PATCH changes only
the Runner slug. There is no owner, GitHub URL, label, image, GitHub name,
online, lifecycle, allocation, runtime update, deregister, scale, or placement
operation.

`GET /internal/runner-bootstrap/v1/config` is a single-purpose authenticated
runtime bootstrap exchange, not an operator-facing Controller capability. It
is unavailable to operator credentials and therefore has no Console action or
CLI command and does not alter the 1:1 rule.

Task-producing create, failed-create retry, and remove mutations follow the
shared Task idempotency contract. Same key and same canonical non-secret intent
returns the original Task; reusing a key with a different non-secret intent is
rejected. Token bytes never participate in the request hash. A token supplied
on create/retry transport replay is cleared without creating or feeding another
attempt.

Slug PATCH does not use original-Task replay. It exclusively follows the
earlier synchronous contract: same key and canonical body returns the stored
exact `200` Runner response bytes, same key with a different body is
`idempotency.conflict`, and PATCH never creates or returns a Task.

### CLI

The exact command surface is:

```text
groundplane runner list
groundplane runner add --github-url <url>
                       --slug <slug>
                       --registration-token-file <path|->
                       [--label <label> ...]
groundplane runner show <slug> [--id]
groundplane runner edit <slug> --slug <new-slug> [--id]
groundplane runner retry <slug> --registration-token-file <path|-> [--id]
groundplane runner remove <slug> [--id]
```

The existing global Tenant/Project scope resolution selects ownership and list
filtering. `runner list` with Tenant scope shows the combined Tenant quota;
Project scope narrows to the exact Project. `runner add` with Project scope
creates a Project-owned repository Runner; without Project scope it creates a
Tenant-owned organization Runner.

There is no token flag. `--registration-token-file -` reads stdin. The CLI
reads at most 4,097 bytes, removes one optional terminal LF byte when present,
then requires the resulting token be exactly 1..4,096 bytes and every byte ASCII
0x21..0x7e. Empty, a second LF, CR, DEL, non-ASCII, NUL or any whitespace
rejects before request construction. It never prints, formats, logs, or
retains the token. Runner targets use the Tenant-scoped slug by default;
`--id` selects the stable Runner id. The derived GitHub name is display-only
and is never a CLI target.

### Console

The Tenant Runners page loads the Tenant-filtered API page, including direct
and Project-owned records, and displays the combined quota as `N / 5`.
Provisioning, failed, and deleting records continue to count. Rows open a
detail drawer containing the public Runner representation, owner, GitHub URL,
Task links, lifecycle, online state, and observation time.

The add dialog selects organization scope or one Tenant Project, requires a
Tenant-unique slug and owner-shaped `github_url`, accepts custom labels and a
masked registration token, and has no GitHub-name or image control. Submission
displays and polls the authoritative Task. It does not insert a fixture Runner,
declare immediate success, or manufacture online state. Detail exposes a
synchronous Edit slug action backed by `PATCH /runners/{id}` and the matching
CLI command.

A failed creation exposes a Runner-specific Retry action with a fresh masked
token. Provisioning and deleting disable conflicting actions. Removal requires
confirmation that states: `This removes local Runner resources only. Delete
the Runner in GitHub separately.` It dispatches and polls the removal Task.

The Console derives relative observation text from `observed_at`; relative text
is not stored or sent by the API. The unsafe host-socket Docker command preview,
`unless-stopped` policy, durable container token environment variable,
arbitrary name/target
fields, immediate-online fixture mutation, and claim that a Runner receives a
reusable Groundplane Agent credential are removed rather than retained as help
text or compatibility behavior. The one-time internal non-secret bootstrap
fetch is not an operator-facing Console action.

### Errors

Synchronous API failures use the shared RFC 7807 document and these exact
public mappings:

| HTTP | `code` | Condition |
| --- | --- | --- |
| `422` | `validation.failed` | Malformed owner XOR, slug, URL, label, token, cursor, id, body, or forbidden field. |
| `404` | `tenant.not_found` | Requested Tenant does not exist. |
| `404` | `project.not_found` | Requested Project does not exist. |
| `404` | `runner.not_found` | Requested Runner does not exist in the resolved scope. |
| `404` | `task.not_found` | Required source creation Task does not exist. |
| `409` | `runner.slug_conflict` | The Tenant-scoped Runner slug already exists. |
| `409` | `resource.in_use` | Tenant quota, host slots, or Runner `/29` pool is exhausted. |
| `409` | `state.conflict` | Invalid retry/removal lifecycle, concurrent operation, revision conflict, or idempotency request still in flight. |
| `422` | `idempotency.conflict` | One idempotency key is reused with a different canonical non-secret intent. |
| `500` | `internal` | Corrupt durable state or an unclassified Controller failure; detail remains non-secret. |

Runner Controller Tasks expose exactly one of these stable terminal failure
codes with bounded redacted detail:

| Task failure code | Meaning |
| --- | --- |
| `registration_token_required` | The one-use token was lost before registration could be proved. |
| `runner.registration_rejected` | GitHub rejected the token, URL scope, name, labels, or disabled-update registration. |
| `runner.image_unavailable` | The release-embedded digest could not be pulled or verified. |
| `runner.bootstrap_failed` | Required Ubuntu package, account, subordinate mapping, directory, or cgroup creation failed without an ownership collision. |
| `runner.network_policy_failed` | Runner pool, bridge, IPv4 enforcement, nftables, IPv6 disablement, or port-driver enforcement failed. |
| `runner.daemon_failed` | The owned rootless daemon did not start, answer, or retain its exact identity. |
| `runner.container_failed` | The immutable Runner container could not be created, started, or inspected. |
| `runner.registration_failed` | The controlled entrypoint pairing phase failed locally without a GitHub rejection and without losing the token. |
| `runner.readiness_timeout` | Registration completed but exact daemon/container/listener readiness was not reached before the Task deadline. |
| `runner.cleanup_failed` | Verified owned local resources could not be completely removed. |
| `runner.ownership_conflict` | A local account, mapping, directory, socket, daemon, cgroup, bridge, network, or container does not match durable ownership evidence. |

Failed creation retains all claims. Failed removal clears the active removal
fence while retaining the Runner and all claims. Successful removal deletes
only local Runner resources and durable claims and never invokes GitHub
deregistration.

### Required supplied inputs

The release-selected Runner OCI repository, configured Controller IPv4:port and TLS
SPKI SHA-256, and approved apt trust-root byte sequences plus repository
bindings and machine-bootstrap `runner.network_pool` are mandatory supplied
inputs with no default. `runner.network_pool` must be a canonical IPv4 prefix
of length 24 through 26 inclusive, dedicated to Runners and disjoint from
`environment_pool`, `system_pool`, and every protected or allocated range.
Its exact `N = 2^(29-p)` deterministic `/29` children are the machine slot
inventory. Host UID, same-number GID, subordinate UID, and subordinate GID
inventories must each contain at least `N` complete collision-free entries or
bootstrap fails before mutations. Their
validation, compilation, and runtime-authority rules are fixed by this ADR.
They are not unresolved product choices, and implementations may not invent,
discover, download, or normalize replacement values.

### DNS materialization contract

Runner DNS never uses ambient host resolver bytes as authority. The compiled
release contract contains exactly these two canonical resolver source assets:

| Asset | Canonical base64 bytes | Decoded size | SHA-256 |
|---|---|---:|---|
| `resolver-upstream.conf` | `bmFtZXNlcnZlciAxLjEuMS4xCm5hbWVzZXJ2ZXIgMS4wLjAuMQpvcHRpb25zIHRpbWVvdXQ6MiBhdHRlbXB0czoyIHJvdGF0ZQo=` | 74 | `d23853fdca3ed710769a32f524a19165b9e1361f05f1762befc8bec1f481677f` |
| `resolver-embedded.conf` | `bmFtZXNlcnZlciAxMjcuMC4wLjExCm9wdGlvbnMgdGltZW91dDoyIGF0dGVtcHRzOjIgcm90YXRlIG5kb3RzOjAK` | 66 | `7b59a25a5f151203d301cc006d86988479e3f2167f0dccaf2e5b79bf6e5cb2cc` |

Base64 follows the canonical alphabet, padding, and no-whitespace rules above.
The decoded upstream bytes, including final LF, are:

```text
nameserver 1.1.1.1
nameserver 1.0.0.1
options timeout:2 attempts:2 rotate
```

The decoded embedded-DNS bytes, including final LF, are:

```text
nameserver 127.0.0.11
options timeout:2 attempts:2 rotate ndots:0
```

The Agent materializes the assets at exactly:

```text
S/control/resolver-upstream.conf
S/control/resolver-embedded.conf
```

Both are regular one-link root:root `0644` files directly below the existing
root:root `0711` Runner root, with no ACL, default ACL, file capability,
symlink, or mount crossing. Publication uses descriptor-relative `openat2`,
an exclusive temporary, file fsync, `renameat2(RENAME_NOREPLACE)`, and parent
fsync. A fresh unclaimed slot rejects either pre-existing path even when bytes
match. A claimed Runner reuses a source only when path, device, inode, UID,
GID, mode, size, and SHA-256 match RuntimeOwnership. Mismatch is
`runner.ownership_conflict`; a source is never overwritten or repaired in
place. This is a dedicated privileged Runner-DNS Agent module, not the
Environment entry materializer.

The release pins `/usr/libexec/groundplane/runner-rootless-launch`. The user
unit executes that launcher, not `dockerd-rootless.sh` directly. The launcher
owns the locked RootlessKit arguments, creates the private user, mount, and
network namespaces with `/etc` and `/run` copy-up, then re-execs itself in
child mode. Child mode:

1. Proves the expected RootlessKit `/etc` copy-up mount and mount namespace.
2. Reads only `resolver-upstream.conf` after validating recorded identity and digest.
3. Atomically replaces inherited copy-up `/etc/resolv.conf` in that namespace.
4. Sets namespace-visible root:root `0644`, equivalent to host-visible allocated Runner UID/GID.
5. Self-bind-mounts the file read-only with `nosuid,nodev,noexec`.
6. Records mount id, parent mount id, device, inode, mode, size, digest, and mount-namespace inode.
7. Executes pinned `/usr/bin/dockerd-rootless.sh` in its child branch with the exact locked dockerd arguments.

Replacing that inherited copy-up file before dockerd start is the sole
permitted resolver overwrite. Live drift requires quiescence and epoch
recreation. Rootless `daemon.json` is exactly these 108 RFC 8785 bytes without
a final LF:

```json
{"dns":["1.1.1.1","1.0.0.1"],"dns-opts":["timeout:2","attempts:2","rotate"],"dns-search":["."],"ipv6":false}
```

The rootful Runner container follows the sole exact four-bind topology:

| Source | Target | Access |
|---|---|---|
| Proxy UDS | `/var/run/docker.sock` | Read-write `rprivate` |
| `R` | same absolute path | Read-write `rprivate` |
| `S/control/resolver-embedded.conf` | `/etc/resolv.conf` | Read-only `rprivate` |
| `S/control/registration-v1.json` | `/run/groundplane/registration-v1.json` | Read-only `rprivate` |

The raw rootless Docker socket is not a Runner bind. Container create sends
exactly `DNS=["1.1.1.1","1.0.0.1"]`,
`DNSOptions=["timeout:2","attempts:2","rotate"]`, and `DNSSearch=["."]`;
inspect must match. Empty search is rejected. The actual container mount
namespace proves the embedded resolver source/target device+inode, RO rprivate,
root:root0644, 66 bytes and exact digest; Docker's masked private resolver file
is not Groundplane authority.

For supported GitHub container jobs and service containers on their private
user-defined rootless network, pinned Moby 29.1.3, the exact daemon defaults,
and the upstream source produce exactly these 363 bytes and SHA-256
`cbc822d7c4ddda1ed546046c966722e107d60c72cb53b5a3b36f5fc3799eb768`:

```text
# Generated by Docker Engine.
# This file can be edited; Docker Engine will not make further changes once it
# has been modified.

nameserver 127.0.0.11
options timeout:2 attempts:2 rotate ndots:0

# Based on host file: '/etc/resolv.conf' (internal resolver)
# ExtServers: [1.1.1.1 1.0.0.1]
# Overrides: [nameservers search options]
# Option ndots from: internal
```

Workflow-created containers may deliberately override their own DNS. They are
not durable Groundplane materializations; nftables egress isolation, not a
claim that their resolver files are immutable, remains the security boundary.

RuntimeOwnership adds exactly two path-sorted persistent resolver-source
entries containing `path`, `device`, `inode`, `uid`, `gid`, `mode`,
`size`, and `sha256`. It adds rootless resolver evidence containing dockerd
PID/start ticks, mount-namespace inode, logical target `/etc/resolv.conf`,
device, inode, host UID/GID, mode, size, digest, mount id, parent mount id, and
required mount flags. It adds rootful resolver evidence containing exact
container PID/start ticks, source path and identity, target
`/etc/resolv.conf`, source/target device and inode, mode, size, digest, mount
id, and read-only state.

`daemon.start` succeeds only after exact source/config/unit evidence,
RootlessKit namespace evidence, dockerd PID/cgroup evidence, resolver view,
socket identity, and Docker `/info` identity match. `container.create`
succeeds only after exact HostConfig arrays, third bind, mount proof, `/29`
endpoint, and embedded bytes match. Recurring 10-second attestation checks both
sources and both active resolver views. Drift projects offline, prevents a next
job, and requires quiescence plus complete epoch reconciliation; live rewrite
is forbidden. Same-boot Controller restart retains an epoch only with
byte-identical ownership. Host reboot preserves the exact persistent sources,
allocates one prospective epoch, creates a new rootless namespace/mount
identity, and recreates the stale-epoch rootful container without GitHub
registration.

DNS resources participate only in the canonical cleanup order: rootful Runner
removal first proves its resolver mount gone; Docker/proxy stop proves rootless
namespace and resolver view gone; marked rootless data-root removal follows;
Network/manager/nft cleanup retains isolation; exact resolver source inodes are
unlinked with parent fsync during runnerfs cleanup. Any substituted source,
mount, PID, container or namespace conflicts and retains ownership/claims.

DNS acceptance requires:

- Golden size, bytes, base64, and SHA tests for upstream, embedded, job-generated, and `daemon.json` artifacts.
- A HostConfig test proving `DNSSearch=["."]` and an explicit regression proving `[]` inherits host search and is rejected.
- Rootful host acceptance with hostile search and unknown directives in host `/etc/resolv.conf`, proving the bound container view remains the exact 66 bytes.
- Rootless launcher tests covering copy-up validation, atomic replacement, read-only remount, wrong source identity, symlink/hardlink/ACL/mode drift, and partial-crash cleanup.
- Ownership substitution tests for every source, inode, mount id, namespace, PID, and target.
- Same-boot restart reuse, host-reboot new mount identity, and tamper-to-offline reconciliation tests.
- Local removal tests proving Docker-private resolver files are removed only by exact container/data-root cleanup and persistent sources only by inode-fenced unlink.
- Ubuntu 24.04 ARM64 acceptance covering shell DNS, image pull/build, container action, service-container name resolution, outbound DNS, and failure of cross-Runner/private-pool access.

### Acceptance evidence

C17 remains Proposed until release and Ubuntu 24.04 ARM64 evidence proves all
of the following against the exact pinned tuple:

- RFC 8785 RuntimeOwnership maximum fixtures, every closed identity variant,
  65,536-byte rejection, resource cardinalities, and byte-identical
  seal/cleanup/replace-runtime round trips;
- journal crash points before and after every issued/bound/complete transition,
  0/1/>1 discovery, rollback resolution of every issued-unknown state,
  active-authority budget charging, Controller-outage quiescence, abort
  receipt/ACK/GC, 16,384-revision remaining-work invariant, and reboot cleanup
  before E+1 materialization;
- the checked-in HostControl proto, helper Create/Inspect DTO/golden vectors,
  all nine AppArmor/seccomp artifacts, exact profile mapping, frame transport,
  helper retirement/observation, bootstrap revision/boot receipts, and absence
  of persistent Agent host mutation authority;
- exact systemd 255 unit bytes and semantics, launcher clearenv boundaries,
  RootlessKit/dockerd argv and environment, effective limits/cgroups/namespaces,
  proxy readiness before Docker and API readiness after Docker;
- the rootful Runner Create golden with User U:G, zero capabilities,
  NoNewPrivs, four mounts, eight labels, setuid/setgid/file-capability negative
  probes, registration/config token one-consume behavior, inert nonce replay
  rejection, and exact eight-entry Listener environment;
- Network/nft materialization and cleanup order, exact B+2 endpoint, three
  chains/twelve rules, inventory drift behavior without unauthorized mutation,
  and global UID proof while nft remains installed;
- proxy route/query/header/body manifest, peer pidfd/cgroup/mount/user-map/NNP
  authentication, procfd leases across stop/restart/daemon restart, exact
  native bind transcripts, deterministic derived volumes, audit fail-close,
  limits/deadlines/backpressure, strict tar/gzip corpus, classic Builder-v1
  success/failure/disconnect/cache-residue cleanup, and no `/grpc` or `/session`;
- release source probes for Actions Runner 2.336.0, Docker CLI/Moby 29.1.3 API
  1.52, vendored runc, RootlessKit/slirp4netns, AppArmor parser/features, and
  the exact ARM64 host. Missing literal artifacts or an x86-only host result is
  not acceptance evidence.


## Alternatives considered

### Operator-supplied GitHub Runner name

Rejected because it adds scoped uniqueness, rename semantics, another index,
and disagreement between local and GitHub identity without improving the MVP's
single persistent Runner model. The stable id already supplies a deterministic
external name. A separate mutable Groundplane slug satisfies human addressing
and the repository identity rule without changing GitHub registration.

### Separate organization and repository fields

Rejected because they duplicate the external locator and create normalization
and replay ambiguity. One immutable canonical URL is sufficient, while owner
kind determines its required shape.

### Runtime-configured, per-Runner, or tag-based image selection

Rejected because mutable tags and operator-selected images can change runtime
and secret-handling behavior across retries, while a machine override can make
the Controller and helper protocol disagree. The release-embedded exact
repository and frozen digest preserve reproducibility.

### RootlessKit port publishing

Rejected because a published rootless port crosses the per-Runner network
boundary and complicates fail-closed host firewall ownership. Container-job
service containers already communicate over their private Docker network.
Host-mode service-port publication is explicitly unsupported in the MVP.

### GitHub Runner self-update

Rejected because it lets the managed workload replace code outside the
Controller lifecycle and invalidates the frozen image contract. Groundplane
must publish a new pinned image deliberately.

### Registration token in Task persistence, argv, durable container environment, or persistent storage

Rejected. The only exception is the explicit entrypoint child environment for direct configure. The token is never Task/journal/OCI/file/stdin/helper/Agent-relay state; the Runner pulls it once from the SPKI-pinned Controller broker and an ambiguous response is never replayed.

### Giving the Runner the Groundplane Agent channel

Rejected because a GitHub workflow is untrusted tenant code. Giving the Runner
an Agent credential or Task channel would cross the control-plane execution
boundary and could let workflow code impersonate the Agent.

### Host Docker socket or privileged Docker-in-Docker

Rejected by ADR 0033 because either permits workflow control over Groundplane
containers and networks. A dedicated rootless daemon supplies Docker-compatible
jobs without host-daemon authority.

### GitHub API status and deregistration

Rejected because C17 manages one local runtime, not GitHub account state.
Online remains a fresh local observation, and GitHub deletion remains an
explicit manual action.

## Integrated runtime and security contract

This section is the sole executable host/runtime authority. Earlier identity,
API, and lifecycle clauses supply inputs; they do not create alternate host
paths, retry rules, or privilege profiles.

### Machine bootstrap, slot roots, and release authority

Ubuntu package dependency closure, global launcher/proxy binaries, AppArmor
profiles, and the slot mount belong exclusively to the authenticated one-time
machine bootstrap/install exemption, never Agent `host.bootstrap`. The bootstrap
consumes the release-bundled exact deb closure with ambient apt/network sources
disabled. It installs `/usr/libexec/groundplane/runner-rootless-launch` and
`/usr/local/libexec/groundplane-runner-docker-proxy` at release-bound digests;
the differing roots are intentional install domains. Runtime admission checks
the exact current-boot install evidence and fails closed. Runner cleanup never
removes machine-scoped packages, binaries, profiles, service, mount, or slot
anchors.

Bootstrap owns immutable revision directory
`/var/lib/groundplane/agent/machine-bootstrap/v1/<64-lowerhex-runner-contract-sha256>/`
mode `0700`, with 20-digit `0600` revisions, HEAD, and the same exclusive-temp,
fsync, rename, predecessor-chain, longest-unique-chain, fork/gap rejection rules
as the Runner journal. Bounds are 1 MiB/revision, 2,048 revisions, and 512
effects; lock is `/run/lock/groundplane-machine-bootstrap.lock`.

A revision is exact canonical JSON:

```text
{"bootstrap_request_sha256":digest,"created_at":UTC,"effects":[...],
 "journal_id":UUID,"origin_boot_id":UUID,"phase":phase,
 "predecessor_sha256":digest-or-null,"receipt":receipt-or-null,
 "release_contract_sha256":digest,"revision":uint-string,
 "schema":"groundplane.machine-bootstrap-journal/v1",
 "terminal":terminal-or-null,"updated_at":UTC}
```

The request digest binds canonical `groundplane.machine-bootstrap-plan.v1`
bytes: release digest, pool, all slot/UID/GID/subid tuples, package/bundle
manifest, global file list, nine security entries, NSS rules, mount/unit bytes,
and every effect request.

Bootstrap Effect `kind` is exactly one high-level enum value
`PACKAGE_STAGE|PACKAGE_INSTALL|GLOBAL_FILE|NSS_PREFLIGHT|APPARMOR_FILE|
APPARMOR_LOAD|SLOT_MOUNT|SLOT_ANCHOR|SLOT_ACL|RECEIPT`. Its `object` is a
separate closed tagged domain:

```text
singleton {type:"singleton"}
global_file {type:"global_file",manifest_index,path_kind,path_digest}
apparmor_profile {type:"apparmor_profile",name,source_path,source_sha256}
slot_anchor {type:"slot_anchor",slot_index,anchor}
```

PACKAGE_STAGE, PACKAGE_INSTALL, NSS_PREFLIGHT, SLOT_MOUNT and RECEIPT require
singleton. GLOBAL_FILE requires global_file and binds the exact manifest path
identity. APPARMOR_FILE/LOAD require apparmor_profile with one of the nine
release names. SLOT_ANCHOR/ACL require slot_anchor with slot `00..N-1` and
anchor `S|CONTROL|HOME|WORK|EXTERNALS|TOOL` as legal for the kind. Effect schema
is exactly `{dispatches,kind,object,object_digest,proof_b64,proof_sha256,
request_sha256,sequence,slot_index,state}`. Object digest is
`sha256:SHA256("groundplane.machine-bootstrap-object.v1" || NUL || RFC8785(object))`.
Integers are strings, nullable values are null, and state is exactly
`planned|issued|bound|complete`. Kind/object mismatch or unknown member
terminalizes journal corruption.


`proof_b64` is null or unpadded base64url of canonical JSON <=65,536 decoded
bytes, and proof_sha256 is null with it or SHA-256 of decoded bytes. Exact proof
objects are:

```text
package_stage {directory_identity,bundle_file_set_sha256}
package_install {dpkg_status_sha256,installed_package_set_sha256}
global_file|apparmor_file {path_kind,path_identity}
nss {nsswitch_identity,account_file_set_sha256,dpkg_status_sha256,
  forbidden_unit_set_sha256}
apparmor_load {features_sha256,load_name,mode,source_sha256}
slot_mount {device,mount_id,parent_mount_id,root_inode,flags_sha256}
slot_anchor {anchor,path_identity}
slot_acl {access_acl_sha256,anchor,default_acl_sha256}
receipt {path_identity,receipt_sha256}
```

All integers inside proof identities are decimal strings. Phase is exactly
`prepared|package_stage_issued|package_stage_bound|package_install_issued|
package_install_bound|executables_issued|executables_bound|
nss_preflight_bound|apparmor_issued|apparmor_bound|slot_mount_issued|
slot_mount_bound|slots_installing|slots_bound|receipt_bound|complete|terminal`.
Terminal is exactly
`{at_revision,code,effect_sequence,evidence_sha256,retryable:false}` where code
is one of `journal_corrupt|journal_bounds_exceeded|release_mismatch|
package_conflict|nss_conflict|apparmor_conflict|slot_mount_conflict|
slot_identity_conflict|dispatch_budget_exhausted|deadline_exhausted`.
 Kinds/order are PACKAGE_STAGE,
PACKAGE_INSTALL, one GLOBAL_FILE per byte-sorted manifest path, NSS_PREFLIGHT,
for each security name APPARMOR_FILE then APPARMOR_LOAD, SLOT_MOUNT, then for
slot00..N-1 SLOT_ANCHOR S/CONTROL/HOME/WORK/EXTERNALS/TOOL and SLOT_ACL
HOME/WORK/EXTERNALS/TOOL, then RECEIPT. Object is a closed enum, never a path.
Proof variants are closed package-stage/install, path identity, NSS file/dpkg,
AppArmor load, slot mount, anchor, ACL, and receipt records. All generated
identities bind before dependencies.

Phases are `prepared`, package stage issued/bound, package install issued/bound,
executables issued/bound, nss preflight bound, AppArmor issued/bound, slot mount
issued/bound, slots installing/bound, receipt bound, complete, or terminal and
are a deterministic projection of first incomplete Effect. Dispatch budgets:
package stage2, install3, each global/AppArmor file2, NSS3, AppArmor load2, slot
mount2, each anchor/ACL2, receipt2. File/mount/parser calls are 30 seconds,
package transaction 10 minutes, bootstrap 30 minutes; counters/deadline do not
reset. Exact desired state adopts, absence retries, mismatch terminalizes.
Package issued may adopt exact desired, rerun an authenticated manifest prefix,
or conflict; bootstrap never uninstalls.

Offline install uses a new network namespace with no configured interface,
exact env `DEBIAN_FRONTEND=noninteractive`, `LANG=C.UTF-8`, `LC_ALL=C.UTF-8`,
fixed PATH, and argv:

```text
/usr/bin/apt-get -o Dir::Etc::sourcelist=/dev/null -o Dir::Etc::sourceparts=- -o Acquire::Retries=0 -o Acquire::http::Proxy=false -o Acquire::https::Proxy=false --no-download --no-install-recommends --yes install <absolute staged deb paths in manifest order>
```

AppArmor load is exact `/usr/sbin/apparmor_parser --replace --skip-cache
/etc/apparmor.d/<name>` with empty inherited environment plus fixed PATH/LANG/
LC_ALL. Unknown results observe exact package/profile state. Terminal schema is
`{at_revision,code,effect_sequence,evidence_sha256,retryable:false}` with only
journal/bounds/release/package/NSS/AppArmor/slot/dispatch/deadline codes.

Bootstrap requires passwd/group/shadow/gshadow NSS sources exactly `files`
(initgroups absent or files), rejects compat/systemd/sss/ldap/winbind/NIS, and
a release package inventory without nscd, sssd, winbind, nslcd, ypbind, or
non-files modules. Host-root bootstrap proves inactive/absent service/socket
baseline. Per-Runner NSS proof later reads only `/etc` and dpkg status and
compares live facts to authenticated receipt; it does not query systemd.
Host-root post-bootstrap drift is outside the threat model.

The install receipt is canonical
`groundplane.machine-bootstrap-install-receipt.v1`, fields
`bootstrap_request_sha256,completed_at,executable_set_sha256,
helper_security_set_sha256,nss_receipt_sha256,package_set_sha256,
release_contract_sha256,slot_definition_set_sha256,slot_mount_unit_sha256,
receipt_sha256`, self-hashed with the last member null and atomically published
as `install-receipt.json` mode0600.

Bootstrap creates persistent `/var/lib/groundplane/runner-slots`, self-bind
remounted `nodev,nosuid,rprivate`, and exactly configured N slots, where N is 1..32, named `slot-00` through `slot-<N-1>`. `S=.../slot-<two digits>`, `R=S/runner`, `W=R/_work`,
`E=R/externals`, `T=W/_tool`, and `S/control` is root0755. Root-owned sticky
parents and exact ACLs allow U to mutate permitted contents but never replace
W/E/T anchors. Pool changes require rebootstrap with zero claims.

Release-owned `groundplane-runner-slots.service`, ordered after local
filesystems/AppArmor and before Agent, invokes only digest-pinned
`runner-slot-mount ensure`. On each boot it verifies install receipt, nine
profiles/features, NSS/dpkg, anchors/ACLs and the persistent self-bind, then
atomically writes current boot receipt below `boots/<boot-id>.json` mode0600.
Receipt schema is `groundplane.machine-bootstrap-boot-receipt.v1` with
`apparmor_set_sha256,boot_id,install_receipt_sha256,mount_device,
mount_flags_sha256,mount_id,mount_parent_id,nss_live_sha256,
release_contract_sha256,slot_anchor_set_sha256,receipt_sha256`, self-hashed with
the last null. Only current and prior boot receipts remain after new fsync.
Agent accepts no Task without current receipt. Another release is permitted only
with zero ownership/claims/journals; MVP has no live migration.

### Sealed host-control helper

The persistent Agent receives no host PID/proc, user/system bus, cgroup, raw
rootless socket, account, nft, or Runner filesystem mutation mount. It directly
uses only rootful Docker API, its journal, and slot ledger. Each host operation
runs one same-digest-pinned Agent-image helper with an operation-specific closed
profile. At most one helper exists per Runner and it is removed/proven absent
before another.

The release checks in `runner_host_control_v1.proto`; request and response are
1..65,536 bytes. The normative wire tags are:

```proto
syntax = "proto3";
package groundplane.runner_host_control.v1;

enum ResultV1 { RESULT_V1_UNSPECIFIED=0; RESULT_V1_APPLIED=1; RESULT_V1_NO_CHANGE=2; RESULT_V1_CONFLICT=3; RESULT_V1_FAILED=4; }
enum FailureCodeV1 { FAILURE_CODE_V1_UNSPECIFIED=0; FAILURE_CODE_V1_NONE=1; FAILURE_CODE_V1_PRECONDITION_MISMATCH=2; FAILURE_CODE_V1_OWNERSHIP_MISMATCH=3; FAILURE_CODE_V1_AMBIGUOUS_STATE=4; FAILURE_CODE_V1_KERNEL_UNSUPPORTED=5; FAILURE_CODE_V1_IO=6; FAILURE_CODE_V1_DBUS_REJECTED=7; FAILURE_CODE_V1_SYSTEMD_JOB_FAILED=8; FAILURE_CODE_V1_NFT_REJECTED=9; FAILURE_CODE_V1_DAEMON_REJECTED=10; FAILURE_CODE_V1_DEADLINE=11; FAILURE_CODE_V1_BOUNDS=12; FAILURE_CODE_V1_INTERNAL=13; }
enum ProofStateV1 { PROOF_STATE_V1_UNSPECIFIED=0; PROOF_STATE_V1_COMPLETE=1; PROOF_STATE_V1_PARTIAL=2; }
enum PresenceV1 { PRESENCE_V1_UNSPECIFIED=0; PRESENCE_V1_ABSENT=1; PRESENCE_V1_PRESENT=2; }
enum MutationV1 { MUTATION_V1_UNSPECIFIED=0; MUTATION_V1_ENSURE=1; MUTATION_V1_REMOVE=2; }
enum MachineModeV1 { MACHINE_MODE_V1_UNSPECIFIED=0; MACHINE_MODE_V1_RELEASE=1; MACHINE_MODE_V1_NSS=2; }
enum EtcFileKindV1 { ETC_FILE_KIND_V1_UNSPECIFIED=0; ETC_FILE_KIND_V1_GROUP=1; ETC_FILE_KIND_V1_GSHADOW=2; ETC_FILE_KIND_V1_PASSWD=3; ETC_FILE_KIND_V1_SHADOW=4; ETC_FILE_KIND_V1_SUBUID=5; ETC_FILE_KIND_V1_SUBGID=6; }
enum RunnerFsObjectV1 { RUNNER_FS_OBJECT_V1_UNSPECIFIED=0; RUNNER_FS_OBJECT_V1_ANCHOR_SLOT=1; RUNNER_FS_OBJECT_V1_ANCHOR_CONTROL=2; RUNNER_FS_OBJECT_V1_ANCHOR_HOME=3; RUNNER_FS_OBJECT_V1_ANCHOR_WORK=4; RUNNER_FS_OBJECT_V1_ANCHOR_EXTERNALS=5; RUNNER_FS_OBJECT_V1_ANCHOR_TOOL=6; RUNNER_FS_OBJECT_V1_RUNNER_MARKER=7; RUNNER_FS_OBJECT_V1_CONFIG_DIR=8; RUNNER_FS_OBJECT_V1_SYSTEMD_DIR=9; RUNNER_FS_OBJECT_V1_SYSTEMD_USER_DIR=10; RUNNER_FS_OBJECT_V1_LOCAL_DIR=11; RUNNER_FS_OBJECT_V1_LOCAL_SHARE_DIR=12; RUNNER_FS_OBJECT_V1_ROOTLESS_DATA_ROOT=13; RUNNER_FS_OBJECT_V1_CLAIM=14; RUNNER_FS_OBJECT_V1_PROXY_UNIT=15; RUNNER_FS_OBJECT_V1_DOCKER_UNIT=16; RUNNER_FS_OBJECT_V1_RUNTIME_ENV=17; RUNNER_FS_OBJECT_V1_DAEMON_JSON=18; RUNNER_FS_OBJECT_V1_PROXY_CONFIG=19; RUNNER_FS_OBJECT_V1_RESOLVER_UPSTREAM=20; RUNNER_FS_OBJECT_V1_RESOLVER_EMBEDDED=21; RUNNER_FS_OBJECT_V1_REGISTRATION_DOCUMENT=22; RUNNER_FS_OBJECT_V1_CLEAN_WORK_TREE=23; RUNNER_FS_OBJECT_V1_CLEAN_EXTERNALS_TREE=24; RUNNER_FS_OBJECT_V1_CLEAN_TOOL_TREE=25; RUNNER_FS_OBJECT_V1_CLEAN_CONFIG_TREE=26; RUNNER_FS_OBJECT_V1_CLEAN_LOCAL_TREE=27; RUNNER_FS_OBJECT_V1_CLEAN_ROOTLESS_DATA_TREE=28; }
enum Login1ActionV1 { LOGIN1_ACTION_V1_UNSPECIFIED=0; LOGIN1_ACTION_V1_OBSERVE=1; LOGIN1_ACTION_V1_SET_LINGER_TRUE=2; LOGIN1_ACTION_V1_SET_LINGER_FALSE=3; LOGIN1_ACTION_V1_TERMINATE_USER=4; }
enum UnitKindV1 { UNIT_KIND_V1_UNSPECIFIED=0; UNIT_KIND_V1_MANAGER=1; UNIT_KIND_V1_PROXY=2; UNIT_KIND_V1_DOCKER=3; }
enum UserUnitActionV1 { USER_UNIT_ACTION_V1_UNSPECIFIED=0; USER_UNIT_ACTION_V1_MANAGER_RELOAD=1; USER_UNIT_ACTION_V1_OBSERVE=2; USER_UNIT_ACTION_V1_START=3; USER_UNIT_ACTION_V1_STOP=4; }
enum NftActionV1 { NFT_ACTION_V1_UNSPECIFIED=0; NFT_ACTION_V1_OBSERVE=1; NFT_ACTION_V1_APPLY=2; NFT_ACTION_V1_DELETE=3; }
enum ControlProcessKindV1 { CONTROL_PROCESS_KIND_V1_UNSPECIFIED=0; CONTROL_PROCESS_KIND_V1_PROXY=1; CONTROL_PROCESS_KIND_V1_DOCKER=2; CONTROL_PROCESS_KIND_V1_ROOTFUL_RUNNER=3; }
enum UidQuiescenceStageV1 { UID_QUIESCENCE_STAGE_V1_UNSPECIFIED=0; UID_QUIESCENCE_STAGE_V1_USER=1; UID_QUIESCENCE_STAGE_V1_FINAL=2; }
enum RuntimeSocketModeV1 { RUNTIME_SOCKET_MODE_V1_UNSPECIFIED=0; RUNTIME_SOCKET_MODE_V1_PRESENT=1; RUNTIME_SOCKET_MODE_V1_ABSENT=2; }
enum SocketKindV1 { SOCKET_KIND_V1_UNSPECIFIED=0; SOCKET_KIND_V1_RAW_DOCKER=1; SOCKET_KIND_V1_PROXY=2; }
enum ProcessRoleV1 { PROCESS_ROLE_V1_UNSPECIFIED=0; PROCESS_ROLE_V1_PROXY=1; PROCESS_ROLE_V1_ROOTLESSKIT=2; PROCESS_ROLE_V1_DOCKERD=3; PROCESS_ROLE_V1_RUNNER_INIT=4; PROCESS_ROLE_V1_USER_MANAGER=5; PROCESS_ROLE_V1_OTHER_UID=6; }
message AllocationV1 { string slot_index=1; string uid=2; string gid=3; string subuid_start=4; string subuid_count=5; string subgid_start=6; string subgid_count=7; }
message AccountV1 { string name=1; string uid=2; string primary_gid=3; string home=4; string subuid_start=5; string subuid_count=6; string subgid_start=7; string subgid_count=8; }
message PathIdentityV1 { string device=1; string inode=2; string mount_id=3; string uid=4; string gid=5; string mode=6; string nlink=7; string size=8; bytes sha256=9; bytes access_acl_sha256=10; bytes default_acl_sha256=11; bytes capability_sha256=12; }
message ExpectedPathV1 { PresenceV1 presence=1; PathIdentityV1 identity=2; }
message ProcessExpectedV1 { ProcessRoleV1 role=1; string pid=2; string start_ticks=3; string control_group=4; bytes executable_sha256=5; }
message SocketExpectedV1 { SocketKindV1 kind=1; string device=2; string inode=3; }
message NftRuleExpectedV1 { string chain=1; string handle=2; string comment=3; bytes expression_sha256=4; }
message MachineObserveV1 { MachineModeV1 mode=1; }
message SlotObserveV1 { AllocationV1 allocation=1; }
message EtcFileObserveV1 { EtcFileKindV1 kind=1; AccountV1 account=2; }
message EtcFileMutateV1 { EtcFileKindV1 kind=1; MutationV1 action=2; AccountV1 account=3; ExpectedPathV1 expected_file=4; }
message RunnerFsObserveV1 { RunnerFsObjectV1 object=1; AllocationV1 allocation=2; string claimed_effect_id=3; bytes desired_bytes=4; }
message RunnerFsMutateV1 { RunnerFsObjectV1 object=1; MutationV1 action=2; AllocationV1 allocation=3; string claimed_effect_id=4; bytes desired_bytes=5; ExpectedPathV1 expected_path=6; }
message Login1OperationV1 { Login1ActionV1 action=1; AccountV1 account=2; ExpectedPathV1 expected_linger_marker=3; }
message UserUnitOperationV1 { UserUnitActionV1 action=1; UnitKindV1 unit=2; AccountV1 account=3; bytes fragment_sha256=4; bytes semantic_sha256=5; bytes expected_unit_proof_sha256=6; }
message NftOperationV1 { NftActionV1 action=1; AllocationV1 allocation=2; bytes canonical_policy=3; bytes policy_sha256=4; repeated NftRuleExpectedV1 expected_rules=5; }
message ControlProcessObserveV1 { ControlProcessKindV1 kind=1; AllocationV1 allocation=2; repeated ProcessExpectedV1 expected_processes=3; repeated SocketExpectedV1 expected_sockets=4; }
message UidQuiescenceObserveV1 { UidQuiescenceStageV1 stage=1; AllocationV1 allocation=2; }
message RuntimeSocketObserveV1 { RuntimeSocketModeV1 mode=1; AllocationV1 allocation=2; bytes expected_engine_identity_sha256=3; }
message RootlessDaemonCleanupV1 { AllocationV1 allocation=1; bytes present_response_sha256=2; string daemon_id=3; bytes expected_inventory_sha256=4; }
message HostControlRequest { uint32 schema=1; string runner_id=2; string tenant_id=3; string task_id=4; string operation_id=5; string effect_id=6; string runtime_epoch=7; string mutation_sequence=8; bytes ownership_nonce=9; bytes invocation_nonce=10; bytes plan_sha256=11; bytes journal_payload_sha256=12; bytes release_contract_sha256=13; string host_boot_id=14; bytes request_sha256=15; string agent_id=16; reserved 17 to 19; oneof operation { MachineObserveV1 machine_observe=20; SlotObserveV1 slot_observe=21; EtcFileObserveV1 etc_file_observe=22; EtcFileMutateV1 etc_file_mutate=23; RunnerFsObserveV1 runner_fs_observe=24; RunnerFsMutateV1 runner_fs_mutate=25; Login1OperationV1 login1_operation=26; UserUnitOperationV1 user_unit_operation=27; NftOperationV1 nft_operation=28; ControlProcessObserveV1 control_process_observe=29; UidQuiescenceObserveV1 uid_quiescence_observe=30; RuntimeSocketObserveV1 runtime_socket_observe=31; RootlessDaemonCleanupV1 rootless_daemon_cleanup=32; } }
```

The same file defines response proof enums/messages exactly as follows; no Go
package option changes the wire:

```proto
enum MachineFileKindV1 { MACHINE_FILE_KIND_V1_UNSPECIFIED=0; MACHINE_FILE_KIND_V1_NSSWITCH=1; MACHINE_FILE_KIND_V1_GROUP=2; MACHINE_FILE_KIND_V1_GSHADOW=3; MACHINE_FILE_KIND_V1_PASSWD=4; MACHINE_FILE_KIND_V1_SHADOW=5; MACHINE_FILE_KIND_V1_SUBUID=6; MACHINE_FILE_KIND_V1_SUBGID=7; MACHINE_FILE_KIND_V1_DPKG_STATUS=8; }
enum SlotAnchorKindV1 { SLOT_ANCHOR_KIND_V1_UNSPECIFIED=0; SLOT_ANCHOR_KIND_V1_SLOT=1; SLOT_ANCHOR_KIND_V1_CONTROL=2; SLOT_ANCHOR_KIND_V1_HOME=3; SLOT_ANCHOR_KIND_V1_WORK=4; SLOT_ANCHOR_KIND_V1_EXTERNALS=5; SLOT_ANCHOR_KIND_V1_TOOL=6; }
enum QuiescenceV1 { QUIESCENCE_V1_UNSPECIFIED=0; QUIESCENCE_V1_YES=1; QUIESCENCE_V1_NO=2; }
enum SocketAcceptV1 { SOCKET_ACCEPT_V1_UNSPECIFIED=0; SOCKET_ACCEPT_V1_NONACCEPTING=1; SOCKET_ACCEPT_V1_ACCEPTING=2; }
message PathEvidenceV1 { PresenceV1 presence=1; PathIdentityV1 identity=2; }
message ExecutableEvidenceV1 { string manifest_index=1; PathIdentityV1 identity=2; }
message MachineFileEvidenceV1 { MachineFileKindV1 kind=1; PathIdentityV1 identity=2; }
message SlotAnchorEvidenceV1 { SlotAnchorKindV1 kind=1; PathIdentityV1 identity=2; }
message AccountFileEvidenceV1 { EtcFileKindV1 kind=1; PathIdentityV1 file=2; PresenceV1 managed_row=3; bytes managed_row_bytes=4; }
message RunnerFsEvidenceV1 { RunnerFsObjectV1 object=1; PathEvidenceV1 path=2; string entry_count=3; bytes entry_set_sha256=4; }
message BusEvidenceV1 { PresenceV1 presence=1; string device=2; string inode=3; string uid=4; string gid=5; string mode=6; }
message LingerEvidenceV1 { PresenceV1 enabled=1; PathEvidenceV1 marker=2; PresenceV1 user_manager=3; string manager_boot_id=4; BusEvidenceV1 user_bus=5; }
message AncestorPidsEvidenceV1 { string control_group=1; string pids_max=2; }
message SystemdUnitEvidenceV1 { UnitKindV1 unit=1; PathIdentityV1 fragment=2; bytes fragment_sha256=3; bytes semantic_sha256=4; string systemd_version=5; string manager_boot_id=6; string load_state=7; string unit_file_state=8; string need_daemon_reload=9; string transient=10; string source_path=11; repeated string drop_in_paths=12; repeated string aliases=13; string active_state=14; string sub_state=15; string main_pid=16; string main_pid_start_ticks=17; string control_group=18; string dbus_job_id=19; string dbus_job_path=20; string dbus_job_result=21; string limit_nofile_soft=22; string limit_nofile_hard=23; string limit_nproc_soft=24; string limit_nproc_hard=25; string tasks_max=26; repeated AncestorPidsEvidenceV1 ancestor_pids=27; string effective_pids_max=28; }
message NftRuleEvidenceV1 { string family=1; string table=2; string chain=3; string handle=4; string comment=5; bytes expression_sha256=6; }
message ProcessEvidenceV1 { ProcessRoleV1 role=1; string pid=2; string start_ticks=3; string real_uid=4; string effective_uid=5; string saved_uid=6; string fs_uid=7; string real_gid=8; string effective_gid=9; string saved_gid=10; string fs_gid=11; string control_group=12; string pid_namespace_inode=13; string mount_namespace_inode=14; string user_namespace_inode=15; bytes executable_sha256=16; bytes argv_sha256=17; repeated string listening_socket_inodes=18; string executable_device=19; string executable_inode=20; string network_namespace_inode=21; bytes uid_map_sha256=22; bytes gid_map_sha256=23; }
message SocketEvidenceV1 { SocketKindV1 kind=1; PresenceV1 presence=2; PathIdentityV1 identity=3; SocketAcceptV1 accept=4; string peer_uid=5; string engine_api_version=6; string engine_version=7; string daemon_id=8; bytes engine_identity_sha256=9; }
message MountIdentityEvidenceV1 { string source_path=1; string source_device=2; string source_inode=3; string source_mount_id=4; string target_path=5; string target_device=6; string target_inode=7; string target_mount_id=8; string mount_type=9; bool read_only=10; string propagation=11; bool create_mountpoint=12; bool non_recursive=13; }
message RootlessRuntimeEvidenceV1 { string daemon_id=1; string engine_version=2; string api_version=3; string architecture=4; bool rootless=5; ProcessEvidenceV1 rootlesskit=6; ProcessEvidenceV1 dockerd=7; PathIdentityV1 data_root=8; bytes daemon_config_sha256=9; bytes resolver_view_sha256=10; SocketEvidenceV1 raw_socket=11; MountIdentityEvidenceV1 proxy_alias=12; PathIdentityV1 state_dir=13; bytes state_dir_entries_sha256=14; }
message MachineObserveProofV1 { ProofStateV1 state=1; MachineModeV1 mode=2; repeated ExecutableEvidenceV1 executables=3; repeated MachineFileEvidenceV1 files=4; }
message SlotObserveProofV1 { ProofStateV1 state=1; repeated SlotAnchorEvidenceV1 anchors=2; }
message EtcFileProofV1 { ProofStateV1 state=1; AccountFileEvidenceV1 file=2; }
message RunnerFsProofV1 { ProofStateV1 state=1; RunnerFsEvidenceV1 object=2; }
message Login1ProofV1 { ProofStateV1 state=1; LingerEvidenceV1 linger=2; }
message UserUnitProofV1 { ProofStateV1 state=1; BusEvidenceV1 user_bus=2; SystemdUnitEvidenceV1 unit=3; }
message NftProofV1 { ProofStateV1 state=1; bytes policy_sha256=2; repeated NftRuleEvidenceV1 rules=3; }
message ControlProcessProofV1 { ProofStateV1 state=1; ControlProcessKindV1 kind=2; repeated ProcessEvidenceV1 processes=3; bytes process_set_sha256=4; RootlessRuntimeEvidenceV1 rootless_runtime=5; }
message UidQuiescenceProofV1 { ProofStateV1 state=1; UidQuiescenceStageV1 stage=2; QuiescenceV1 quiescent=3; string offender_count=4; repeated ProcessEvidenceV1 bounded_offenders=5; bytes offender_set_sha256=6; }
message RuntimeSocketProofV1 { ProofStateV1 state=1; RuntimeSocketModeV1 mode=2; SocketEvidenceV1 raw=3; SocketEvidenceV1 proxy=4; }
message RootlessDaemonCleanupProofV1 { ProofStateV1 state=1; string before_count=2; bytes before_sha256=3; string removed_count=4; bytes removed_sha256=5; string remaining_count=6; bytes remaining_sha256=7; string daemon_id=8; }
message HostControlResponse { uint32 schema=1; string runner_id=2; string tenant_id=3; string task_id=4; string operation_id=5; string effect_id=6; string runtime_epoch=7; string mutation_sequence=8; bytes ownership_nonce=9; bytes invocation_nonce=10; bytes plan_sha256=11; bytes journal_payload_sha256=12; bytes release_contract_sha256=13; string host_boot_id=14; bytes request_sha256=15; string agent_id=16; ResultV1 result=17; FailureCodeV1 failure_code=18; bytes response_sha256=19; oneof proof { MachineObserveProofV1 machine_observe=20; SlotObserveProofV1 slot_observe=21; EtcFileProofV1 etc_file_observe=22; EtcFileProofV1 etc_file_mutate=23; RunnerFsProofV1 runner_fs_observe=24; RunnerFsProofV1 runner_fs_mutate=25; Login1ProofV1 login1_operation=26; UserUnitProofV1 user_unit_operation=27; NftProofV1 nft_operation=28; ControlProcessProofV1 control_process_observe=29; UidQuiescenceProofV1 uid_quiescence_observe=30; RuntimeSocketProofV1 runtime_socket_observe=31; RootlessDaemonCleanupProofV1 rootless_daemon_cleanup=32; } }
```

Both request and response require `schema=1`; every other value is a protocol
failure with no response. Request field15 is exactly
`SHA256("groundplane.runner-host-control.request.v1" || NUL || deterministic
protobuf bytes with field15 omitted)`. Response field19 is exactly
`SHA256("groundplane.runner-host-control.response.v1" || NUL || deterministic
protobuf bytes with field19 omitted)`. Domain strings are ASCII bytes shown,
NUL is one zero byte, and the digest field is absent rather than empty/default.

Conditional validation is not implicit generated behavior. The checked-in
canonical artifact `runner_host_control_v1.validator.json`, schema
`groundplane.runner-host-control-validator/v1`, contains exactly
`{bounds,conditional_rules,operation_profiles,path_derivations,proof_rules,
schema}`. Its runtime-artifact bytes/size/SHA are release authority. The table
exhaustively encodes the conditional rules stated here: complete Allocation and
Account; ExpectedPath absent/present identity; RELEASE executable count/order
and no files; NSS exactly eight enum-ordered files and no executables; six slot
anchors; one derived account row <=512; RunnerFs object/path/bytes/claim rules;
nft APPLY/OBSERVE/DELETE rules; process/socket/UID scan cardinalities; cleanup
counts; result/failure/proof combinations. APPLIED/NO_CHANGE require a matching
COMPLETE proof; CONFLICT and FAILED also require the operation-matching typed
COMPLETE or PARTIAL proof, never an empty proof. Runtime rejects if proto,
validator, DTO, normalizer, or vector digest differs.


Decoder rejects unknown fields/groups/extensions, duplicates, out-of-order tags,
nonminimal varints/lengths, encoded default scalars, invalid UTF-8, and any
deterministic remarshal mismatch. Response fields 1..16 equal request bytes.
All digests are 32 raw bytes; ownership nonce 32 and invocation nonce 16. IDs
use agt_/run_/tnt_/task_/op_/step_ strict uppercase ULIDs; UUID and decimal
bounds follow the journal. Zero enum is invalid. Allocation and Account fields
are complete. Conditional presence, proof order/cardinality, path derivation,
managed row <=512, desired bytes <=16384, nft bytes <=16384/rules12, process
and socket maxima, UID scan max4096/first32 offenders, and cleanup counts<=1024
are generated validators. APPLIED/NO_CHANGE require NONE+COMPLETE; CONFLICT
requires one of three conflict codes and typed COMPLETE/PARTIAL; FAILED requires
codes5..13. Response SHA domain-hashes deterministic response with field19
absent. Decode/hash failure emits no response and exits64.

Helper container name is `gp-hc-v1-<32-lowerhex-invocation_nonce>`. Its exactly
14 ASCII-key-sorted labels are agent-id, contract, contract-sha256,
create-request-sha256, effect-id, host-control-profile,
host-control-request-sha256, invocation-nonce, operation-id, ownership-nonce,
runner-id, runtime-epoch, task-id, tenant-id under `com.groundplane.*`.
Contract is `runner-host-control-helper-v1`; values equal request/release/journal.

`HelperContainerCreateV1` is a checked-in canonical DTO, not Moby Go struct
serialization. Top fields are exactly AttachStderr/Stdin/Stdout true,
Cmd `runner-host-control-helper`, Entrypoint
`/usr/local/bin/groundplane-agent`, four ordered Env PATH/LANG/LC_ALL/GOMAXPROCS,
HostConfig, RepoDigest Image, 14 Labels, profile NetworkDisabled,
`NetworkingConfig:{EndpointsConfig:{}}`, OpenStdin/StdinOnce true, Tty false,
profile User, WorkingDir `/`. HostConfig is exactly AutoRemove false, explicit
CapAdd, CapDrop ALL, CgroupnsMode, private IPC, log none, Memory/Swap128MiB,
Mounts, NanoCpus1e9, NetworkMode, PidMode, PidsLimit64, Privileged false,
ReadonlyRootfs true, Restart no/0, ordered SecurityOpt, empty UTS, userns host.
Cleanup alone is 64MiB/Pids32. Null is forbidden; explicit empty arrays remain.
Create digest is SHA-256 domain `groundplane.helper-create-intent.v1` over
RFC8785 `{body,method:"POST",path:"/v1.52/containers/create",query:{name}}`
with only the create-digest label omitted.

All mounts are Target-sorted and exactly
`{BindOptions:{CreateMountpoint:false,NonRecursive:<directory-true-file-or-socket-false>,Propagation:"rprivate"},ReadOnly,Source,Target,Type:"bind"}`.
SecurityOpt is exactly
`["apparmor=<groundplane-hc-class-v1>","no-new-privileges=true",
"seccomp=<release canonical compact JSON>"]`.

The exact `com.groundplane.host-control-profile` values and total operation
mapping are below. `PidMode=""`, `CgroupnsMode="private"`, NetworkMode `none`
and NetworkDisabled true apply unless the row states otherwise. All ordinary
profiles use 128MiB/64 PIDs; cleanup uses 64MiB/32 PIDs. Mount arrays are Target
byte-sorted and use the one locked directory/file NonRecursive rule.

- Machine RELEASE -> `machine-release-observe`: User `0:0`, CapAdd `[]`, read
  security; manifest executable i in source-path order mounts RO file to
  `/run/groundplane/host/executables/<i as 00..count-1>`, count1..32.
- Machine NSS -> `machine-nss-observe`: User `0:0`, CapAdd `[]`, read security;
  `/var/lib/dpkg/status` -> `/run/groundplane/host/dpkg-status` RO file, then
  `/etc` -> `/run/groundplane/host/etc` RO directory; no bus.
- SlotObserve -> `slot-observe`: User `0:0`, CapAdd `[]`, read security; exact S
  -> `/run/groundplane/host/slot` RO directory.
- Etc observe -> `etc-observe`: User `0:0`, CapAdd `[]`, read security; `/etc`
  -> `/run/groundplane/host/etc` RO directory.
- Etc mutate GROUP/PASSWD/SUBUID/SUBGID -> `etc-mutate-root`: User `0:0`,
  CapAdd `[]`, etc-write security, same `/etc` mount RW. GSHADOW/SHADOW ->
  `etc-mutate-shadow`, identical except CapAdd exactly `[CAP_CHOWN]` and policy
  permits chown only the derived shadow/gshadow temp and target.
- RunnerFs objects1..22 observe -> `runnerfs-root-observe`: User `0:0`, CapAdd
  `[]`, read security, S -> host/slot RO. Objects7..22 mutate ->
  `runnerfs-root-mutate`: User `0:0`, CapAdd `[]`, slot-write, S RW.
  Objects23..28 observe -> `runnerfs-tree-observe`: User U:G, CapAdd exactly
  `[CAP_DAC_READ_SEARCH]`, read, S RO. Their mutate ->
  `runnerfs-tree-mutate`: User U:G, CapAdd exactly
  `[CAP_CHOWN,CAP_DAC_OVERRIDE,CAP_FOWNER]`, slot-write, S RW.
- Login1 -> `login1`: User `0:0`, CapAdd `[]`, login1 security; exact
  `/run/dbus/system_bus_socket` -> `/run/groundplane/system-bus` RO file.
- UserUnit -> `user-systemd`: User U:G, CapAdd `[]`, user-systemd security;
  `/run/user/U/bus` -> `/run/groundplane/user-bus` RO file.
- Nft -> `nft`: User `0:0`, CapAdd exactly `[CAP_NET_ADMIN]`, nft security,
  Mounts `[]`, NetworkMode host, NetworkDisabled false, private PID/cgroup.
- ControlProcess proxy|Docker|Runner -> `process-control-observe`: User U:G,
  CapAdd `[]`, process security, PidMode/CgroupnsMode host; the one exact selected
  existing leaf cgroup -> `/run/groundplane/cgroup/control` RO directory.
- UID USER -> `process-user-observe`: User U:G, CapAdd `[]`, process security,
  host PID/cgroup; exact `/sys/fs/cgroup/user.slice/user-U.slice` ->
  `/run/groundplane/cgroup/user` RO directory. UID FINAL ->
  `process-final-observe`: same user/security/host modes with Mounts `[]`.
- Runtime PRESENT -> `runtime-sockets-present`: User U:G, CapAdd `[]`,
  socket-observe security, private PID/cgroup; proxy UDS ->
  `/run/groundplane/proxy-docker.sock`, then raw UDS ->
  `/run/groundplane/raw-docker.sock`, both RO files. Runtime ABSENT ->
  `runtime-sockets-absent`: same user/caps/security/private modes;
  `/run/user` -> `/run/groundplane/runtime` RO directory, policy limited to U.
- RootlessDaemonCleanup -> `rootless-daemon-cleanup`: User U:G, CapAdd `[]`,
  daemon-cleanup security, private PID/cgroup; raw UDS ->
  `/run/groundplane/raw-docker.sock` RO file; 64MiB/32 PIDs.

Security class names are exactly `groundplane-hc-read-v1`,
`groundplane-hc-etc-write-v1`, `groundplane-hc-slot-write-v1`,
`groundplane-hc-login1-v1`, `groundplane-hc-user-systemd-v1`,
`groundplane-hc-nft-v1`, `groundplane-hc-process-v1`,
`groundplane-hc-socket-observe-v1`, and
`groundplane-hc-daemon-cleanup-v1`. No runtime stat chooses a profile.
 All other namespaces are
private. Profiles map to exactly nine release classes: read, etc-write,
slot-write, login1, user-systemd, nft, process, socket-observe, daemon-cleanup,
all named `groundplane-hc-<class>-v1`. These digest-pinned helpers and proxy are
trusted for the semantics of netlink, D-Bus, and Docker UDS payloads; seccomp
and AppArmor reduce surface but do not make compromised trusted code harmless.

RunnerFs path mapping is exact: 1 S; 2 S/control; 3 R; 4 W; 5 E; 6 T;
7 `S/control/runner-marker-v1.json`; 8 `R/.config`; 9 `R/.config/systemd`;
10 `R/.config/systemd/user`; 11 `R/.local`; 12 `R/.local/share`; 13
`R/.local/share/docker`; 14 `S/control/claim-<claimed_effect_id>.json`; 15
`R/.config/systemd/user/groundplane-docker-proxy.service`; 16
`R/.config/systemd/user/groundplane-docker.service`; 17 `S/control/runtime.env`;
18 `S/control/daemon.json`; 19 `S/control/docker-proxy-v1.json`; 20
`S/control/resolver-upstream.conf`; 21 `S/control/resolver-embedded.conf`; 22
`S/control/registration-v1.json`; 23 contents below W; 24 below E; 25 below T;
26 below R/.config after unit objects are absent; 27 below R/.local excluding
object13 until28 completes; 28 below object13. Objects1..6 are root-observe only;
7..22 root observe/mutate; 23..28 tree observe/mutate. Object14 alone requires
claimed_effect_id and canonical claim bytes; every other rejects it. File
objects7,14..22 require desired bytes for ensure/observe; directories reject
bytes; cleanup objects accept REMOVE only.

Inspect normalization schema `groundplane.helper-container-inspect-normalized.v1`
is exactly `{"body":<HelperContainerCreateV1 reconstructed from Inspect>,
"image_id":"sha256:<64hex>","name":"gp-hc-v1-<nonce>",
"runtime_mounts":[<zero-or-more normalized raw Inspect.Mounts entries>],
"schema":"groundplane.helper-container-inspect-normalized.v1"}`. A runtime
mount is exactly `{"Destination":target,"Propagation":"rprivate","RW":bool,
"Source":source,"Type":"bind"}`, Target-sorted. The array is `[]` iff the
profile's exact HostConfig.Mounts is empty; otherwise it has identical count and
one-for-one Target-sorted correspondence: Source=Source, Destination=Target,
RW=`!ReadOnly`, Type=`bind`, and Propagation=`rprivate`. Full-ID Inspect must match raw
Id, slash-name, local image ID, RepoDigest, Path/Args, and reconstruct every
projected Config/HostConfig member one-for-one. Only CapAdd=null and
HostConfig.Mounts=null normalize to []; every other null/type difference rejects.
Unprojected fields equal the pinned safe-default vector; networking empty
EndpointsConfig is synthesized only after raw NetworkSettings fixture match.
SecurityOpt order is preserved.

Golden file is canonical JSON exactly `{api_version,moby_commit,schema,vectors}`
with schema `groundplane.helper-create-golden.v1`. ASCII-profile-sorted vectors
are exactly `{create_body_base64,create_body_sha256,create_intent_sha256,
normalized_base64,normalized_sha256,profile,raw_inspect_base64,
raw_inspect_sha256}`. Concrete fixtures cover all nineteen profiles, RELEASE
count1/count32 and control proxy/Docker/Runner leaves. Runtime recomputes and
byte-compares without template substitution. Missing proto, validator, DTO,
normalizer, security source, or golden bytes blocks release.

### Canonical lifecycle order

Materialization is exactly: lock/reserve slot; claim and separately ensure group,
user, home, subuid, subgid, data root and all files; create/bind rootful Network;
install/attest nft; install exactly two disabled units; enable linger; reload;
start proxy; prove proxy readiness; start Docker; prove processes and raw/proxy
sockets and proxy API; immediately reobserve proxy socket; create/start rootful
Runner with proxy-only Docker bind; perform token/bootstrap/listener proof;
remove helpers; seal. No Runner-UID process exists before nft.

Cleanup is exactly: stop/remove rootful Runner; run raw-daemon cleanup while raw
socket is available; stop Docker and prove raw nonaccepting; stop proxy and prove
proxy absent; remove two units; remove data root; remove rootful Network/bridge;
disable linger and TerminateUser; global zero-UID proof while nft remains;
remove nft last; remove subgid, subuid, home, user, group targets and claims;
release slot. Machine artifacts remain. For `delete_runner` this removes home and all registration paths. For `replace_runtime`, the seven exact transfer effects replace removal of R/credential/registration ancestors and leaves; old ownership remains until atomic E+1 adoption. No mutation occurs without current
authenticated Controller authority.

### Rootful Network and nft policy

Rootful preflight requires Ubuntu 24.04 ARM64, Moby 29.1.3 API1.52, IPv4
forwarding enabled, and `/v1.52/info` reporting nftables firewall backend. The
configured Runner pool is disjoint from system/environment pools, persisted
allocations, Docker IPv4 subnets, protected host-route prefixes, and the exact
host-assigned Controller IPv4/port.

For slot i, pool base P yields B=P+8*i, subnet B/29, gateway B+1, sole Runner
endpoint B+2, unused B+3..B+6, broadcast B+7. Network Create normalized bytes
are exactly:

```json
{"Attachable":false,"ConfigOnly":false,"Driver":"bridge","EnableIPv4":true,"EnableIPv6":false,"IPAM":{"Config":[{"AuxiliaryAddresses":{},"Gateway":"<B+1>","IPRange":"<B/29>","Subnet":"<B/29>"}],"Driver":"default","Options":{}},"Ingress":false,"Internal":false,"Labels":{"com.groundplane.contract":"runner-network-v1","com.groundplane.contract-sha256":"<release-contract-sha256>","com.groundplane.create-request-sha256":"<request-sha256>","com.groundplane.ownership-nonce":"<64-lowerhex>","com.groundplane.runner-id":"<runner-id>","com.groundplane.runtime-epoch":"<canonical-unsigned-decimal>","com.groundplane.tenant-id":"<tenant-id>"},"Name":"gp_runner_<runner-id>","Options":{"com.docker.network.bridge.enable_icc":"false","com.docker.network.bridge.enable_ip_masquerade":"true","com.docker.network.bridge.gateway_mode_ipv4":"nat","com.docker.network.bridge.host_binding_ipv4":"127.0.0.1","com.docker.network.bridge.inhibit_ipv4":"false","com.docker.network.bridge.name":"g<T>"},"Scope":"local"}
```

T is the first 14 lowercase unpadded-base32 characters of SHA-256 over Runner
id; bridge is `g<T>`, nft table `gp_<T>`. Unlisted fields/options are omitted.
The request digest omits only its create-digest label. Response is 201, warning
empty, nonempty full ID, request/response <=4KiB, then immediate ID evidence
fsync. Unknown result filters exact contract/release/nonce/Runner/Tenant/epoch
labels: zero permits one byte-identical retry, one exact inspect adopts, and
more/mismatch conflicts. Network never has Start. Before Runner, inspect proves
empty endpoints and exact bridge/gateway/routes; after connect exactly the owned
Runner is B+2/29 with gateway B+1 and no IPv6.

Docker alone owns bridge/veth/IPAM/route/NAT and its nft tables. Groundplane
owns only non-NAT `inet gp_<T>`. `deny4` is the canonical union of fixed special
prefixes, Groundplane pools/allocations, other Docker subnets and protected
nondefault route/address/on-link prefixes. Fixed prefixes are exactly:
`0.0.0.0/8`, `10.0.0.0/8`, `100.64.0.0/10`, `127.0.0.0/8`,
`169.254.0.0/16`, `172.16.0.0/12`, `192.0.0.0/24`, `192.0.2.0/24`,
`192.88.99.0/24`, `192.168.0.0/16`, `198.18.0.0/15`,
`198.51.100.0/24`, `203.0.113.0/24`, `224.0.0.0/4`, and `240.0.0.0/4`.
Host-exact destinations are separately blocked by `fib daddr type local`.
Prefixes are masked, numeric/prefix sorted, deduplicated, contained prefixes
removed, and exact siblings merged only without broadening. Reject /0,
inventory failure, malformed input, >128 elements, or resolver membership.
Canonical source object domain is `groundplane.runner-deny4-sources.v1` with
sorted `special`, `groundplane_pools`, `docker_subnets`, `protected_routes`.

The exact table has empty flags, owner comment
`groundplane:<runner-id>:<runtime-epoch>:<generation>`, one constant/interval
IPv4 `deny4` set with source-digest comment, exactly three filter base chains
priority -10/policy accept, and exactly these twelve rules/comments:

```text
host_input hook input
  iifname "g<T>" meta nfproto ipv4 ip saddr <B+2> ip daddr <controller-ip> tcp dport <controller-port> accept  # controller-input-allow
  iifname "g<T>" drop                                                                        # bridge-input-drop

runner_forward hook forward
  iifname "g<T>" meta nfproto ipv6 drop                                                       # runner-ipv6-drop
  iifname "g<T>" meta nfproto ipv4 ip saddr != <B+2> drop                                     # runner-source-enforce
  iifname "g<T>" meta nfproto ipv4 ip daddr @deny4 drop                                       # private-forward-drop
  oifname "g<T>" ct state { established, related } accept                                     # return-established-allow
  oifname "g<T>" drop                                                                         # return-default-drop

runner_output hook output
  meta skuid <runner-uid> meta nfproto ipv6 drop                                               # rootless-ipv6-drop
  meta skuid <runner-uid> meta nfproto ipv4 fib daddr type local drop                          # rootless-local-drop
  meta skuid <runner-uid> meta nfproto ipv4 ip daddr @deny4 drop                               # rootless-private-drop
  oifname "g<T>" ct state { established, related } accept                                     # bridge-established-allow
  oifname "g<T>" drop                                                                         # bridge-default-drop
```

There is no FORWARD Controller exception. There are no counters/logs/limits,
marks, NAT, helpers, flowtables, maps, anonymous sets, or user chains. Canonical
policy domain `groundplane.runner-nft-policy.v1` includes family/name/comments,
source digest/elements, chains/hooks/priorities/policies and every expression,
is <=16KiB, and excludes handles/counters/generation/display/Docker objects.
Initial install requires absence. Replacement, only after current Controller
authority and quiescence, verifies owner then atomically deletes/recreates the
whole same-name table in one typed nfnetlink batch; flush/per-rule mutation is
forbidden. Readback must reconstruct identical set, three chains, twelve rules,
comments and no extras before workload.

Materialization creates/binds Network while inert, then nft, before any Runner
UID process. Cleanup removes the exact Network/bridge, then terminates/proves
zero UID while nft remains, then removes nft by exact policy/handle. Recurring
proof recomputes Network/bridge/endpoint/routes and exact table. Drift marks
local conflict, refuses new Agent operations, and reports offline. Without
Controller authority it performs no stop, repair, or delete; an active hostile
job may continue until authenticated reconcile.


### Exact systemd user runtime

Proxy fragment bytes are ASCII/UTF-8, exactly one final LF, no CR/BOM/trailing
space or extra blank line:

```ini
[Unit]
Description=Groundplane Runner Docker policy proxy
Requires=dbus.socket
After=dbus.socket

[Service]
Type=notify
NotifyAccess=main
ExecStart=/usr/libexec/groundplane/runner-rootless-launch --proxy --config /var/lib/groundplane/runner-slots/slot-@@SLOT@@/control/docker-proxy-v1.json
Restart=no
TimeoutStartSec=30s
TimeoutStopSec=30s
KillMode=control-group
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
PrivateUsers=yes
ProtectSystem=strict
ProtectHome=read-only
RestrictAddressFamilies=AF_UNIX
RuntimeDirectory=groundplane-docker-proxy
RuntimeDirectoryMode=0700
RuntimeDirectoryPreserve=no
InaccessiblePaths=%t/bus
ReadWritePaths=%t/groundplane-docker-proxy
LimitCORE=0
LimitNOFILE=4096
TasksMax=128
StandardOutput=journal
StandardError=journal
SyslogIdentifier=groundplane-docker-proxy
LogRateLimitIntervalSec=30s
LogRateLimitBurst=1000

[Install]
WantedBy=default.target
```

Docker fragment has the same byte constraints and is exactly:

```ini
[Unit]
Description=Groundplane Runner rootless Docker Engine
Requires=dbus.socket groundplane-docker-proxy.service
After=dbus.socket groundplane-docker-proxy.service

[Service]
Type=notify
NotifyAccess=all
EnvironmentFile=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/control/runtime.env
ExecStart=/usr/libexec/groundplane/runner-rootless-launch
Restart=no
TimeoutStartSec=120s
TimeoutStopSec=120s
KillMode=mixed
UMask=0077
NoNewPrivileges=no
PrivateUsers=no
PrivateTmp=no
PrivateDevices=no
ProtectSystem=no
ProtectHome=no
Delegate=yes
LimitCORE=0
LimitNOFILE=262144
LimitNPROC=16384
TasksMax=8192
OOMPolicy=continue
StandardOutput=journal
StandardError=journal
SyslogIdentifier=groundplane-docker
LogRateLimitIntervalSec=30s
LogRateLimitBurst=1000

[Install]
WantedBy=default.target
```

No socket/third unit, PrivatePIDs, ProtectProc, or ProcSubset exists. Both are
disabled/Restart=no. Bootstrap proves user-manager hard limits and ancestor
pids.max permit proxy 4096/128 and Docker 262144/16384/8192; readiness records
actual soft/hard limits and ancestor bound. There are no CPU/memory unit limits.

Proxy launcher mode ignores inherited manager environment and execs the proxy
with the exact six-entry sorted env shown below: HOME, LANG, LC_ALL,
NOTIFY_SOCKET byte-identical from the user manager, PATH, and XDG_RUNTIME_DIR;
no five-entry summary or alternate env exists. It sends READY
only after validating RuntimeDirectory and binding/listening U:G0600. Before
Docker, proof is fd-stable stat/connect only, never Engine ping. After Docker
READY/raw UDS, proxy API ping succeeds and source socket inode is immediately
revalidated before Runner Create and mounted identity checked after Start.

`S/control/runtime.env` is ASCII with one final LF and exactly:

```text
HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner
XDG_CONFIG_HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.config
XDG_DATA_HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.local/share
XDG_RUNTIME_DIR=/run/user/@@UID@@
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
LANG=C.UTF-8
LC_ALL=C.UTF-8
```

For proxy mode the native launcher discards inherited manager environment and
execs `/usr/local/libexec/groundplane-runner-docker-proxy` with exact unsigned
ASCII-key-sorted env and argv:

```text
HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner
LANG=C.UTF-8
LC_ALL=C.UTF-8
NOTIFY_SOCKET=<exact-nonempty-systemd255-Type=notify-value>
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
XDG_RUNTIME_DIR=/run/user/@@UID@@
```

```text
/usr/local/libexec/groundplane-runner-docker-proxy
--config
/var/lib/groundplane/runner-slots/slot-@@SLOT@@/control/docker-proxy-v1.json
```

For Docker mode the launcher validates the seven runtime.env values and exact
nonempty Type=notify NOTIFY_SOCKET, strips every incidental manager variable,
and execs RootlessKit with exactly nine unsigned-ASCII-key-sorted env entries:

```text
DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/@@UID@@/bus
HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner
LANG=C.UTF-8
LC_ALL=C.UTF-8
NOTIFY_SOCKET=<exact-nonempty-systemd255-Type=notify-value>
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
XDG_CONFIG_HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.config
XDG_DATA_HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.local/share
XDG_RUNTIME_DIR=/run/user/@@UID@@
```

RootlessKit argv is exactly:

```text
/usr/bin/rootlesskit
--state-dir=/run/user/@@UID@@/dockerd-rootless
--net=slirp4netns
--mtu=65520
--slirp4netns-sandbox=true
--slirp4netns-seccomp=true
--disable-host-loopback
--port-driver=none
--copy-up=/etc
--copy-up=/run
--propagation=rslave
/usr/libexec/groundplane/runner-rootless-launch
--child
```

There is no pidns, detached netns, publish, cgroupns, or extra argv. At child,
launcher validates and forwards exactly ROOTLESSKIT_STATE_DIR, PARENT_EUID and
PARENT_EGID derived values. It accepts once then strips only
`_ROOTLESSKIT_REEXEC_COUNT_<pidns-inode>_<child-pid>=1`, bound to observed
positive identities. It rejects every other undocumented/unknown RootlessKit
variable and requires absent pipefd/systemd-activation variables and LISTEN_*.

After resolver/mount and proxy inode readiness, child execs exact
`/usr/bin/dockerd-rootless.sh` with fourteen unsigned-ASCII-key-sorted entries:

```text
DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/@@UID@@/bus
DOCKERD=/usr/bin/dockerd
HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner
LANG=C.UTF-8
LC_ALL=C.UTF-8
NOTIFY_SOCKET=<same-exact-systemd-value>
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ROOTLESSKIT_PARENT_EGID=@@GID@@
ROOTLESSKIT_PARENT_EUID=@@UID@@
ROOTLESSKIT_STATE_DIR=/run/user/@@UID@@/dockerd-rootless
XDG_CONFIG_HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.config
XDG_DATA_HOME=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.local/share
XDG_RUNTIME_DIR=/run/user/@@UID@@
_DOCKERD_ROOTLESS_CHILD=1
```

Script argv is exactly:

```text
/usr/bin/dockerd-rootless.sh
--host=unix:///run/user/@@UID@@/docker.sock
--data-root=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/runner/.local/share/docker
--config-file=/var/lib/groundplane/runner-slots/slot-@@SLOT@@/control/daemon.json
--userland-proxy=false
```

`DOCKERD` prevents PATH selection and `_DOCKERD_ROOTLESS_CHILD=1` prevents a
nested RootlessKit. No DOCKERD_ROOTLESS_ROOTLESSKIT, containerd state, SELinux,
Docker endpoint/proxy/activation/LISTEN or undocumented variable enters. These
fourteen are envp into pinned dash. Final dockerd has the unordered exact same
key/value set plus `PWD=R`; any other exported key fails qualification.

Each semantics object has exactly 42 closed members. Proxy canonical RFC8785
object after placeholder substitution is:

```json
{"after":["dbus.socket"],"delegate":null,"description":"Groundplane Runner Docker policy proxy","environment_files":null,"exec_start_argv":["/usr/libexec/groundplane/runner-rootless-launch","--proxy","--config","/var/lib/groundplane/runner-slots/slot-@@SLOT@@/control/docker-proxy-v1.json"],"exec_start_path":"/usr/libexec/groundplane/runner-rootless-launch","gid":"@@GID@@","inaccessible_paths":["%t/bus"],"kill_mode":"control-group","limit_core":"0","limit_nofile":"4096","limit_nproc":null,"log_rate_limit_burst":"1000","log_rate_limit_interval_usec":"30000000","no_new_privileges":true,"notify_access":"main","oom_policy":null,"private_devices":true,"private_tmp":true,"private_users":true,"protect_home":"read-only","protect_system":"strict","read_write_paths":["%t/groundplane-docker-proxy"],"requires":["dbus.socket"],"restart":"no","restrict_address_families":["AF_UNIX"],"runtime_directory":["groundplane-docker-proxy"],"runtime_directory_mode":"0700","runtime_directory_preserve":"no","schema":"groundplane.systemd-user-unit.semantic.v1","standard_error":"journal","standard_output":"journal","syslog_identifier":"groundplane-docker-proxy","systemd_version":"255","tasks_max":"128","timeout_start_usec":"30000000","timeout_stop_usec":"30000000","type":"notify","uid":"@@UID@@","umask":"0077","unit_name":"groundplane-docker-proxy.service","wanted_by":["default.target"]}
```

Docker canonical object is:

```json
{"after":["dbus.socket","groundplane-docker-proxy.service"],"delegate":true,"description":"Groundplane Runner rootless Docker Engine","environment_files":[{"ignore_missing":false,"path":"/var/lib/groundplane/runner-slots/slot-@@SLOT@@/control/runtime.env"}],"exec_start_argv":["/usr/libexec/groundplane/runner-rootless-launch"],"exec_start_path":"/usr/libexec/groundplane/runner-rootless-launch","gid":"@@GID@@","inaccessible_paths":null,"kill_mode":"mixed","limit_core":"0","limit_nofile":"262144","limit_nproc":"16384","log_rate_limit_burst":"1000","log_rate_limit_interval_usec":"30000000","no_new_privileges":false,"notify_access":"all","oom_policy":"continue","private_devices":false,"private_tmp":false,"private_users":false,"protect_home":"no","protect_system":"no","read_write_paths":null,"requires":["dbus.socket","groundplane-docker-proxy.service"],"restart":"no","restrict_address_families":null,"runtime_directory":null,"runtime_directory_mode":null,"runtime_directory_preserve":null,"schema":"groundplane.systemd-user-unit.semantic.v1","standard_error":"journal","standard_output":"journal","syslog_identifier":"groundplane-docker","systemd_version":"255","tasks_max":"8192","timeout_start_usec":"120000000","timeout_stop_usec":"120000000","type":"notify","uid":"@@UID@@","umask":"0077","unit_name":"groundplane-docker.service","wanted_by":["default.target"]}
```

Every absent supported directive is null, present empty list `[]`, and present
false `false`; unknown/duplicate member or wrong type rejects. Digest is
`sha256:` plus lowercase hex SHA-256 of domain
`groundplane.systemd-user-unit.semantic.v1`, NUL, canonical object. Fragment
digest hashes exact substituted final-LF bytes without a domain.


Publication substitutes only `@@SLOT@@`, `@@UID@@`, and `@@GID@@` in one
simultaneous byte pass. SLOT is exactly two lowercase decimal digits in
`00..N-1`; UID/GID are nonzero canonical decimals. Unknown, missing, repeated
with inconsistent value, or residual `@@[A-Z0-9_]+@@` rejects; no shell/systemd
escaping, trimming, normalization, or intermediate expansion occurs.
`R/.config`, `R/.config/systemd`, and `R/.config/systemd/user` are root0755.
Both fragments, runtime.env, daemon.json and proxy config are regular one-link
root0444 with no ACL/default ACL, xattr/file capability, symlink, hardlink or
mount crossing. Parents are not U-writable. Desired state is disabled, with no
enablement link/drop-in/alias/transient override.

Unit proof and RuntimeOwnership bind exact NOFILE soft/hard, NPROC soft/hard,
TasksMax and every ancestor `pids.max`, in addition to fragment semantics,
active/substate, MainPID/start ticks and cgroup. Process proof fills every
ProcessIdentity field for proxy, RootlessKit and dockerd. Rootless daemon proof
also binds daemon ID/version/API/arch/rootless, data root, resolver,
raw-socket identity, state-dir path/device/inode/uid/gid/mode/closed-entry digest,
and the fd-stable proxy alias MountIdentity. The alias is exactly source
`/run/user/@@UID@@/groundplane-docker-proxy/docker.sock` to
`/var/run/docker.sock`, RW, rprivate, NonRecursive=false, with source and target
dev/inode/mount IDs proved before dockerd exec, after dockerd READY, immediately
before Runner Create, and after Runner Start. Any inode or mount change
conflicts.

`AncestorPidsEvidenceV1` is sorted root-to-leaf by derived cgroup ancestry, has
unique paths, and includes every ancestor through the unit leaf. `pids_max` is
exactly `max` or a positive canonical decimal; `effective_pids_max` is the
canonical decimal minimum of every finite entry and the process limit, and
rejects when no finite effective bound exists or when it is below the required
unit value.

`ProcessEvidenceV1` populates every reusable ProcessIdentity field, including
executable device/inode/digest, cgroup, PID/mount/user/network namespace inodes,
and uid_map/gid_map digests. `RootlessRuntimeEvidenceV1` is required exactly for
Docker process proof and binds daemon identity/version/API/architecture/rootless,
full RootlessKit and dockerd process evidence, data-root identity,
daemon-config digest, resolver-view digest, raw-socket evidence, exact proxy
alias mount, state-dir identity and closed-entry digest. Mount evidence includes
mount type `bind`, ReadOnly, Propagation `rprivate`, CreateMountpoint=false and
NonRecursive; the proxy UDS alias is explicitly NonRecursive=false.



RuntimeOwnership requires both units active/running; proxy MainPID is the exact
proxy, Docker MainPID the RootlessKit parent; positive PID/start ticks, derived
cgroups, SourcePath empty, no drop-ins/aliases, actual soft/hard limits and
ancestor pids bound, and exact RootlessKit state-dir identity/closed entries.

### Rootless Docker policy proxy

Raw UDS is `/run/user/U/docker.sock`; proxy UDS is
`/run/user/U/groundplane-docker-proxy/docker.sock`, U:G0600 below runtime0700.
Rootful Runner receives only proxy UDS at `/var/run/docker.sock`.

On accept proxy opens pidfd, brackets proof with start ticks, and rechecks before
every mutation. ROOTFUL_RUNNER is exact U:G Runner process/cgroup/mntns/id.
ROOTLESS_WORKLOAD_ROOT is SO_PEERCRED U:G, exact owned running rootless
container descendant, container UID0 mapping to host U, exact uid/gid maps,
labels/cgroup/mntns/start ticks and `NoNewPrivs=1`. Nonroot workloads cannot
open Docker. Daemon/proxy/infrastructure, nested userns, stale/unowned/orphan,
PID reuse, and 0666/subordinate alternatives reject. Rootful Runner uses
Userns host but its process is U:G; container UID0 would be host root and must
remain unreachable.

Every nested ContainerCreate rejects caller SecurityOpt and injects exactly
`["no-new-privileges=true"]`; rootless namespaced default capabilities remain.
Peer reinspection proves NNP. Caller `com.groundplane.*` is denied. Every proxy
object has exactly docker-policy, runner-id, runtime-epoch, create-nonce, and
create-request-sha256 labels. Create digest omits only its own digest label.
Unknown create keeps procfd leases open and uses exact label discovery,
0-result ten-second proof plus one redispatch, exact one-candidate inspection,
and conflict for mismatch/multiple. Proxy Restart=no; process loss closes
leases, blocks start/restart/create, and requires authenticated epoch teardown.

The release-bound canonical route manifest, not Swagger, is policy authority.
It defines route/method/path grammar, every query/header/framing/body/ownership/
response rule, and matches each pinned request exactly once. API is 1.52;
DOCKER_BUILDKIT=0 and Builder V1 only. `/grpc`, `/session`, build-cancel,
BuildKit/Buildx, secrets/SSH/export/push/multiplatform, Swarm, plugins, ports,
and custom driver/IPAM remain denied. Docker Compose 2.40.3 is unsupported and
not release-qualified; it is not a distinguishable route or principal, and no
categorical Compose-detection claim exists.

Request line including CRLF is <=8192; aggregate headers <=65536 and one line
<=8192; ordinary JSON <=1048576; archive request/response <=67108864; finite
ordinary response <=16777216. Connections64, ordinary32, hijacks4, pulls4,
build1; class queue <=10s then 429. Streaming events/log/attach/exec/pull/build
has no byte cap but backpressure/concurrency/deadlines apply. Header deadline30s;
ordinary no-byte5m; wait disconnect-cancel/absolute24h; pull/build no-byte30m
and absolute6h. Only 101 upgrade or 200 hijack is accepted. Disconnect cancels
upstream; shutdown stops admission, drains ordinary5s, CloseWrites as applicable,
then closes both halves.

Audit record is exact canonical JSON <=4096 bytes:
`{schema,at,request_id,runner_id,runtime_epoch,principal,peer_pid,
peer_start_ticks,peer_uid,peer_gid,peer_mntns,peer_cgroup_sha256,method,
route_id,decision,reason,upstream_status,request_bytes,response_bytes,
duration_us}`. Schema is `groundplane.docker-proxy-audit/v1`; `at` is
RFC3339Nano `Z`, request_id 32 lowercase hex, integer/count fields canonical
decimal strings, principal/decision/reason closed enums, upstream_status null or
three-digit decimal, and cgroup is represented only by digest, never raw path.
Bodies, query strings, env, cmd, buildargs, labels, names, image refs, tar/stream,
registry auth/config and session values are forbidden. Mutation admission audit
must durably succeed before dispatch; completion is best effort and never makes
an issued mutation retryable.

For every route proxy preserves allowed upstream status, reviewed response
headers, Docker JSON and stdcopy framing byte-for-byte. Attach/exec accept only
101 TCP upgrade or 200 hijack, preserve upgrade headers, apply bounded
backpressure, propagate client and upstream half-close with CloseWrite, and do
not reinterpret stream frames. Disconnect cancels upstream; shutdown stops
admission, drains ordinary5s, half-closes applicable hijacks, then closes both
halves.


Build spool is O_TMPFILE or immediately unlinked0600 in proxy0700. Wire <=1GiB,
expanded<=4GiB, entries<=250000, normalized path<=4096, and selected Dockerfile
is exactly one regular file <=1MiB. Accept only uncompressed tar or exactly one
gzip member with its mandatory validated CRC32/ISIZE trailer; reject an
additional member or bytes after that trailer. Reject empty, absolute,
backslash, NUL/control, `.`/`..`, normalized duplicate/collision, device/FIFO/
socket, GNU sparse, escaping link, link cycle or expansion bomb. Permit only
regular file, directory, symlink and hardlink plus bounded ustar/PAX headers;
Dockerfile cannot be a link. Fully validate before one upstream byte.

Builder forces version1,pull0,rm1,forcerm1 and rejects Groundplane labels,
remote contexts/frontends. An exclusive object-mutation lease spans PREPARED
through CLEAN or CONFLICT and serializes build with pull/delete and container/
volume create/delete. Before upload, journal full image IDs with RepoTags/
RepoDigests, containers, named volumes and complete usage/reference graph.
While forwarding the classic JSON stream byte-identically, a closed API1.52
decoder accepts one or more ordered schema-valid Aux BuildResult messages, each
with `ID="sha256:<64hex>"`. The last Aux ID is the sole final-image authority,
must equal the postinventory final image, and is journaled before CLEAN. Earlier
Aux IDs are recorded only as nonfinal/cache candidates. A successful build with
no Aux, any malformed Aux, or a final-ID/postinventory mismatch conflicts. A golden human
`Successfully built <shorthex>` status may coexist or be truncated and is
non-authoritative; if a complete such line is present, its short hex must
prefix-match the final Aux ID. A preexisting cache-hit Aux final ID is valid, marked
preexisting, and never deleted. Docker29.1.3/API1.52 release qualification locks
exact single-stage and multi-stage Aux/status transcripts.

On success, postinventory must have no post-pre build containers/volumes after
cleanup; retain the final image and only proven post-pre untagged classic cache
images, journaled by full ID. On failure/cancel/ambiguous terminal, remove only
post-pre untagged images child-before-parent after proving no preexisting,
retained-final, tag/digest, container or cache reference; then remove/prove
post-pre containers/volumes. Never delete the parsed final merely because it is
untagged. Any stream/inventory/reference mismatch is CONFLICT. Data-root removal
is the cross-tenant cleanup boundary.


Before parent ContainerCreate, enumerate every target-only mount and every image
Config.Volumes target not already covered by an explicit accepted bind, tmpfs,
or current-owned named volume. Normalize and Target-sort, reject duplicate or
overlapping targets, and cap at 64. For index00..63 create local-driver volume
`gpv1_<64hex-parent-create-nonce>_<two-digit-index>` with empty DriverOpts.
Derived nonce is SHA-256 of `groundplane.proxy-derived-volume-nonce.v1`, NUL,
parent nonce bytes, NUL, uint16be index, NUL, target bytes. It carries the five
standard ownership labels plus reserved parent-container-create-nonce and
target-sha256 labels.

Journal each VolumeCreate before dispatch. Discovery filters its complete label
tuple: zero retries byte-identically within budget, one adopts only exact full
name/driver/options/labels/digest Inspect, and more or mismatch conflicts.
Rewrite parent Mount Source to the volume name and include the Target-sorted
`{target,name,derived_nonce,derived_request_digest}` tuple in parent Create
preimage. If parent Create proves zero, remove/prove all derived volumes; after
known parent full-ID absence, remove/prove each. AutoRemove destroy event or
association lookup 404 triggers the same cleanup. Proxy death never guesses or
adopts associations; authenticated epoch teardown resolves them. Engine never
creates an unlabeled anonymous volume.


Host binds accept only the exact Runner-native sets. Fixed W/E/T anchors are
immutable; mutable descendants use openat2 O_PATH beneath W, then same-UID
`/proc/<proxy-pid>/fd/N`, kept across stopped/restart/daemon restart until
parent absence. Caller proc paths and arbitrary workflow/service/Compose binds
reject. Limits are 8 leases/container,64 containers,256 live fds,path4096,
component255; 429 on bound. Every normalized directory bind has
NonRecursive=true; file/UDS false; all rprivate and CreateMountpoint=false,
Target-sorted. The proxy socket alias uses false.

### Registration document and rootful Runner

Registration bytes remain canonical `S/control/registration-v1.json`, root0444
one-link below root0755, <=16KiB, journaled and mounted RO at
`/run/groundplane/registration-v1.json`. Its exact existing schema and eight
Runner labels remain. The entrypoint is sole reader only before bootstrap
completion. It verifies bytes/labels, derives exact configure argv, then pulls
the token. Before Listener/job admission Controller durably consumes and
invalidates the registration nonce and ownership records the receipt digest.
The same container remains, so later U:G workflow code can read the inert registration document among the four Runner mounts; nested containers never get
the registration bind. Replay at every registration/config endpoint rejects.
Lifetime sole-reader would require a second container and is not MVP.

`RunnerContainerCreateV1` is a checked-in RFC8785 DTO/golden. It forces
User `U:G`, Privileged false, CapAdd[], CapDrop[ALL], readonly rootfs,
UsernsMode host, and SecurityOpt exactly
`["no-new-privileges=true","seccomp=<canonical compact Moby profile>"]`, with
existing exact private cgroup/dedicated Network/static B+2/restart-none/log/
resource fields and four Target-sorted binds: proxy UDS RW, R RW,
resolver-embedded RO, registration RO. UDS/file mounts use NonRecursive=false;
R directory true. It has no raw/rootful socket, bus, Agent/Controller, control
path, or credential mount. Exactly eight labels exist: contract
runner-container-v1, contract-sha256, create-request-sha256, ownership-nonce,
registration-document-sha256, runner-id, runtime-epoch, tenant-id. Create digest
omits only its own digest label.

Readiness inspects Config.User, Privileged, caps, ordered SecurityOpt, four
mounts, labels, and live init status: all UIDs U, all GIDs G, NoNewPrivs1, and
all capability sets zero. Setuid/setgid/file-cap fixtures cannot gain UID0 or
caps. RuntimeOwnership binds exact DTO/inspect/process/endpoint/resolver/mount
facts.

The Listener bypasses config.sh/env.sh/run.sh, rejects R/.env and inherited
variables, and receives exact ordered base env:

```text
DOCKER_API_VERSION=1.52
DOCKER_BUILDKIT=0
DOCKER_HOST=unix:///var/run/docker.sock
HOME=<R>
LANG=C.UTF-8
LC_ALL=C.UTF-8
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
TMPDIR=<W>/_temp
```

Configure gets those eight plus
`ACTIONS_RUNNER_INPUT_TOKEN=<registration-token>` ninth; run gets eight. Token is exactly 1..4096 bytes and every byte is ASCII 0x21..0x7e; this is the universal broker and configure boundary. Direct configure argv is
`./bin/Runner.Listener configure --unattended --url <github_url> --name
<derived_name> --work _work --disableupdate`, omitting `--labels` iff empty and
otherwise appending `--labels <comma-joined-canonical-labels>`. `--disableupdate`
proves local DisableUpdate=true, advertisement and exact server echo, and
persisted `.runner`; it does not claim the Listener locally rejects refresh
messages. Unexpected version/file/runtime drift conflicts Ready and requires
Controller cleanup/rematerialization.

### Secret recovery and ownership proof

Entrypoint makes the one-time SPKI-pinned Controller IPv4 pull bound to Runner,
epoch, ownership nonce, registration-document digest and current intent.
Registration and config brokers independently atomically consume one raw token.
Ownership nonce remains phase-scoped through registration completion and config
pull and becomes inert only after final bootstrap/config completion, cancel,
expiry, terminal, or cleanup. Lost token response is never resent: Runner
quiesces and Controller registration proof decides config-only recovery or
`registration_token_required`. Helpers remain secret-blind and Agent has no host
PID/proc relay. There is no registration socket, tmpfs token file, Docker exec,
fifth bind, or durable token env.

On sealed reboot, exact preserved registration files and Controller nonce
invalidation proof permit only fresh config-token recovery in the E+1
materialization, without GitHub configure or registration token. Any mismatch
terminalizes registration_token_required; a separate explicit Task is required
to obtain a new GitHub registration token. Completion is not lifecycle seal
until Agent proof and ownership seal succeed.

Initial/post-start/recurring proof compares the single RuntimeOwnership schema.
Drift marks offline/conflict and refuses new operations. Without Controller
authority it does not mutate externally; authenticated cleanup uses only sealed
identities. Host-root tampering remains outside threat model.


## Consequences

- The durable Runner desired record gains immutable `github_url` and
  release-embedded `image_ref` plus mutable Tenant-scoped `slug`; the lifecycle
  record gains the managed container id and once-allocated runtime epoch, while
  RuntimeOwnership gains the exact unit, daemon, socket, network, bridge, nft,
  container, cgroup, and directory identities required for safe cleanup.
- The public Runner model gains an exact lifecycle, Task identities, derived
  name, canonical GitHub URL, absolute observation time, and freshness-derived
  online value while keeping host allocation and runtime internals private.
- The production Controller needs a zeroing in-process attempt broker, durable
  registration and one-time-config intents, immutable Agent plans, proof
  comparison, and logical lifecycle transitions. The privileged Agent needs the sealed common.Runner entrypoint,
  process quiescence, exact account and user-unit management, one rootless
  daemon per Runner, live runtime inspection, fail-closed nftables, and
  ownership-verified local cleanup. The Controller performs none of those host
  effects directly.
- Every Groundplane release must build, publish, and compile the exact child and
  index digests of its release-selected single-platform `linux/arm64` Runner image
  quickly enough to satisfy GitHub's supported Runner-version policy.
- Controller restart intentionally loses unconsumed registration tokens;
  failure and fresh retry are part of the public contract rather than an
  availability defect.
- Existing unsafe Console Runner guidance and fixture mutation must be removed
  cleanly when this contract is implemented; no compatibility path preserves
  host-socket, durable-token-environment, arbitrary-name, or fabricated-online behavior.
- ADR 0033 remains authoritative only for the combined per-Tenant five quota, no
  cross-Tenant reach, one `/29` per Runner, retained fences, local-only removal,
  excluded future features, and isolation/lifecycle clauses not explicitly
  replaced here. ADR 0050 cleanly supersedes ADR 0033's `/29` allocation source
  and `system_pool` coupling: machine-supplied `runner.network_pool` is the sole
  allocation source.
- C17 cannot become Accepted from this ADR alone. Product, API/CLI, Console,
  generated contracts, implementation ledger, automated evidence, and Ubuntu
  24.04 host evidence must land as one coherent vertical contract.
