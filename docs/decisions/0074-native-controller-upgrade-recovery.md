# ADR 0074: Native Controller release activation and recovery

- Status: Accepted within the owner-approved upgrade-safety initiative
- Date: 2026-09-10
- Replaces: the staged-binary Agent/Component update paragraph in `mvp.md`
- Preserves: ADR0010 Agent replacement, ADR0016 native bootstrap, ADR0041 Tasks

## Decision

The Controller is not a Component. Its update is a native Controller Task,
never an Agent assignment or application Script. The Controller service remains
the sole persistent Groundplane unit. Application containers, data, routes,
Agent identity and queued application Tasks survive the operation.

### Immutable input and surfaces

Deployment tooling stages a root-owned release directory below
`/var/lib/groundplane/controller-updates/releases/<release-sha256>`. A bounded
JSON manifest pins Controller binary SHA256, display version, OCI Agent digest,
persistent-state epoch and Agent-channel schema. Release identity is SHA256 of
the exact manifest bytes. Both manifest and binary are rechecked before use.
Equal storage epoch and channel schema are mandatory; this operation does not
migrate incompatible state. Distribution and signatures remain deployment
concerns. No HTTP upload, arbitrary path/URL, shell command or plugin input.

Normal surfaces: Update on `/platform/controller`, `groundplane controller
update --release sha256:<digest>`, and `POST /controller/update` with
`{release:"sha256:<digest>"}` → `202 {task_id}`. Existing Host reads expose
installed Controller digest, staged candidate and latest update summary. The
request freezes release/predecessor identities and Agent image/generation in
its protected idempotent Task. Repeating an equal key returns that Task even
while active; a changed release mismatches. Lost publication responses resolve
against the same protected durable marker, never a second activation. Only one native
activation may be active. Retry is a fresh explicit update after recovery, not
blind host-effect replay.

The last qualified manifest supplies desired Agent image selection for subsequent
bodyless enrollment/update actions, overriding bootstrap `agent.image` without
editing YAML. New selections fail with `resource.in_use` during unfinished native
recovery. Before replacing a Healthy journal, the store fsyncs its validated
release into private `selected.json`; a crash on either side retains the same
successful selection. Healthy journal content is authoritative until replacement.
Failed/cancelled later trials never promote their manifest or fall back past the
last successful release. Deployment's private canonical `candidate.json` selects
one staged release digest for display; reads verify its manifest and binary.

### Handoff and recovery

1. Validate manifest, binary, compatibility and installed recovery guard.
   No candidate code runs yet.
2. Pause Agent admission, drain admitted preparations/sends and wait up to 120
   seconds for active work. Never abort it. Native work is already serialized by
   the Controller executor. Busy deadline/pre-activation cancellation releases
   the hold and retains the current runtime.
3. Retain the exact predecessor executable and persist a private, bounded,
   fsynced activation journal tied to the Task and frozen identities, after
   successful drain and before activation. A restarting Controller restores admission holds before
   opening its Agent channel. This is host handoff evidence, not a second Task
   journal or an operator resource.
   Startup rejects an unfinished journal without its matching durable Task claim.
4. A transient systemd service runs the retained predecessor's fixed private
   recovery mode. It survives Controller stop and performs only the closed
   swap/start/readiness/restore procedure. It atomically replaces the executable,
   starts the candidate and waits for the same Task to resume and prove
   its actual running executable digest, every HTTP listener, the Agent listener,
   a fresh etcd health read and authenticated Agent readiness.
5. The candidate resumes the same Task and uses ADR0010 to update the Agent to
   the release-pinned digest. Failure restores the prior Controller and, if
   changed, Agent image using fresh generation/token authority. Successful
   recovery is a failed update, never false update success.
6. A predecessor-owned `ExecStartPre` guard permits one journaled candidate
   startup attempt. Repeated startup or an interrupted unfinished trial restores
   the predecessor before `ExecStart`, even if the candidate cannot execute.
   The attempt is fenced to its kernel boot id; a new boot restores the
   predecessor because the transient watchdog did not survive reboot.
   A watchdog bounds a live but unready candidate. Journal transitions, binary
   swaps and rollback are replay-safe. Neither route needs a healthy candidate.

Before stopping for rollback, the watchdog CAS-claims `stopping`. A racing
startup guard may restore predecessor bytes in that phase but leaves the phase
unchanged; no Controller may qualify recovery until the pending stop completes.
If guard recovery won first, the watchdog cannot claim the stop. This prevents
a stale rollback read from stopping an already recovered Controller. A watchdog
whose settled operation has been replaced by a newer journal exits successfully
so systemd collects it instead of retrying obsolete recovery indefinitely.

Host summary reads the newest retained native Task through a closed,
descending one-record index. Publication and retention update this index in
the same existing Task transactions, and reads verify primary and index at one
revision. Activation files supplement phase only; they do not omit a queued or
pre-activation-failed Task or create an independent success record. Null means
no retained candidate/history, not an empty synthetic object. Metadata failures
disable updates without converting otherwise known Host health to unknown.

The Task deadline is 600 seconds, including drain, startup, Agent update and
recovery. Recovery retains evidence and safety holds on its own failure and
continues at normal startup. After committed activation, Abort returns
`resource.in_use`: cancellation cannot kill the recovery owner. Before activation
ordinary exact Task Abort applies. Clients retain/reread the Task id across the
brief API disconnect.

### Bootstrap and ownership

Bootstrap installs Controller and root-owned recovery executables before
enabling the sole service. Private `cmd/controller` recovery modes are process
dispatch, not public CLI actions. Release staging/guard installation belong to
the explicit deployment/bootstrap exception. An older binary cannot retroactively
supply this guarantee; the first upgrade-capable QA baseline needs bootstrap.

Shared pure values: `internal/common/controllerupgrade`; orchestration:
`internal/controller/controllerupgrade`; journal/binary filesystem mechanics:
`internal/infra/controllerrelease`; unit control: `internal/infra/systemd`.
`internal/app` only composes. No generic host executor or second persistent
supervision service.

## Proof and sources

Interrupt every journal/swap boundary; reject malformed/symlinked/mutable/
incompatible inputs; prove active-work drain, same-Task restart and bad-candidate
rollback under continuous application HTTP/WebSocket traffic.

systemd can own a detached transient service; `Type=exec` verifies execution,
not merely fork. `ExecStartPre` precedes the main executable. These support the
handoff; Groundplane readiness still needs its own checks, not systemctl success.
See upstream [systemd-run v255](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd-run.xml)
and [systemd.service v255](https://raw.githubusercontent.com/systemd/systemd/v255/man/systemd.service.xml).
The kernel's [boot identity](https://www.kernel.org/doc/html/v6.9/admin-guide/sysctl/kernel.html#random)
is stable within one boot and distinguishes recovery after host restart.
The Linux man-pages [`/proc/pid/exe` contract](https://man7.org/linux/man-pages/man5/proc_pid_exe.5.html)
provides the executing binary even after its installation pathname is replaced;
qualification hashes `/proc/self/exe`, not the potentially newer installed path.
