// Package workexecution owns the execution lifecycle shared by scheduling,
// persistence and host executors. It contains no operation implementation or I/O.
package workexecution

import "time"

// Operation selects a compiled operation owner, never an arbitrary executable.
type Operation string

const (
	Release             Operation = "release"
	ServiceLifecycle    Operation = "service_lifecycle"
	ServiceRemoval      Operation = "service_removal"
	BackingProvision    Operation = "backing_provision"
	Attach              Operation = "attach"
	Detach              Operation = "detach"
	Script              Operation = "script"
	Network             Operation = "network"
	Volume              Operation = "volume"
	Component           Operation = "component"
	Environment         Operation = "environment"
	Hierarchy           Operation = "hierarchy"
	RemovalBackupObject Operation = "removal_backup_object"
	Backup              Operation = "backup"
	BackupPrune         Operation = "backup_prune"
	Restore             Operation = "restore"
	Image               Operation = "image"
	Runner              Operation = "runner"
	ControllerSoftware  Operation = "controller_software"
	AgentSoftware       Operation = "agent_software"
	SoftwarePreparation Operation = "software_preparation"
	SoftwareStaging     Operation = "software_staging"
	ConnectorCheck      Operation = "connector_check"
	ConnectorRemoval    Operation = "connector_removal"
	SecretRemoval       Operation = "secret_removal"
	ScriptRemoval       Operation = "script_removal"
	BackingCheck        Operation = "backing_check"
	DNSCheck            Operation = "dns_check"
	EdgeCheck           Operation = "edge_check"
	RunnerCheck         Operation = "runner_check"
	SecretRotation      Operation = "secret_rotation"
	EntrySettlement     Operation = "entry_settlement"
	EntryConfiguration  Operation = "entry_configuration"
	ResourceDefinition  Operation = "resource_definition"
	ReleaseGroup        Operation = "release_group"
	AgentLifecycle      Operation = "agent_lifecycle"
	EtcdConfiguration   Operation = "etcd_configuration"
)

type Executor string

const (
	Controller Executor = "controller"
	Agent      Executor = "agent"
)

// Placement is immutable once prepared. A Controller operation may also target
// a host, but host-independent work leaves HostID empty.
type Placement struct {
	Executor Executor `json:"executor"`
	HostID   string   `json:"host_id,omitempty"`
}

// Input refers to immutable operation-owned material, not to diagnostic history.
// Its digest is computed over the complete captured input by the operation owner.
type Input struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
}

// Claim is an exclusive effect scope. ScopeID is a stable resource ID. HostID
// distinguishes host-local objects from control-plane resource ownership.
type Claim struct {
	HostID  string `json:"host_id,omitempty"`
	ScopeID string `json:"scope_id"`
}

type Phase string

const (
	Waiting   Phase = "waiting"
	Ready     Phase = "ready"
	Assigned  Phase = "assigned"
	Running   Phase = "running"
	Attention Phase = "attention"
	Settled   Phase = "settled"
)

type Outcome string

const (
	Succeeded  Outcome = "succeeded"
	Failed     Outcome = "failed"
	Cancelled  Outcome = "cancelled"
	Superseded Outcome = "superseded"
)

type Direction string

const (
	Forward  Direction = "forward"
	Recovery Direction = "recovery"
)

type Cleanup string

const (
	CleanupNone     Cleanup = "none"
	CleanupRequired Cleanup = "required"
	CleanupRunning  Cleanup = "running"
	CleanupStopped  Cleanup = "stopped"
	CleanupComplete Cleanup = "complete"
)

type WaitReason string

const (
	WaitInput       WaitReason = "input"
	WaitDependency  WaitReason = "dependency"
	WaitConflict    WaitReason = "conflict"
	WaitReservation WaitReason = "reservation"
	WaitExecutor    WaitReason = "executor"
	WaitCapacity    WaitReason = "capacity"
	WaitRetry       WaitReason = "retry"
)

type Wait struct {
	Reason     WaitReason `json:"reason"`
	WorkItemID string     `json:"work_item_id,omitempty"`
	TaskID     string     `json:"task_id,omitempty"`
	Until      *time.Time `json:"until,omitempty"`
}

// Item is the resource execution record. Public Task identity is its parent,
// not a second Task manufactured to obtain an Agent assignment.
type Item struct {
	ID              string    `json:"id"`
	TaskID          string    `json:"task_id"`
	Operation       Operation `json:"operation"`
	TargetID        string    `json:"target_id"`
	Placement       Placement `json:"placement"`
	Dependencies    []string  `json:"dependencies,omitempty"`
	Claims          []Claim   `json:"claims"`
	Input           *Input    `json:"input,omitempty"`
	Phase           Phase     `json:"phase"`
	Wait            *Wait     `json:"wait,omitempty"`
	AttemptNumber   uint64    `json:"attempt_number"`
	ActiveAttemptID string    `json:"active_attempt_id,omitempty"`
	Outcome         Outcome   `json:"outcome,omitempty"`
	FirstFailure    *Input    `json:"first_failure,omitempty"`
	Result          *Input    `json:"result,omitempty"`
	CleanupResult   *Input    `json:"cleanup_result,omitempty"`
	Cleanup         Cleanup   `json:"cleanup"`
	ForwardRevoked  bool      `json:"forward_revoked"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	Deadline        time.Time `json:"deadline"`
}

// Identity binds the assignment to one authenticated executor incarnation. A
// connection loss never changes this identity or authorizes another incarnation.
type Identity struct {
	Executor   Executor `json:"executor"`
	HostID     string   `json:"host_id,omitempty"`
	AgentID    string   `json:"agent_id,omitempty"`
	Generation uint64   `json:"generation"`
}

type Attempt struct {
	ID         string     `json:"id"`
	WorkItemID string     `json:"work_item_id"`
	TaskID     string     `json:"task_id"`
	Number     uint64     `json:"number"`
	Executor   Identity   `json:"executor"`
	Direction  Direction  `json:"direction"`
	Input      Input      `json:"input"`
	AssignedAt time.Time  `json:"assigned_at"`
	Deadline   time.Time  `json:"deadline"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Outcome    Outcome    `json:"outcome,omitempty"`
	Result     *Input     `json:"result,omitempty"`
}

// Checkpoint is safety state, not a display event. Operation-owned immutable
// evidence is referenced rather than copied into every journal entry.
type Checkpoint struct {
	AttemptID  string    `json:"attempt_id"`
	Sequence   uint64    `json:"sequence"`
	StageID    string    `json:"stage_id"`
	Evidence   Input     `json:"evidence"`
	RecordedAt time.Time `json:"recorded_at"`
}
