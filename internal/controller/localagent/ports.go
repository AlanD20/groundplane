// Package localagent owns the durable state machine for the one Controller-managed
// local Agent. Infrastructure adapters implement the ports in this file; Docker,
// etcd, runtime-path, and channel details do not cross this seam.
package localagent

import (
	"context"
	"time"
)

// Phase is the durable lifecycle phase of the singleton local Agent.
type Phase string

const (
	PhaseProvisioning Phase = "provisioning"
	PhaseReady        Phase = "ready"
	PhaseDeleting     Phase = "deleting"
)

const (
	// ReadyTimeout is the accepted enrollment readiness deadline.
	ReadyTimeout = 120 * time.Second
	// RemovedTaskReason is the durable abort reason used during Agent removal.
	RemovedTaskReason = "agent_removed"
)

// Config is the durable, Controller-owned runtime configuration served to the
// authenticated Agent. Durations are stored as seconds to match the wire contract.
type Config struct {
	PullIntervalSeconds int32             `json:"pull_interval_seconds"`
	MaxConcurrentTasks  int32             `json:"max_concurrent_tasks"`
	Labels              map[string]string `json:"labels,omitempty"`
}

// Credential contains only the durable-safe representation of the channel token.
// EncryptedToken remains until final record deletion; Digest is removed atomically
// when the record enters PhaseDeleting.
type Credential struct {
	EncryptedToken []byte `json:"encrypted_token"`
	Digest         string `json:"digest,omitempty"`
}

// Record is the durable singleton aggregate. It is an internal persistence
// contract, not an operator-facing API response.
type Record struct {
	ID               string     `json:"id"`
	EnrollmentTaskID string     `json:"enrollment_task_id"`
	Image            string     `json:"image"`
	Generation       uint64     `json:"generation"`
	Phase            Phase      `json:"phase"`
	Config           Config     `json:"config"`
	Credential       Credential `json:"credential"`
	CreatedAt        time.Time  `json:"created_at"`
	ReadyAt          time.Time  `json:"ready_at,omitempty"`
}

// StoredRecord carries the repository CAS revision independently of the record.
type StoredRecord struct {
	Record   Record
	Revision int64
}

// Repository owns singleton CAS and the transactional credential digest lookup.
// BeginDelete must remove the digest lookup in the same transaction that changes
// the phase. Delete must remove the primary record, encrypted token, and indexes.
type Repository interface {
	CreateSingleton(ctx context.Context, record Record) (StoredRecord, error)
	GetSingleton(ctx context.Context) (StoredRecord, error)
	UpdateConfig(
		ctx context.Context,
		id string,
		generation uint64,
		revision int64,
		config Config,
	) (StoredRecord, error)
	MarkReady(ctx context.Context, id string, generation uint64, revision int64, readyAt time.Time) (StoredRecord, error)
	BeginDelete(ctx context.Context, id string, generation uint64, revision int64) (StoredRecord, error)
	Delete(ctx context.Context, id string, generation uint64, revision int64) error
}

// RuntimeMaterial is the complete secret-bearing input needed to recreate the
// Controller-owned config and token files. Implementations must not retain or log
// EncryptedToken after the call.
type RuntimeMaterial struct {
	AgentID        string
	Generation     uint64
	Config         Config
	EncryptedToken []byte
}

// Runtime generates durable-safe credentials and reconciles their host runtime
// material. GenerateCredential must not create runtime files; materialization is
// allowed only after CreateSingleton commits the durable record.
type Runtime interface {
	GenerateCredential(ctx context.Context, agentID string) (Credential, error)
	Materialize(ctx context.Context, material RuntimeMaterial) error
	Remove(ctx context.Context, agentID string) error
}

// ContainerDesired is the infrastructure-neutral identity of the Agent container.
type ContainerDesired struct {
	AgentID    string
	Image      string
	Generation uint64
}

// Container converges and removes only the Controller-owned singleton container.
// Both operations must be idempotent. Remove must reject an ownership mismatch.
type Container interface {
	Converge(ctx context.Context, desired ContainerDesired) error
	Remove(ctx context.Context, agentID string, generation uint64) error
}

// SessionSnapshot is the non-secret authenticated channel state used for health.
type SessionSnapshot struct {
	Generation uint64
	Online     bool
	Revoked    bool
	LastReady  time.Time
	Capacity   int32
	Version    string
}

// Sessions owns authenticated readiness and generation fencing. Ready must be a
// race-free subscription: it returns an already-closed channel when a matching
// generation has already reported Ready. Removal operations are idempotent when
// the matching session is already absent or offline.
type Sessions interface {
	Ready(ctx context.Context, agentID string, generation uint64) (<-chan struct{}, error)
	Snapshot(agentID string) (SessionSnapshot, bool)
	StopAssignments(ctx context.Context, agentID string, generation uint64) error
	Revoke(ctx context.Context, agentID string, generation uint64) error
	WaitOffline(ctx context.Context, agentID string, generation uint64) error
}

// Tasks aborts all active work assigned to one exact Agent generation. The
// maximum is the durable configured concurrency bound; success means every
// observed assignment has committed a terminal acknowledgement.
type Tasks interface {
	AbortActive(ctx context.Context, agentID string, generation uint64, maximum int32, reason string) error
}

// Timer is the cancellable timer surface used by readiness deadlines.
type Timer interface {
	C() <-chan time.Time
	Stop()
}

// Clock makes deadlines and health projections deterministic in tests.
type Clock interface {
	Now() time.Time
	NewTimer(duration time.Duration) Timer
}

// SystemClock is the production wall-clock adapter.
type SystemClock struct{}

func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}

func (SystemClock) NewTimer(duration time.Duration) Timer {
	return &systemTimer{timer: time.NewTimer(duration)}
}

type systemTimer struct {
	timer *time.Timer
}

func (timer *systemTimer) C() <-chan time.Time {
	return timer.timer.C
}

func (timer *systemTimer) Stop() {
	timer.timer.Stop()
}
