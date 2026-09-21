package agentchannel

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"io"
	"time"
)

const tokenSize = 32

// Token is the decoded, fixed-size Agent credential presented to an Authenticator.
type Token [tokenSize]byte

// Authorization is the non-secret result of successful Agent authentication.
type Authorization struct {
	Generation uint64
	Config     *agentpb.AgentConfig
}

// Authenticator authenticates one Agent generation without retaining or
// returning its credential.
type Authenticator interface {
	Authenticate(context.Context, string, Token) (Authorization, error)
	Configuration(context.Context, string, uint64) (*agentpb.AgentConfig, error)
}

// TaskStore is the durable execution seam used by one authenticated stream.
// Its etcd DTOs remain inside the Controller daemon and never cross the human
// API boundary.
type TaskStore interface {
	ListAgentAssignments(context.Context, string, uint64, int32) ([]etcd.TaskAssignment, error)
	ReconnectAgentAssignment(context.Context, etcd.TaskAssignment) (etcd.TaskAssignment, error)
	ClaimNextTask(context.Context, string, uint64, time.Time) (etcd.TaskAssignment, bool, error)
	GetTask(context.Context, string) (etcdstore.Versioned[etcd.TaskRecord], error)
	ListTaskEvents(context.Context, string, int64) (etcd.TaskEventSnapshot, error)
	AppendTaskEvent(context.Context, etcd.TaskEventInput, time.Time) (etcd.TaskEventAppend, error)
	AcknowledgeTask(
		context.Context,
		string,
		uint64,
		string,
		string,
		taskjournal.TaskStatus,
		etcd.TaskResultRecord,
		time.Time,
	) (etcdstore.Versioned[etcd.TaskRecord], error)
}
type environmentCreationTaskStore interface {
	AcknowledgeEnvironmentCreation(
		context.Context,
		string,
		uint64,
		string,
		string,
		string,
		taskjournal.TaskStatus,
		etcd.TaskResultRecord,
		time.Time,
	) (etcdstore.Versioned[etcd.TaskRecord], error)
}

// PlanResolver deterministically rebuilds one Task's ephemeral execution plan
// from retained etcd inputs. Rendered artifacts are never persisted.
type PlanResolver interface {
	ResolveExecutionPlan(context.Context, etcd.TaskRecord) (*agentpb.ExecutionPlan, error)
}

type ScriptArtifactResolver interface {
	ResolveScriptAssignmentArtifacts(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
	) (*agentpb.ScriptAssignmentArtifacts, error)
	ResolveScriptExecutionCheckpoints(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
	) ([]*agentpb.ScriptExecutionCheckpoint, error)
}

// MaterializationResolver returns one task-owned transient plaintext source.
// The channel takes ownership and closes it on every path.
type MaterializationResolver interface {
	ResolveMaterialization(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		*agentpb.ExecutionStep,
	) (io.ReadCloser, error)
}

type ManagedConfigSource struct {
	MediaType string
	Length    uint64
	Content   io.ReadCloser
}

// ManagedConfigResolver reconstructs one immutable Component artifact from
// durable, revision-pinned inputs. The channel owns and closes Content.
type ManagedConfigResolver interface {
	ResolveManagedConfig(
		context.Context,
		etcd.TaskRecord,
		*agentpb.ExecutionPlan,
		*agentpb.ExecutionStep,
	) (ManagedConfigSource, error)
}

// BackupSecretSlotResolver returns task-owned transient plaintext slots. The
// channel takes ownership of every returned buffer and clears it on every path.
type BackupSecretSlotResolver interface {
	ResolveBackupSecretSlots(
		context.Context,
		backupsecret.Request,
	) (map[agentpb.BackupSecretSlotPurpose][]byte, error)
}

// BackupCheckpointer durably accepts one Agent operation boundary before the
// stream acknowledges that the next side effect is authorized.
type BackupCheckpointer interface {
	CheckpointBackup(
		context.Context,
		string,
		uint64,
		*agentpb.BackupCheckpointRequest,
	) (*agentpb.BackupCheckpointAck, error)
}

type BackingHookCheckpointer interface {
	CheckpointBackingHook(
		context.Context,
		string,
		uint64,
		*agentpb.BackingHookCheckpointRequest,
	) (*agentpb.BackingHookCheckpointAck, error)
}
