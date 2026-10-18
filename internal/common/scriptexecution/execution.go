package scriptexecution

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Request struct {
	TaskID       string
	OperationID  string
	WorkItemID   string
	AttemptID    string
	AssignedAt   time.Time
	ExecutionID  string
	Input        workexecution.Input
	Projection   *agentpb.ScriptRunnerProjection
	BodyMetadata *agentpb.ScriptBodyArtifactMetadata
	Body         []byte
	Entries      []*agentpb.ScriptEntryArtifact
}

type BodyEvidence struct {
	SHA256 []byte
	UID    uint32
	GID    uint32
	Device uint64
	Inode  uint64
	Leaf   string
}

type ContainerEvidence struct {
	ID                    string
	OwnershipLabelsSHA256 []byte
}

type ContainerRecovery struct {
	Found    bool
	Evidence ContainerEvidence
}

type RunResult struct {
	ExitCode int32
}

// QuiescenceProof records only whether the captured process can still run.
// An exit observed after Stop is not evidence that its Script succeeded.
type QuiescenceProof struct {
	ContainerID string    `json:"container_id,omitempty"`
	Absent      bool      `json:"absent"`
	ObservedAt  time.Time `json:"observed_at"`
}

// QuiescenceRequest contains identity only: stopping a captured container must
// not require reading Script plaintext or Entry secret values again.
type QuiescenceRequest struct {
	TaskID, WorkItemID, OperationID, ExecutionID string
	Projection                                   *agentpb.ScriptRunnerProjection
}

type CleanupProof struct {
	ContainerID              string `json:"container_id,omitempty"`
	BodyDevice               uint64 `json:"body_device"`
	BodyInode                uint64 `json:"body_inode"`
	BodyLeaf                 string `json:"body_leaf"`
	ContainerAbsent          bool   `json:"container_absent"`
	BodyAbsent               bool   `json:"body_absent"`
	ExecutionDirectoryAbsent bool   `json:"execution_directory_absent"`
}

// CleanupRequest carries captured ownership and artifact metadata only. It
// cannot authorize execution and contains neither Script nor Entry plaintext.
type CleanupRequest struct {
	QuiescenceRequest
	BodyMetadata *agentpb.ScriptBodyArtifactMetadata
}

type Engine interface {
	Quiesce(context.Context, QuiescenceRequest) (QuiescenceProof, error)
	PrepareBody(context.Context, Request) (BodyEvidence, error)
	RecoverContainer(context.Context, Request, BodyEvidence) (ContainerRecovery, error)
	CreateContainer(context.Context, Request, BodyEvidence) (ContainerEvidence, error)
	RunContainer(context.Context, Request, BodyEvidence, ContainerEvidence) (RunResult, error)
	Cleanup(CleanupRequest, *BodyEvidence, *ContainerEvidence) (CleanupProof, error)
	Close() error
}
