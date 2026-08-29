package scriptexecution

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Request struct {
	TaskID       string
	OperationID  string
	AssignmentID string
	ExecutionID  string
	StepID       string
	PlanHash     []byte
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

type CleanupProof struct {
	ContainerID              string
	BodyDevice               uint64
	BodyInode                uint64
	BodyLeaf                 string
	ContainerAbsent          bool
	BodyAbsent               bool
	ExecutionDirectoryAbsent bool
}

type Engine interface {
	PrepareBody(context.Context, Request) (BodyEvidence, error)
	RecoverContainer(context.Context, Request, BodyEvidence) (ContainerRecovery, error)
	CreateContainer(context.Context, Request, BodyEvidence) (ContainerEvidence, error)
	RunContainer(context.Context, Request, BodyEvidence, ContainerEvidence) (RunResult, error)
	Cleanup(Request, *BodyEvidence, *ContainerEvidence) (CleanupProof, error)
	Close() error
}
