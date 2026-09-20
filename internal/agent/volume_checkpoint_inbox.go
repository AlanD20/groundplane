package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// WorkerOutput is a closed ordered union. Exactly one member is non-nil, and
// each Task's terminal step progress is emitted before its final result.
type WorkerOutput struct {
	Progress              *TaskProgress
	Result                *TaskResult
	BackupCheckpoint      *agentpb.BackupCheckpointRequest
	ScriptCheckpoint      *agentpb.ScriptCheckpointRequest
	BackingHookCheckpoint *agentpb.BackingHookCheckpointRequest
	VolumeCheckpoint      *agentpb.VolumeRemovalCheckpointRequest
}

func (p *WorkerPool) CheckpointVolumeRemoval(
	ctx context.Context,
	request *agentpb.VolumeRemovalCheckpointRequest,
) (*agentpb.VolumeRemovalCheckpointAck, error) {
	if p == nil || p.volumeCheckpoints == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "agent: Volume checkpoint transport is not configured")
	}
	if err := executionplan.ValidateVolumeRemovalCheckpointRequest(request); err != nil {
		return nil, err
	}
	owned := proto.Clone(request).(*agentpb.VolumeRemovalCheckpointRequest)
	acks, abandon, err := p.volumeCheckpoints.Register(owned)
	if err != nil {
		return nil, err
	}
	defer abandon()
	select {
	case p.outputs <- WorkerOutput{VolumeCheckpoint: owned}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case ack := <-acks:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *WorkerPool) AcceptVolumeRemovalCheckpointAck(ack *agentpb.VolumeRemovalCheckpointAck) error {
	if p == nil || p.volumeCheckpoints == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: Volume checkpoint acknowledgement is missing")
	}
	return p.volumeCheckpoints.Accept(ack)
}
