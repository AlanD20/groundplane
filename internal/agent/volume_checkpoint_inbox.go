package agent

import (
	"context"
	"sync"

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

type volumeCheckpointWaiter struct {
	request   *agentpb.VolumeRemovalCheckpointRequest
	ack       chan *agentpb.VolumeRemovalCheckpointAck
	abandoned bool
}

type volumeCheckpointInbox struct {
	mu      sync.Mutex
	pending map[string]*volumeCheckpointWaiter
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
	waiter := &volumeCheckpointWaiter{request: owned, ack: make(chan *agentpb.VolumeRemovalCheckpointAck, 1)}
	inbox := p.volumeCheckpoints
	inbox.mu.Lock()
	if inbox.pending[owned.RequestId] != nil {
		inbox.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "agent: Volume checkpoint is already pending")
	}
	inbox.pending[owned.RequestId] = waiter
	inbox.mu.Unlock()
	defer func() {
		inbox.mu.Lock()
		waiter.abandoned = true
		inbox.mu.Unlock()
	}()
	select {
	case p.outputs <- WorkerOutput{VolumeCheckpoint: owned}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case ack := <-waiter.ack:
		return ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *WorkerPool) AcceptVolumeRemovalCheckpointAck(ack *agentpb.VolumeRemovalCheckpointAck) error {
	if p == nil || p.volumeCheckpoints == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: Volume checkpoint acknowledgement is missing")
	}
	inbox := p.volumeCheckpoints
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	waiter := inbox.pending[ack.RequestId]
	if waiter == nil {
		return errs.New(errs.KindStateConflict, "agent: unexpected Volume checkpoint acknowledgement")
	}
	if err := executionplan.ValidateVolumeRemovalCheckpointAck(ack, waiter.request); err != nil {
		return err
	}
	delete(inbox.pending, ack.RequestId)
	if !waiter.abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.VolumeRemovalCheckpointAck)
	}
	return nil
}
