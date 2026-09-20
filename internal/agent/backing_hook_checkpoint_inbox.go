package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backingHookCheckpointKey struct {
	taskID       string
	assignmentID string
	stepID       string
	state        agentpb.BackingHookCheckpointState
}

type backingHookCheckpointWaiter struct {
	request   *agentpb.BackingHookCheckpointRequest
	ack       chan *agentpb.BackingHookCheckpointAck
	abandoned bool
}

type backingHookCheckpointInbox struct {
	mu      sync.Mutex
	pending map[backingHookCheckpointKey]*backingHookCheckpointWaiter
}

func newBackingHookCheckpointInbox() *backingHookCheckpointInbox {
	return &backingHookCheckpointInbox{pending: make(map[backingHookCheckpointKey]*backingHookCheckpointWaiter)}
}

func (p *WorkerPool) checkpointBackingHook(
	ctx context.Context,
	request *agentpb.BackingHookCheckpointRequest,
) (*agentpb.BackingHookCheckpointAck, error) {
	if p == nil || p.backingHookCheckpoints == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "agent: backing hook checkpoint transport is not configured")
	}
	validated, err := executionplan.ValidateBackingHookCheckpointRequest(request)
	if err != nil {
		return nil, err
	}
	key := backingHookCheckpointKey{
		taskID: validated.GetTaskId(), assignmentID: validated.GetAssignmentId(),
		stepID: validated.GetStepId(), state: validated.GetState(),
	}
	waiter := &backingHookCheckpointWaiter{
		request: validated, ack: make(chan *agentpb.BackingHookCheckpointAck, 1),
	}
	inbox := p.backingHookCheckpoints
	inbox.mu.Lock()
	if _, exists := inbox.pending[key]; exists {
		inbox.mu.Unlock()
		return nil, errs.New(errs.KindStateConflict, "agent: backing hook checkpoint is already pending")
	}
	inbox.pending[key] = waiter
	inbox.mu.Unlock()
	defer func() {
		inbox.mu.Lock()
		waiter.abandoned = true
		inbox.mu.Unlock()
	}()
	select {
	case p.outputs <- WorkerOutput{BackingHookCheckpoint: proto.Clone(validated).(*agentpb.BackingHookCheckpointRequest)}:
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

func (p *WorkerPool) AcceptBackingHookCheckpointAck(ack *agentpb.BackingHookCheckpointAck) error {
	if p == nil || p.backingHookCheckpoints == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: backing hook checkpoint acknowledgement is missing")
	}
	key := backingHookCheckpointKey{
		taskID: ack.GetTaskId(), assignmentID: ack.GetAssignmentId(), stepID: ack.GetStepId(), state: ack.GetState(),
	}
	inbox := p.backingHookCheckpoints
	inbox.mu.Lock()
	waiter := inbox.pending[key]
	if waiter == nil {
		inbox.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: backing hook checkpoint acknowledgement is unexpected")
	}
	if err := executionplan.ValidateBackingHookCheckpointAck(ack, waiter.request); err != nil {
		inbox.mu.Unlock()
		return err
	}
	delete(inbox.pending, key)
	clearBackingHookCheckpointRequest(waiter.request)
	abandoned := waiter.abandoned
	inbox.mu.Unlock()
	if !abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.BackingHookCheckpointAck)
	}
	return nil
}
