package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type scriptCheckpointKey struct {
	taskID       string
	assignmentID string
	stepID       string
	executionID  string
	state        agentpb.ScriptExecutionState
}

type scriptCheckpointWaiter struct {
	request   *agentpb.ScriptCheckpointRequest
	ack       chan *agentpb.ScriptCheckpointAck
	abandoned bool
}

type scriptCheckpointInbox struct {
	mu      sync.Mutex
	pending map[scriptCheckpointKey]*scriptCheckpointWaiter
}

func newScriptCheckpointInbox() *scriptCheckpointInbox {
	return &scriptCheckpointInbox{pending: make(map[scriptCheckpointKey]*scriptCheckpointWaiter)}
}

func (inbox *scriptCheckpointInbox) Register(
	request *agentpb.ScriptCheckpointRequest,
) (<-chan *agentpb.ScriptCheckpointAck, func(), error) {
	if inbox == nil || request == nil {
		return nil, nil, errs.New(errs.KindInternal, "agent: Script checkpoint inbox is not configured")
	}
	key := scriptCheckpointRequestKey(request)
	waiter := &scriptCheckpointWaiter{
		request: proto.Clone(request).(*agentpb.ScriptCheckpointRequest),
		ack:     make(chan *agentpb.ScriptCheckpointAck, 1),
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, exists := inbox.pending[key]; exists {
		return nil, nil, errs.New(errs.KindStateConflict, "agent: Script checkpoint is already pending")
	}
	inbox.pending[key] = waiter
	return waiter.ack, func() {
		inbox.mu.Lock()
		defer inbox.mu.Unlock()
		if current := inbox.pending[key]; current == waiter {
			current.abandoned = true
		}
	}, nil
}

func (inbox *scriptCheckpointInbox) Accept(ack *agentpb.ScriptCheckpointAck) error {
	if inbox == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: Script checkpoint acknowledgement is missing")
	}
	key := scriptCheckpointAckKey(ack)
	inbox.mu.Lock()
	waiter := inbox.pending[key]
	if waiter == nil {
		inbox.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Script checkpoint acknowledgement is unexpected")
	}
	if _, err := executionplan.ValidateScriptCheckpointAck(ack, waiter.request); err != nil {
		inbox.mu.Unlock()
		return err
	}
	delete(inbox.pending, key)
	abandoned := waiter.abandoned
	inbox.mu.Unlock()
	if !abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.ScriptCheckpointAck)
	}
	return nil
}

func scriptCheckpointRequestKey(request *agentpb.ScriptCheckpointRequest) scriptCheckpointKey {
	return scriptCheckpointKey{
		taskID: request.GetTaskId(), assignmentID: request.GetAssignmentId(), stepID: request.GetStepId(),
		executionID: request.GetScriptExecutionId(), state: request.GetState(),
	}
}

func scriptCheckpointAckKey(ack *agentpb.ScriptCheckpointAck) scriptCheckpointKey {
	return scriptCheckpointKey{
		taskID: ack.GetTaskId(), assignmentID: ack.GetAssignmentId(), stepID: ack.GetStepId(),
		executionID: ack.GetScriptExecutionId(), state: ack.GetState(),
	}
}

func (p *WorkerPool) CheckpointScript(ctx context.Context, request *agentpb.ScriptCheckpointRequest) error {
	if ctx == nil || p == nil || p.scriptCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Script checkpoint transport is not configured")
	}
	validated, err := executionplan.ValidateScriptCheckpointRequest(request)
	if err != nil {
		return err
	}
	acknowledged, abandon, err := p.scriptCheckpoints.Register(validated)
	if err != nil {
		return err
	}
	select {
	case p.outputs <- WorkerOutput{ScriptCheckpoint: proto.Clone(validated).(*agentpb.ScriptCheckpointRequest)}:
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
	select {
	case <-acknowledged:
		return nil
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
}

func (p *WorkerPool) AcceptScriptCheckpointAck(ack *agentpb.ScriptCheckpointAck) error {
	if p == nil || p.scriptCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Script checkpoint transport is not configured")
	}
	return p.scriptCheckpoints.Accept(ack)
}
