package checkpointmailbox

import (
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

type ScriptInbox struct {
	mu      sync.Mutex
	pending map[scriptCheckpointKey]*scriptCheckpointWaiter
}

func NewScriptInbox() *ScriptInbox {
	return &ScriptInbox{pending: make(map[scriptCheckpointKey]*scriptCheckpointWaiter)}
}

func (inbox *ScriptInbox) Register(
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

func (inbox *ScriptInbox) Accept(ack *agentpb.ScriptCheckpointAck) error {
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
