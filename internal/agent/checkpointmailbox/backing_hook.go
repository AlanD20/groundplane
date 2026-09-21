package checkpointmailbox

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"sync"
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

type BackingHookInbox struct {
	mu      sync.Mutex
	pending map[backingHookCheckpointKey]*backingHookCheckpointWaiter
}

func NewBackingHookInbox() *BackingHookInbox {
	return &BackingHookInbox{pending: make(map[backingHookCheckpointKey]*backingHookCheckpointWaiter)}
}

// Register owns correlation state for an already validated checkpoint request.
func (inbox *BackingHookInbox) Register(
	validated *agentpb.BackingHookCheckpointRequest,
) (<-chan *agentpb.BackingHookCheckpointAck, func(), error) {
	key := backingHookCheckpointKey{
		taskID: validated.GetTaskId(), assignmentID: validated.GetAssignmentId(),
		stepID: validated.GetStepId(), state: validated.GetState(),
	}
	waiter := &backingHookCheckpointWaiter{
		request: validated, ack: make(chan *agentpb.BackingHookCheckpointAck, 1),
	}
	inbox.mu.Lock()
	if _, exists := inbox.pending[key]; exists {
		inbox.mu.Unlock()
		return nil, nil, errs.New(errs.KindStateConflict, "agent: backing hook checkpoint is already pending")
	}
	inbox.pending[key] = waiter
	inbox.mu.Unlock()
	return waiter.ack, func() {
		inbox.mu.Lock()
		waiter.abandoned = true
		inbox.mu.Unlock()
	}, nil
}
func (inbox *BackingHookInbox) Accept(ack *agentpb.BackingHookCheckpointAck) error {
	key := backingHookCheckpointKey{
		taskID: ack.GetTaskId(), assignmentID: ack.GetAssignmentId(), stepID: ack.GetStepId(), state: ack.GetState(),
	}
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
	ClearBackingHookRequest(waiter.request)
	abandoned := waiter.abandoned
	inbox.mu.Unlock()
	if !abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.BackingHookCheckpointAck)
	}
	return nil
}

func ClearBackingHookRequest(request *agentpb.BackingHookCheckpointRequest) {
	if request == nil {
		return
	}
	for _, fact := range request.GetFacts() {
		if fact != nil {
			clear(fact.Value)
			fact.Value = nil
		}
	}
	request.Facts = nil
}
