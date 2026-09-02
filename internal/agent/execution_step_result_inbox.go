package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type executionStepResultKey struct {
	taskID       string
	assignmentID string
	stepID       string
}

type executionStepResultWaiter struct {
	request   *agentpb.ExecutionStepResultRequest
	ack       chan *agentpb.ExecutionStepResultAck
	abandoned bool
}

type executionStepResultInbox struct {
	mu      sync.Mutex
	pending map[executionStepResultKey]*executionStepResultWaiter
}

func newExecutionStepResultInbox() *executionStepResultInbox {
	return &executionStepResultInbox{pending: make(map[executionStepResultKey]*executionStepResultWaiter)}
}

func (inbox *executionStepResultInbox) Register(
	request *agentpb.ExecutionStepResultRequest,
) (<-chan *agentpb.ExecutionStepResultAck, func(), error) {
	if inbox == nil {
		return nil, nil, errs.New(errs.KindInternal, "agent: execution step result inbox is not configured")
	}
	validated, err := executionplan.ValidateExecutionStepResultRequest(request)
	if err != nil {
		return nil, nil, err
	}
	key := executionStepResultRequestKey(validated)
	waiter := &executionStepResultWaiter{
		request: validated,
		ack:     make(chan *agentpb.ExecutionStepResultAck, 1),
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, exists := inbox.pending[key]; exists {
		return nil, nil, errs.New(errs.KindStateConflict, "agent: execution step result is already pending")
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

func (inbox *executionStepResultInbox) Accept(ack *agentpb.ExecutionStepResultAck) error {
	if inbox == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: execution step result acknowledgement is missing")
	}
	key := executionStepResultAckKey(ack)
	inbox.mu.Lock()
	waiter := inbox.pending[key]
	if waiter == nil {
		inbox.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: execution step result acknowledgement is unexpected")
	}
	if _, err := executionplan.ValidateExecutionStepResultAck(ack, waiter.request); err != nil {
		inbox.mu.Unlock()
		return err
	}
	delete(inbox.pending, key)
	abandoned := waiter.abandoned
	inbox.mu.Unlock()
	if !abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.ExecutionStepResultAck)
	}
	return nil
}

func executionStepResultRequestKey(request *agentpb.ExecutionStepResultRequest) executionStepResultKey {
	return executionStepResultKey{
		taskID: request.GetTaskId(), assignmentID: request.GetAssignmentId(),
		stepID: request.GetResult().GetStepId(),
	}
}

func executionStepResultAckKey(ack *agentpb.ExecutionStepResultAck) executionStepResultKey {
	return executionStepResultKey{
		taskID: ack.GetTaskId(), assignmentID: ack.GetAssignmentId(), stepID: ack.GetStepId(),
	}
}

func (p *WorkerPool) CheckpointExecutionStepResult(
	ctx context.Context,
	assignment *Assignment,
	result *agentpb.ExecutionStepResult,
) error {
	if ctx == nil || assignment == nil || p == nil || p.executionStepResults == nil {
		return errs.New(errs.KindInternal, "agent: execution step result transport is not configured")
	}
	request, err := executionplan.ValidateExecutionStepResultRequest(&agentpb.ExecutionStepResultRequest{
		TaskId: assignment.TaskID, AssignmentId: assignment.AssignmentID, Result: result,
	})
	if err != nil {
		return err
	}
	acknowledged, abandon, err := p.executionStepResults.Register(request)
	if err != nil {
		return err
	}
	select {
	case p.outputs <- WorkerOutput{ExecutionStepResult: request}:
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
	select {
	case <-acknowledged:
		for _, existing := range assignment.AcknowledgedStepResults {
			if existing.GetStepId() != request.GetResult().GetStepId() {
				continue
			}
			if !proto.Equal(existing, request.GetResult()) {
				return errs.New(errs.KindStateConflict, "agent: acknowledged execution step result conflicts with assignment")
			}
			return nil
		}
		assignment.AcknowledgedStepResults = append(
			assignment.AcknowledgedStepResults,
			proto.Clone(request.GetResult()).(*agentpb.ExecutionStepResult),
		)
		return nil
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
}

func (p *WorkerPool) AcceptExecutionStepResultAck(ack *agentpb.ExecutionStepResultAck) error {
	if p == nil || p.executionStepResults == nil {
		return errs.New(errs.KindInternal, "agent: execution step result transport is not configured")
	}
	return p.executionStepResults.Accept(ack)
}
