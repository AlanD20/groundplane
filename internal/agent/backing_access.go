package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type BackingAccessChecker interface {
	Check(context.Context, *agentpb.CheckBackingAccess) (*agentpb.BackingAccessResult, error)
}

func (client *Client) SetBackingAccessChecker(checker BackingAccessChecker) error {
	if client == nil || checker == nil {
		return errs.New(errs.KindValidationFailed, "Agent Backing access checker is required")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.started {
		return errs.New(errs.KindStateConflict, "Agent Backing access checker cannot change after start")
	}
	client.backingAccess = checker
	return nil
}

// One bounded worker belongs to this authenticated stream. Only the stream
// loop writes results. Cancellation joins cleanup before the slot is reused.
type backingAccessSession struct {
	checker   BackingAccessChecker
	outputs   chan *agentpb.BackingAccessResult
	requestID string
	cancel    context.CancelFunc
	workers   sync.WaitGroup
}

func newBackingAccessSession(checker BackingAccessChecker) *backingAccessSession {
	return &backingAccessSession{checker: checker, outputs: make(chan *agentpb.BackingAccessResult, 1)}
}

func (session *backingAccessSession) start(
	ctx context.Context,
	request *agentpb.CheckBackingAccess,
) (*agentpb.BackingAccessResult, error) {
	if err := backingaccess.ValidateMachineRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if session.requestID != "" || session.checker == nil {
		return unavailableBackingAccess(request, backingaccess.RuntimeUnavailable), nil
	}
	owned := proto.CloneOf(request)
	ctx, cancel := context.WithTimeout(ctx, backingaccess.ExchangeTimeout)
	session.requestID, session.cancel = owned.RequestId, cancel
	session.workers.Add(1)
	go func() {
		defer session.workers.Done()
		defer cancel()
		defer backingaccess.ClearMachineRequest(owned)
		result, err := session.checker.Check(ctx, owned)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil || backingaccess.ValidateMachineResult(owned, result) != nil {
			failure := backingaccess.MachineFailure(err)
			if failure == "" {
				failure = backingaccess.ExecutionFailure
			}
			result = unavailableBackingAccess(owned, failure)
		} else {
			result = proto.CloneOf(result)
		}
		session.outputs <- result
	}()
	return nil, nil
}

func (session *backingAccessSession) Close() error {
	if session.cancel != nil {
		session.cancel()
	}
	session.workers.Wait()
	session.requestID, session.cancel = "", nil
	select {
	case <-session.outputs:
	default:
	}
	return nil
}

func unavailableBackingAccess(request *agentpb.CheckBackingAccess, failure string) *agentpb.BackingAccessResult {
	return &agentpb.BackingAccessResult{RequestId: request.RequestId, TaskId: request.TaskId,
		HelperName: backingaccess.HelperName(request.TaskId), CleanupRequired: true, Failure: failure}
}

func (session *backingAccessSession) handle(
	ctx context.Context,
	stream agentStream,
	message *agentpb.ControllerMessage,
) (bool, error) {
	request, cancel := message.GetCheckBackingAccess(), message.GetCancelBackingAccess()
	if request == nil && cancel == nil {
		return false, nil
	}
	if proto.Size(message) > backingaccess.MaximumEnvelopeBytes || len(message.ProtoReflect().GetUnknown()) != 0 {
		return true, errs.New(errs.KindValidationFailed, "Agent Backing access envelope is invalid")
	}
	if cancel != nil {
		if err := backingaccess.ValidateMachineCancel(cancel); err != nil {
			return true, err
		}
		if session.requestID == cancel.RequestId {
			return true, session.Close()
		}
		return true, nil
	}
	result, err := session.start(ctx, request)
	if err != nil || result == nil {
		return true, err
	}
	return true, sendBackingAccess(ctx, stream, result)
}

func sendBackingAccess(ctx context.Context, stream agentStream, result *agentpb.BackingAccessResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(
		&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingAccessResult{BackingAccessResult: result}},
	)
}
