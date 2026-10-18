package agent

import (
	"context"
	"sync"

	contract "github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type EdgeConnectionChecker interface {
	Check(context.Context, *agentpb.CheckEdgeConnections) (*agentpb.EdgeConnectionResult, error)
}

func (client *Client) SetEdgeConnectionChecker(checker EdgeConnectionChecker) error {
	if client == nil || checker == nil {
		return errs.New(errs.KindValidationFailed, "Agent Tunnel checker is required")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.started {
		return errs.New(errs.KindStateConflict, "Agent Tunnel checker cannot change after start")
	}
	client.edgeChecks = checker
	return nil
}

// A stream owns one bounded worker. Only its writer sends results, and
// cancellation joins helper cleanup before the slot can accept new work.
type edgeCheckSession struct {
	checker   EdgeConnectionChecker
	outputs   chan *agentpb.EdgeConnectionResult
	requestID string
	cancel    context.CancelFunc
	workers   sync.WaitGroup
}

func newEdgeCheckSession(checker EdgeConnectionChecker) *edgeCheckSession {
	return &edgeCheckSession{checker: checker, outputs: make(chan *agentpb.EdgeConnectionResult, 1)}
}

func (session *edgeCheckSession) start(
	ctx context.Context,
	request *agentpb.CheckEdgeConnections,
) (*agentpb.EdgeConnectionResult, error) {
	if err := contract.ValidateMachineRequest(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if session.requestID != "" || session.checker == nil {
		return unavailableEdgeCheck(request, contract.AgentUnavailable), nil
	}
	owned := proto.CloneOf(request)
	ctx, cancel := context.WithTimeout(ctx, contract.ExchangeTimeout)
	session.requestID, session.cancel = owned.RequestId, cancel
	session.workers.Add(1)
	go func() {
		defer session.workers.Done()
		defer cancel()
		result, err := session.checker.Check(ctx, owned)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil || contract.ValidateMachineResult(owned, result) != nil {
			failure := contract.MachineFailure(err)
			if failure == "" {
				failure = contract.ExecutionFailure
			}
			result = unavailableEdgeCheck(owned, failure)
		} else {
			result = proto.CloneOf(result)
		}
		session.outputs <- result
	}()
	return nil, nil
}

func (session *edgeCheckSession) close() {
	if session.cancel != nil {
		session.cancel()
	}
	session.workers.Wait()
	session.requestID, session.cancel = "", nil
	select {
	case <-session.outputs:
	default:
	}
}

func unavailableEdgeCheck(request *agentpb.CheckEdgeConnections, failure string) *agentpb.EdgeConnectionResult {
	return &agentpb.EdgeConnectionResult{RequestId: request.RequestId, TaskId: request.TaskId,
		ConnectorContainerId: request.ConnectorContainerId, HelperName: contract.HelperName(request.TaskId),
		CleanupOnly: request.CleanupOnly, CleanupRequired: true, Failure: failure}
}

func (session *edgeCheckSession) handle(
	ctx context.Context,
	stream agentStream,
	message *agentpb.ControllerMessage,
) (bool, error) {
	request, cancel := message.GetCheckEdgeConnections(), message.GetCancelEdgeConnections()
	if request == nil && cancel == nil {
		return false, nil
	}
	if proto.Size(message) > contract.MaximumEnvelopeBytes+16 || len(message.ProtoReflect().GetUnknown()) != 0 {
		return true, errs.New(errs.KindValidationFailed, "Agent Tunnel check envelope is invalid")
	}
	if cancel != nil {
		if err := contract.ValidateMachineCancel(cancel); err != nil {
			return true, err
		}
		if session.requestID == cancel.RequestId {
			session.close()
		}
		return true, nil
	}
	result, err := session.start(ctx, request)
	if err != nil || result == nil {
		return true, err
	}
	return true, sendEdgeCheck(ctx, stream, result)
}

func sendEdgeCheck(ctx context.Context, stream agentStream, result *agentpb.EdgeConnectionResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return stream.Send(
		&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EdgeConnectionResult{EdgeConnectionResult: result}},
	)
}
