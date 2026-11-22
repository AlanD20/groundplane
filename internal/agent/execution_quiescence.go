package agent

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/agent/containerterminal"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ExecutionQuiescenceChecker performs the physical, read-only half of Agent
// maintenance admission. It must not stop helpers, settle work or alter local
// journals merely to make the check succeed.
type ExecutionQuiescenceChecker interface {
	RequireQuiescent(context.Context) error
}

func (client *Client) SetExecutionQuiescenceChecker(checker ExecutionQuiescenceChecker) error {
	if client == nil || checker == nil {
		return errs.New(errs.KindValidationFailed, "Agent execution quiescence checker is required")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.started {
		return errs.New(errs.KindStateConflict, "Agent execution quiescence checker cannot change after start")
	}
	client.executionQuiescence = checker
	return nil
}

type executionOwnershipQuiescence interface {
	Quiescent() bool
}

func localExecutionQuiescent(
	ctx context.Context,
	workers *executionWorkers,
	ownership executionOwnershipQuiescence,
	images *imageSession,
	observations *observationSession,
	backingAccess *backingAccessSession,
	dnsChecks *dnsCheckSession,
	edgeChecks *edgeCheckSession,
	terminals *containerterminal.Session,
	physical ExecutionQuiescenceChecker,
) bool {
	if ctx == nil || ctx.Err() != nil || !workers.quiescent() || ownership == nil || !ownership.Quiescent() ||
		images == nil || images.busy || observations == nil || observations.requestID != "" ||
		backingAccess == nil || backingAccess.requestID != "" || dnsChecks == nil || dnsChecks.requestID != "" ||
		edgeChecks == nil || edgeChecks.requestID != "" || terminals == nil || !terminals.Quiescent() ||
		physical == nil {
		return false
	}
	return physical.RequireQuiescent(ctx) == nil
}

// handleExecutionQuiescence runs synchronously in the sole receive loop. No
// later Controller message can start local activity before this reply is sent.
// The Controller owns the corresponding admission hold across the exchange.
func (client *Client) handleExecutionQuiescence(
	ctx context.Context,
	stream agentStream,
	message *agentpb.ControllerMessage,
	workers *executionWorkers,
	ownership executionOwnershipQuiescence,
	images *imageSession,
	observations *observationSession,
	backingAccess *backingAccessSession,
	dnsChecks *dnsCheckSession,
	edgeChecks *edgeCheckSession,
	terminals *containerterminal.Session,
) (bool, error) {
	request := message.GetCheckExecutionQuiescence()
	if request == nil {
		return false, nil
	}
	if proto.Size(message) > 256 || executionplan.RejectUnknown(message) != nil ||
		!validExecutionQuiescenceRequestID(request.RequestId) {
		return true, errs.New(errs.KindValidationFailed, "Agent execution quiescence request is invalid")
	}
	quiescent := localExecutionQuiescent(
		ctx,
		workers,
		ownership,
		images,
		observations,
		backingAccess,
		dnsChecks,
		edgeChecks,
		terminals,
		client.executionQuiescence,
	)
	result := &agentpb.ExecutionQuiescenceResult{RequestId: request.RequestId, Quiescent: quiescent}
	if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ExecutionQuiescenceResult{
		ExecutionQuiescenceResult: result,
	}}); err != nil {
		return true, err
	}
	return true, nil
}

func validExecutionQuiescenceRequestID(value string) bool {
	if len(value) != 32 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}
