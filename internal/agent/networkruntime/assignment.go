// Package networkruntime executes captured Network procedures on their assigned host.
package networkruntime

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Input struct {
	attempt   workexecution.Attempt
	procedure *agentpb.NetworkWorkInput
	recovery  *networkexecution.Recovery
	boundary  networkexecution.Progress
}

func DecodeAssignment(message *agentpb.NetworkWorkAssignment, authenticated workexecution.Identity) (Input, error) {
	if message == nil || proto.Size(message) > executionplan.MaximumPlanBytes || executionplan.RejectUnknown(message) != nil {
		return Input{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, authenticated)
	if err != nil {
		return Input{}, err
	}
	if attempt.Direction == workexecution.Recovery {
		procedure, source, boundary, err := executionplan.OpenNetworkRecoveryInput(attempt, message)
		if err != nil {
			return Input{}, err
		}
		return Input{attempt: attempt, procedure: procedure, recovery: &source, boundary: boundary}, nil
	}
	if len(message.RecoverySource) != 0 || len(message.RecoveryBoundary) != 0 {
		return Input{}, invalidInput()
	}
	procedure, err := executionplan.OpenNetworkWorkInput(attempt, message.Input)
	if err != nil {
		return Input{}, err
	}
	return Input{attempt: attempt, procedure: procedure}, nil
}

func (input Input) WithStart(ack *agentpb.WorkStartAck) (Input, bool, error) {
	if input.procedure == nil {
		return Input{}, false, invalidInput()
	}
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, input.attempt)
	if err != nil {
		return Input{}, false, err
	}
	input.attempt.StartedAt = &started
	return input, reconcile, nil
}

func invalidInput() error {
	return errs.New(errs.KindValidationFailed, "Network execution message does not match its captured assignment")
}
