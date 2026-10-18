// Package lifecycleruntime executes captured Service lifecycle procedures.
package lifecycleruntime

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Input struct {
	continuation *lifecycleexecution.Continuation
	recovery     *lifecycleexecution.Recovery
	attempt      workexecution.Attempt
	procedure    *agentpb.ServiceLifecycleWorkInput
}

func DecodeAssignment(message *agentpb.ServiceLifecycleAssignment, authenticated workexecution.Identity) (Input, error) {
	if message == nil || proto.Size(message) > executionplan.MaximumPlanBytes || executionplan.RejectUnknown(message) != nil ||
		executionplan.ValidateServiceLifecycleWorkInput(message.Input) != nil {
		return Input{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, authenticated)
	if err != nil {
		return Input{}, err
	}
	if message.Input.CapturedSource.InputId != attempt.Input.ID ||
		hex.EncodeToString(message.Input.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return Input{}, invalidInput()
	}
	input := Input{attempt: attempt, procedure: proto.CloneOf(message.Input)}
	if attempt.Direction == workexecution.Recovery {
		if len(message.Continuation) != 0 {
			return Input{}, invalidInput()
		}
		source, err := lifecycleexecution.DecodeRecovery(input.procedure, message.RecoverySource)
		if err != nil || source.ValidateFor(attempt, input.procedure) != nil || source.Progress == nil || source.Progress.BeganAt == nil {
			return Input{}, invalidInput()
		}
		input.recovery = &source
	} else if attempt.Direction != workexecution.Forward || len(message.RecoverySource) != 0 {
		return Input{}, invalidInput()
	} else if len(message.Continuation) != 0 {
		continuation, err := lifecycleexecution.DecodeContinuation(input.procedure, message.Continuation)
		if err != nil || continuation.ValidateFor(attempt, input.procedure) != nil {
			return Input{}, invalidInput()
		}
		input.continuation = &continuation
	} else if attempt.Number != 1 {
		return Input{}, invalidInput()
	}
	return input, nil
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
	return errs.New(errs.KindValidationFailed, "Service lifecycle message does not match its captured assignment")
}
