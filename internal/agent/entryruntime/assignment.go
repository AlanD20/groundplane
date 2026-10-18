// Package entryruntime executes captured Entry files and selected workloads.
package entryruntime

import (
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Input struct {
	attempt   workexecution.Attempt
	procedure *agentpb.EntryConfigurationWorkInput
	recovery  *entryexecution.Recovery
	boundary  entryexecution.Progress
}

func DecodeAssignment(message *agentpb.EntryConfigurationWorkAssignment, authenticated workexecution.Identity) (Input, error) {
	if message == nil || proto.Size(message) > executionplan.MaximumPlanBytes || executionplan.RejectUnknown(message) != nil {
		return Input{}, invalidInput()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, authenticated)
	if err != nil {
		return Input{}, err
	}
	procedure, err := executionplan.OpenEntryConfigurationWorkInput(attempt, message.Input)
	if err != nil {
		return Input{}, err
	}
	if attempt.Direction == workexecution.Recovery {
		source, err := entryexecution.DecodeRecovery(message.RecoverySource, procedure)
		if err != nil {
			return Input{}, err
		}
		if err := source.ValidateFor(attempt, procedure); err != nil {
			return Input{}, err
		}
		boundary, err := entryexecution.DecodeProgress(source.Forward, procedure, message.RecoveryBoundary)
		if err != nil {
			return Input{}, err
		}
		checkpoint, err := boundary.Checkpoint(source.Forward, procedure)
		if err != nil || source.Failure.Checkpoint == nil || checkpoint != *source.Failure.Checkpoint {
			return Input{}, invalidInput()
		}
		return Input{attempt: attempt, procedure: procedure, recovery: &source, boundary: boundary}, nil
	}
	if attempt.Direction != workexecution.Forward || len(message.RecoverySource) != 0 || len(message.RecoveryBoundary) != 0 {
		return Input{}, invalidInput()
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
	return errs.New(errs.KindValidationFailed, "Entry execution message does not match its captured assignment")
}
