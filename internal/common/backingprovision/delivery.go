package backingprovision

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Assignment struct {
	Attempt   workexecution.Attempt
	Procedure *agentpb.BackingProvisionInput
	Recovery  *Recovery
}

func (assignment Assignment) Validate() error {
	if _, err := Open(assignment.Attempt, assignment.Procedure); err != nil {
		return err
	}
	if assignment.Attempt.Direction == workexecution.Forward {
		if assignment.Recovery != nil {
			return invalid("forward backing creation cannot carry cleanup authority")
		}
		return nil
	}
	layout, err := CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil {
		return err
	}
	if assignment.Recovery == nil {
		return invalid("backing creation cleanup has no captured failure")
	}
	return assignment.Recovery.ValidateFor(assignment.Attempt, layout)
}

func EncodeAssignment(assignment Assignment) (*agentpb.BackingProvisionAssignment, error) {
	if err := assignment.Validate(); err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return nil, err
	}
	var recovery []byte
	if assignment.Recovery != nil {
		layout, err := CaptureLayout(assignment.Attempt, assignment.Procedure)
		if err != nil {
			return nil, err
		}
		recovery, err = EncodeRecovery(*assignment.Recovery, layout)
		if err != nil {
			return nil, err
		}
	}
	return &agentpb.BackingProvisionAssignment{
		Attempt:        attempt,
		Input:          proto.CloneOf(assignment.Procedure),
		RecoverySource: recovery,
	}, nil
}

func DecodeAssignment(
	message *agentpb.BackingProvisionAssignment,
	identity workexecution.Identity,
) (Assignment, error) {
	if message == nil || proto.Size(message) > MaximumInputBytes+2*MaximumProgressBytes+4096 ||
		executionplan.RejectUnknown(message) != nil {
		return Assignment{}, invalid("backing creation assignment is missing or oversized")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return Assignment{}, err
	}
	procedure, err := Open(attempt, message.Input)
	if err != nil {
		return Assignment{}, err
	}
	assignment := Assignment{Attempt: attempt, Procedure: procedure}
	if len(message.RecoverySource) != 0 {
		layout, err := CaptureLayout(attempt, procedure)
		if err != nil {
			return Assignment{}, err
		}
		recovery, err := DecodeRecovery(message.RecoverySource, layout)
		if err != nil {
			return Assignment{}, err
		}
		assignment.Recovery = &recovery
	}
	return assignment, assignment.Validate()
}
