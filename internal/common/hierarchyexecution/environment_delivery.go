// Package hierarchyexecution owns native host execution for hierarchy removal.
package hierarchyexecution

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type EnvironmentInput struct {
	Attempt  workexecution.Attempt
	Removal  *agentpb.EnvironmentRemovalInput
	Recovery *EnvironmentRecovery
}

func (input EnvironmentInput) Validate() error {
	if input.Attempt.Validate() != nil || executionplan.ValidateEnvironmentRemovalInput(input.Removal) != nil {
		return invalidEnvironmentProgress()
	}
	if input.Attempt.Direction == workexecution.Forward {
		if input.Recovery != nil {
			return invalidEnvironmentProgress()
		}
		return nil
	}
	if input.Attempt.Direction != workexecution.Recovery || input.Recovery == nil {
		return invalidEnvironmentProgress()
	}
	return input.Recovery.ValidateFor(input.Attempt)
}

func DecodeEnvironmentAssignment(message *agentpb.EnvironmentRemovalAssignment, identity workexecution.Identity) (EnvironmentInput, error) {
	if message == nil || message.Removal == nil || proto.Size(message) > executionplan.MaximumPlanBytes {
		return EnvironmentInput{}, errs.New(errs.KindValidationFailed, "Environment removal assignment is missing or oversized")
	}
	if err := executionplan.RejectUnknown(message); err != nil {
		return EnvironmentInput{}, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return EnvironmentInput{}, err
	}
	input := EnvironmentInput{Attempt: attempt, Removal: proto.Clone(message.Removal).(*agentpb.EnvironmentRemovalInput)}
	if len(message.Recovery) != 0 {
		source, err := DecodeEnvironmentRecovery(message.Recovery)
		if err != nil {
			return EnvironmentInput{}, err
		}
		input.Recovery = &source
	}
	if err := input.Validate(); err != nil {
		return EnvironmentInput{}, err
	}
	return input, nil
}

func EncodeEnvironmentAssignment(input EnvironmentInput) (*agentpb.EnvironmentRemovalAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	message := &agentpb.EnvironmentRemovalAssignment{Attempt: attempt, Removal: proto.Clone(input.Removal).(*agentpb.EnvironmentRemovalInput)}
	if input.Recovery != nil {
		message.Recovery, err = EncodeEnvironmentRecovery(*input.Recovery)
		if err != nil {
			return nil, err
		}
	}
	if proto.Size(message) > executionplan.MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal assignment is oversized")
	}
	return message, nil
}

func (input EnvironmentInput) WithStart(ack *agentpb.WorkStartAck) (EnvironmentInput, bool, error) {
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, input.Attempt)
	if err != nil {
		return EnvironmentInput{}, false, err
	}
	input.Attempt.StartedAt = &started
	return input, reconcile, nil
}
