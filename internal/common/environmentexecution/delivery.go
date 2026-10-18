package environmentexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func EncodeAssignment(input Input) (*agentpb.EnvironmentProvisionAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	message := &agentpb.EnvironmentProvisionAssignment{Attempt: attempt, Input: proto.CloneOf(input.Creation)}
	if source := input.RecoverySource; source != nil {
		assignment := *source
		assignment.FinishedAt, assignment.Result, assignment.Outcome = nil, nil, ""
		identity, err := workexecution.EncodeAgentAttempt(assignment)
		if err != nil {
			return nil, err
		}
		evidence, err := EncodeReference(*source.Result)
		if err != nil {
			return nil, err
		}
		message.RecoverySource = &agentpb.EnvironmentProvisionRecoverySource{Attempt: identity,
			FinishedAt: timestamppb.New(*source.FinishedAt), Outcome: string(source.Outcome), Evidence: evidence}
		if source.StartedAt != nil {
			message.RecoverySource.StartedAt = timestamppb.New(*source.StartedAt)
		}
	}
	return message, nil
}

func DecodeRecoverySource(message *agentpb.EnvironmentProvisionRecoverySource) (workexecution.Attempt, error) {
	if message == nil || message.Attempt == nil || executionplan.RejectUnknown(message) != nil ||
		message.FinishedAt == nil || message.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalid("environment cleanup source receipt is invalid")
	}
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: message.Attempt.HostId,
		AgentID: message.Attempt.AgentId, Generation: message.Attempt.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	finished := message.FinishedAt.AsTime().UTC()
	attempt.FinishedAt, attempt.Outcome = &finished, workexecution.Outcome(message.Outcome)
	if message.StartedAt != nil {
		if message.StartedAt.CheckValid() != nil {
			return workexecution.Attempt{}, invalid("environment cleanup source start receipt is invalid")
		}
		started := message.StartedAt.AsTime().UTC()
		attempt.StartedAt = &started
	}
	evidence, err := DecodeReference(message.Evidence)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	attempt.Result = &evidence
	if err := attempt.Validate(); err != nil {
		return workexecution.Attempt{}, err
	}
	return attempt, nil
}

func EncodeReference(reference workexecution.Input) (*agentpb.WorkInputReference, error) {
	if err := reference.Validate(); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}, nil
}

func DecodeReference(reference *agentpb.WorkInputReference) (workexecution.Input, error) {
	if reference == nil || executionplan.RejectUnknown(reference) != nil {
		return workexecution.Input{}, invalid("environment provisioning evidence is missing")
	}
	input := workexecution.Input{ID: reference.InputId, SHA256: hex.EncodeToString(reference.Sha256)}
	return input, input.Validate()
}

func (input Input) WithStart(ack *agentpb.WorkStartAck) (Input, bool, error) {
	started, reconcile, err := workexecution.DecodeAgentStartAck(ack, input.Attempt)
	if err != nil {
		return Input{}, false, err
	}
	input.Attempt.StartedAt = &started
	return input, reconcile, nil
}

func OpenHelper(request *agentpb.EnvironmentProvisionHelperRequest) (Input, error) {
	if request == nil || request.Assignment == nil || request.Assignment.Attempt == nil ||
		executionplan.RejectUnknown(request) != nil {
		return Input{}, invalid("environment provisioning helper authority is missing")
	}
	native := request.Assignment.Attempt
	input, err := DecodeAssignment(request.Assignment, workexecution.Identity{Executor: workexecution.Agent,
		HostID: native.HostId, AgentID: native.AgentId, Generation: native.ExecutorGeneration})
	if err != nil {
		return Input{}, err
	}
	input, reconcile, err := input.WithStart(request.Start)
	if err != nil {
		return Input{}, err
	}
	if !request.InspectOnly {
		if reconcile || input.Attempt.Direction != workexecution.Forward {
			return Input{}, invalid("reconciled environment provisioning can only inspect")
		}
	}
	return input, nil
}
