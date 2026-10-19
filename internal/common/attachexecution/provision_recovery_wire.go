package attachexecution

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func EncodeProvisionRecoveryAssignment(
	input ProvisionRecoveryInput,
) (*agentpb.AttachProvisionRecoveryAssignment, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	if input.RequiresConsumer() && input.Consumer == nil {
		return nil, invalid("Attach recovery assignment is missing its captured network transition")
	}
	attempt, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	source, err := EncodeProvisionRecovery(input.Source)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachProvisionRecoveryAssignment{
		Attempt:  attempt,
		Recovery: source,
		Consumer: proto.CloneOf(input.Consumer),
	}, nil
}

func DecodeProvisionRecoveryAssignment(
	message *agentpb.AttachProvisionRecoveryAssignment,
	identity workexecution.Identity,
) (ProvisionRecoveryInput, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil {
		return ProvisionRecoveryInput{}, invalid("Attach provisioning recovery assignment is malformed")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return ProvisionRecoveryInput{}, err
	}
	source, err := DecodeProvisionRecovery(message.Recovery)
	if err != nil {
		return ProvisionRecoveryInput{}, err
	}
	input := ProvisionRecoveryInput{Attempt: attempt, Source: source, Consumer: proto.CloneOf(message.Consumer)}
	if input.RequiresConsumer() && input.Consumer == nil {
		return ProvisionRecoveryInput{}, invalid("Attach recovery assignment has no captured network transition")
	}
	return input, input.Validate()
}

func EncodeProvisionInspectionRequest(
	input ProvisionRecoveryInput,
	report ProvisionInspection,
) (*agentpb.AttachProvisionRecoveryReportRequest, error) {
	raw, reference, err := EncodeProvisionInspection(input, report)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(reference.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachProvisionRecoveryReportRequest{Attempt: identity, Report: raw,
		Evidence: &agentpb.WorkInputReference{InputId: reference.ID, Sha256: digest}}, nil
}

func DecodeProvisionInspectionAck(
	input ProvisionRecoveryInput,
	report ProvisionInspection,
	ack *agentpb.AttachProvisionRecoveryReportAck,
) (workexecution.Attempt, error) {
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil || ack.FinishedAt == nil ||
		ack.FinishedAt.CheckValid() != nil {
		return workexecution.Attempt{}, invalid("Attach provisioning inspection acknowledgement is malformed")
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.Attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.Attempt) {
		return workexecution.Attempt{}, invalid(
			"Attach provisioning inspection acknowledgement belongs to another attempt",
		)
	}
	_, reference, err := EncodeProvisionInspection(input, report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	provided := workexecution.Input{ID: ack.Evidence.InputId, SHA256: hex.EncodeToString(ack.Evidence.Sha256)}
	outcome := workexecution.Failed
	if report.ExecutionStopped() {
		outcome = workexecution.Succeeded
	}
	finished := ack.FinishedAt.AsTime()
	if reference != provided || ack.Outcome != string(outcome) || finished.Before(report.ObservedAt) {
		return workexecution.Attempt{}, invalid(
			"Attach provisioning inspection acknowledgement differs from the saved report",
		)
	}
	terminal := input.Attempt
	if ack.StartedAt != nil {
		if ack.StartedAt.CheckValid() != nil {
			return workexecution.Attempt{}, invalid("Attach provisioning inspection start time is invalid")
		}
		started := ack.StartedAt.AsTime()
		if terminal.StartedAt != nil && !terminal.StartedAt.Equal(started) || started.After(report.ObservedAt) {
			return workexecution.Attempt{}, invalid(
				"Attach provisioning inspection acknowledgement changed its Start time",
			)
		}
		terminal.StartedAt = &started
	} else if terminal.StartedAt != nil {
		return workexecution.Attempt{}, invalid("Attach provisioning inspection acknowledgement lost its Start time")
	}
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &reference, outcome
	return terminal, terminal.Validate()
}
