package ingressexecution

import (
	"encoding/hex"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func EncodeAssignment(attempt workexecution.Attempt, plan Plan) (*agentpb.PublicIngressAssignment, error) {
	if err := ValidateAssignment(attempt, plan); err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	addresses := make([]string, len(plan.Addresses))
	for index, address := range plan.Addresses {
		addresses[index] = address.String()
	}
	return &agentpb.PublicIngressAssignment{
		Attempt: identity, HostId: plan.HostID, TaskId: plan.TaskID, Revision: plan.Revision,
		Digest: plan.Digest, Addresses: addresses, Configuration: append([]byte(nil), plan.Configuration...),
		Predecessor: encodeReceipt(plan.Predecessor),
	}, nil
}

func DecodeAssignment(
	message *agentpb.PublicIngressAssignment,
	identity workexecution.Identity,
) (workexecution.Attempt, Plan, error) {
	if !validMessage(message, MaximumConfigurationBytes+4096) || len(message.Addresses) > 16 {
		return workexecution.Attempt{}, Plan{}, invalidTransport()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return workexecution.Attempt{}, Plan{}, err
	}
	plan := Plan{
		HostID: message.HostId, TaskID: message.TaskId, Revision: message.Revision, Digest: message.Digest,
		Addresses: make(
			[]netip.Addr,
			len(message.Addresses),
		), Configuration: append([]byte(nil), message.Configuration...),
		Predecessor: decodeReceipt(message.Predecessor),
	}
	for index, value := range message.Addresses {
		address, err := netip.ParseAddr(value)
		if err != nil || address.String() != value {
			return workexecution.Attempt{}, Plan{}, invalidTransport()
		}
		plan.Addresses[index] = address
	}
	if err := ValidateAssignment(attempt, plan); err != nil {
		return workexecution.Attempt{}, Plan{}, err
	}
	return attempt, plan, nil
}

// ValidateAssignment binds exact materialized configuration to the assigned Host and Task.
func ValidateAssignment(attempt workexecution.Attempt, plan Plan) error {
	digest, err := plan.SHA256()
	if err != nil {
		return err
	}
	if attempt.Executor.HostID != plan.HostID || attempt.TaskID != plan.TaskID || attempt.Input.SHA256 != digest {
		return errs.New(errs.KindStateConflict, "public ingress assignment does not match its authenticated input")
	}
	return nil
}

func EncodeReport(attempt workexecution.Attempt, report Report) (*agentpb.PublicIngressResultRequest, error) {
	if err := report.Validate(); err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	var outcome agentpb.PublicIngressOutcome
	switch report.Result {
	case Applied:
		outcome = agentpb.PublicIngressOutcome_PUBLIC_INGRESS_OUTCOME_APPLIED
	case Restored:
		outcome = agentpb.PublicIngressOutcome_PUBLIC_INGRESS_OUTCOME_RESTORED
	case Uncertain:
		outcome = agentpb.PublicIngressOutcome_PUBLIC_INGRESS_OUTCOME_UNCERTAIN
	}
	return &agentpb.PublicIngressResultRequest{
		Attempt: identity,
		Outcome: outcome,
		Receipt: encodeReceipt(report.Receipt),
	}, nil
}

func DecodeReport(
	message *agentpb.PublicIngressResultRequest,
	identity workexecution.Identity,
) (workexecution.Attempt, Report, error) {
	if !validMessage(message, 4096) {
		return workexecution.Attempt{}, Report{}, invalidTransport()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return workexecution.Attempt{}, Report{}, err
	}
	report := Report{Receipt: decodeReceipt(message.Receipt)}
	switch message.Outcome {
	case agentpb.PublicIngressOutcome_PUBLIC_INGRESS_OUTCOME_APPLIED:
		report.Result = Applied
	case agentpb.PublicIngressOutcome_PUBLIC_INGRESS_OUTCOME_RESTORED:
		report.Result = Restored
	case agentpb.PublicIngressOutcome_PUBLIC_INGRESS_OUTCOME_UNCERTAIN:
		report.Result = Uncertain
	}
	if err := report.Validate(); err != nil {
		return workexecution.Attempt{}, Report{}, err
	}
	return attempt, report, nil
}

func EncodeAcknowledgement(attempt workexecution.Attempt) (*agentpb.PublicIngressResultAck, error) {
	if attempt.Validate() != nil || attempt.Result == nil || attempt.FinishedAt == nil {
		return nil, invalidTransport()
	}
	receipt := attempt
	receipt.Result, receipt.FinishedAt, receipt.Outcome = nil, nil, ""
	identity, err := workexecution.EncodeAgentAttempt(receipt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, invalidTransport()
	}
	ack := &agentpb.PublicIngressResultAck{
		Attempt: identity, Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt),
		Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
	}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}

func encodeReceipt(receipt *Receipt) *agentpb.PublicIngressReceipt {
	if receipt == nil {
		return nil
	}
	return &agentpb.PublicIngressReceipt{
		RuntimeId: receipt.RuntimeID,
		Revision:  receipt.Revision,
		Digest:    receipt.Digest,
	}
}

func decodeReceipt(receipt *agentpb.PublicIngressReceipt) *Receipt {
	if receipt == nil {
		return nil
	}
	return &Receipt{RuntimeID: receipt.RuntimeId, Revision: receipt.Revision, Digest: receipt.Digest}
}

func validMessage(message proto.Message, limit int) bool {
	return message.ProtoReflect().IsValid() && proto.Size(message) <= limit &&
		executionplan.RejectUnknown(message) == nil
}

func invalidTransport() error {
	return errs.New(errs.KindValidationFailed, "public ingress transport evidence is invalid")
}
