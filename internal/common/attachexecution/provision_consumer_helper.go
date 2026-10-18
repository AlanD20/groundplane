package attachexecution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func EncodeProvisionConsumerHelper(input ProvisionInput, progress ProvisionProgress, inspectOnly bool) (*agentpb.AttachConsumerHelperRequest, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	if progress.Validate(input.Attempt, authority) != nil || !progress.InFlight || !authority.IsConsumerCommand(progress.Completed) {
		return nil, invalid("Attach connection requires its acknowledged command boundary")
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(authority)
	if err != nil {
		return nil, invalid("Attach connection authority cannot be encoded")
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	report, err := EncodeProvisionReport(input, ProvisionReport{Progress: progress})
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachConsumerHelperRequest{
		Attempt: identity, Consumer: proto.CloneOf(input.Provision.Consumer), Authority: raw,
		Start:  &agentpb.WorkStartAck{Attempt: proto.CloneOf(identity), StartedAt: timestamppb.New(*input.Attempt.StartedAt)},
		Report: report, InspectOnly: inspectOnly,
		ProvisionAcknowledgement: &agentpb.AttachProvisionCheckpointAck{Attempt: proto.CloneOf(identity), Report: append([]byte(nil), report...)},
	}, nil
}

// The helper receives only network authority, never backing credentials or hook
// inputs. Its start grant and checkpoint must describe the same assignment.
func openProvisionConsumerHelper(ctx context.Context, request *agentpb.AttachConsumerHelperRequest) (ConsumerHelperInput, error) {
	if ctx == nil || request == nil || request.Attempt == nil || request.ProvisionAcknowledgement == nil ||
		request.Acknowledgement != nil || len(request.Recovery) != 0 || len(request.ProvisionRecovery) != 0 || executionplan.RejectUnknown(request) != nil ||
		proto.Size(request) > executionplan.MaximumPlanBytes || len(request.Authority) == 0 || len(request.Authority) > 4096 {
		return ConsumerHelperInput{}, invalid("Attach connection helper envelope is invalid")
	}
	claimed := request.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId, AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	attempt.StartedAt = &started
	authority, err := jcs.Decode[ProvisionAuthority](request.Authority)
	if err != nil || authority.Validate(attempt) != nil || authority.ConsumerSHA256 == "" {
		return ConsumerHelperInput{}, invalid("Attach connection authority is invalid")
	}
	canonical, err := json.Marshal(authority)
	if err != nil {
		return ConsumerHelperInput{}, invalid("Attach connection authority cannot be encoded")
	}
	canonical, err = jcs.Canonicalize(canonical)
	if err != nil || !bytes.Equal(canonical, request.Authority) {
		return ConsumerHelperInput{}, invalid("Attach connection authority is not canonical")
	}
	consumer := request.Consumer
	if err := validateProvisionConsumerAuthority(ctx, authority, consumer); err != nil {
		return ConsumerHelperInput{}, err
	}
	report, err := DecodeProvisionReportAuthority(attempt, authority, request.Report)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	if !report.Progress.InFlight || !authority.IsConsumerCommand(report.Progress.Completed) {
		return ConsumerHelperInput{}, invalid("Attach connection command has not been acknowledged")
	}
	acknowledged, err := workexecution.DecodeAgentAttempt(request.ProvisionAcknowledgement.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) || !bytes.Equal(request.Report, request.ProvisionAcknowledgement.Report) {
		return ConsumerHelperInput{}, invalid("Attach connection acknowledgement differs from its command")
	}
	return ConsumerHelperInput{Attempt: attempt, Consumer: proto.CloneOf(consumer), InspectOnly: request.InspectOnly}, nil
}

func validateProvisionConsumerAuthority(ctx context.Context, authority ProvisionAuthority, consumer *agentpb.AttachConsumerTransition) error {
	if err := executionplan.ValidateAttachConsumerTransition(ctx, consumer); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(consumer)
	if err != nil {
		return invalid("Attach connection input cannot be encoded")
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	if consumer.Action != agentpb.AttachNetworkAction_ATTACH_NETWORK_ACTION_CONNECT || authority.ConsumerSHA256 != hex.EncodeToString(digest[:]) ||
		authority.ConsumerNetworkID != consumer.NetworkId || authority.ConsumerServiceID != consumer.ServiceId || authority.ConsumerCandidateSHA256 != hex.EncodeToString(consumer.Candidate.YamlSha256) {
		return invalid("Attach connection differs from its captured authority")
	}
	return nil
}
