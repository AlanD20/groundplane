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

type ConsumerHelperInput struct {
	Attempt     workexecution.Attempt
	Authority   RemovalAuthority
	Consumer    *agentpb.AttachConsumerTransition
	Progress    RemovalProgress
	InspectOnly bool
}

func EncodeConsumerHelperRequest(input RemovalInput, progress RemovalProgress, inspectOnly bool) (*agentpb.AttachConsumerHelperRequest, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	if !authority.HasConsumer || !progress.InFlight || progress.Completed != authority.ConsumerOrdinal {
		return nil, invalid("consumer helper requires its acknowledged consumer command")
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	if input.Attempt.StartedAt == nil {
		return nil, invalid("consumer helper requires its saved start grant")
	}
	raw, err := json.Marshal(authority)
	if err != nil {
		return nil, invalid("consumer authority cannot be encoded")
	}
	raw, err = jcs.Canonicalize(raw)
	if err != nil {
		return nil, err
	}
	report, err := EncodeRemovalReportAuthority(input.Attempt, authority, RemovalReport{Progress: progress})
	if err != nil {
		return nil, err
	}
	ack, err := EncodeRemovalCheckpointAck(input, RemovalReport{Progress: progress})
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachConsumerHelperRequest{Attempt: identity, Consumer: proto.CloneOf(input.Removal.Consumer),
		Authority: raw, Start: &agentpb.WorkStartAck{Attempt: proto.CloneOf(identity), StartedAt: timestamppb.New(*input.Attempt.StartedAt)},
		Report: report, Acknowledgement: ack, InspectOnly: inspectOnly}, nil
}

// OpenConsumerHelperRequest verifies the start grant and exact acknowledged
// command. It needs no adapter credentials or hook plaintext.
func OpenConsumerHelperRequest(ctx context.Context, request *agentpb.AttachConsumerHelperRequest) (ConsumerHelperInput, error) {
	if request == nil || request.Attempt == nil || proto.Size(request) > executionplan.MaximumPlanBytes ||
		executionplan.RejectUnknown(request) != nil || len(request.Authority) == 0 || len(request.Authority) > 4096 ||
		request.Acknowledgement == nil {
		return ConsumerHelperInput{}, invalid("Detach consumer helper envelope is missing or invalid")
	}
	claimed := request.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId,
		AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	if attempt.Direction != workexecution.Forward {
		return ConsumerHelperInput{}, invalid("consumer helper requires captured forward authority")
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	attempt.StartedAt = &started
	authority, err := jcs.Decode[RemovalAuthority](request.Authority)
	if err != nil || authority.Validate(attempt) != nil || !authority.HasConsumer {
		return ConsumerHelperInput{}, invalid("Detach consumer authority is invalid")
	}
	canonical, err := json.Marshal(authority)
	if err != nil {
		return ConsumerHelperInput{}, invalid("consumer authority cannot be encoded")
	}
	canonical, err = jcs.Canonicalize(canonical)
	if err != nil || !bytes.Equal(canonical, request.Authority) {
		return ConsumerHelperInput{}, invalid("consumer authority is not canonical")
	}
	if err := executionplan.ValidateAttachConsumerTransition(ctx, request.Consumer); err != nil {
		return ConsumerHelperInput{}, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request.Consumer)
	if err != nil {
		return ConsumerHelperInput{}, invalid("consumer input cannot be encoded")
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	if authority.ConsumerSHA256 != hex.EncodeToString(digest[:]) ||
		authority.ConsumerNetworkID != request.Consumer.NetworkId || authority.ConsumerServiceID != request.Consumer.ServiceId ||
		authority.ConsumerCandidateSHA256 != hex.EncodeToString(request.Consumer.Candidate.YamlSha256) {
		return ConsumerHelperInput{}, invalid("consumer input differs from captured command authority")
	}
	report, err := DecodeRemovalReportAuthority(attempt, authority, request.Report)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	if !report.Progress.InFlight || report.Progress.Completed != authority.ConsumerOrdinal {
		return ConsumerHelperInput{}, invalid("consumer helper has no in-flight command")
	}
	acknowledged, err := workexecution.DecodeAgentAttempt(request.Acknowledgement.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(attempt, acknowledged) ||
		!bytes.Equal(request.Report, request.Acknowledgement.Report) {
		return ConsumerHelperInput{}, invalid("consumer helper report differs from its acknowledgement")
	}
	return ConsumerHelperInput{Attempt: attempt, Authority: authority, Consumer: proto.CloneOf(request.Consumer),
		Progress: report.Progress, InspectOnly: request.InspectOnly}, nil
}

func ValidateConsumerHelperResponse(input ConsumerHelperInput, response *agentpb.AttachConsumerHelperResponse) error {
	if input.Consumer == nil || response == nil || response.Input == nil || response.ObservedAt == nil ||
		response.ObservedAt.CheckValid() != nil || executionplan.RejectUnknown(response) != nil ||
		response.Inspection != input.InspectOnly || response.NetworkId != input.Consumer.NetworkId ||
		response.ServiceId != input.Consumer.ServiceId || input.Attempt.StartedAt == nil ||
		response.ObservedAt.AsTime().Before(*input.Attempt.StartedAt) {
		return invalid("consumer helper response differs from its captured execution")
	}
	if (workexecution.Input{ID: response.Input.InputId, SHA256: hex.EncodeToString(response.Input.Sha256)}) != input.Attempt.Input {
		return invalid("consumer helper response belongs to a different input")
	}
	if !input.InspectOnly && !response.Detached {
		return invalid("Detach consumer helper has not proved endpoint removal")
	}
	return nil
}
