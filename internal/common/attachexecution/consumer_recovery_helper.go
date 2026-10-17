package attachexecution

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func EncodeConsumerRecoveryHelperRequest(input RecoveryInput) (*agentpb.AttachConsumerHelperRequest, error) {
	if input.Validate() != nil || !input.RequiresConsumer() || input.Consumer == nil || input.Attempt.StartedAt == nil {
		return nil, invalid("consumer recovery requires its captured transition and inspection start grant")
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	source, err := EncodeRemovalRecovery(input.Source)
	if err != nil {
		return nil, err
	}
	return &agentpb.AttachConsumerHelperRequest{Attempt: identity, Consumer: proto.CloneOf(input.Consumer),
		Start:    &agentpb.WorkStartAck{Attempt: proto.CloneOf(identity), StartedAt: timestamppb.New(*input.Attempt.StartedAt)},
		Recovery: source, InspectOnly: true}, nil
}

func openConsumerRecoveryHelper(ctx context.Context, request *agentpb.AttachConsumerHelperRequest) (ConsumerHelperInput, error) {
	if request == nil || request.Attempt == nil || request.Start == nil || !request.InspectOnly ||
		len(request.Authority) != 0 || len(request.Report) != 0 || request.Acknowledgement != nil ||
		len(request.Recovery) > MaximumRemovalRecoveryBytes || proto.Size(request) > executionplan.MaximumPlanBytes ||
		executionplan.RejectUnknown(request) != nil {
		return ConsumerHelperInput{}, invalid("consumer recovery permits captured inspection only")
	}
	claimed := request.Attempt
	identity := workexecution.Identity{Executor: workexecution.Agent, HostID: claimed.HostId,
		AgentID: claimed.AgentId, Generation: claimed.ExecutorGeneration}
	attempt, err := workexecution.DecodeAgentAttempt(claimed, identity)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	attempt.StartedAt = &started
	source, err := DecodeRemovalRecovery(request.Recovery)
	if err != nil {
		return ConsumerHelperInput{}, err
	}
	input := RecoveryInput{Attempt: attempt, Source: source, Consumer: request.Consumer}
	if err := input.Validate(); err != nil {
		return ConsumerHelperInput{}, err
	}
	if !input.RequiresConsumer() || input.Consumer == nil {
		return ConsumerHelperInput{}, invalid("consumer recovery belongs to a different interrupted command")
	}
	if err := validateConsumerAuthority(ctx, source.Failure.Authority, request.Consumer); err != nil {
		return ConsumerHelperInput{}, err
	}
	forward := source.Forward
	return ConsumerHelperInput{Attempt: attempt, Forward: &forward, Authority: source.Failure.Authority,
		Consumer: proto.CloneOf(request.Consumer), InspectOnly: true}, nil
}
