package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) ResolverCheckpoint(ctx context.Context, request *agentpb.ResolverWorkCheckpointRequest) (*agentpb.ResolverWorkCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ResolverWorkCheckpointRequest{
		ResolverWorkCheckpointRequest: request}}, request.GetAttempt(), "resolver_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetResolverWorkCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteResolver(ctx context.Context, request *agentpb.ResolverWorkCompletionRequest) (*agentpb.ResolverWorkResultAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ResolverWorkCompletionRequest{
		ResolverWorkCompletionRequest: request}}, request.GetAttempt(), "resolver_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetResolverWorkResultAck(), nil
}

func (exchange *nativeExchange) FailResolver(ctx context.Context, request *agentpb.ResolverWorkFailureRequest) (*agentpb.ResolverWorkResultAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ResolverWorkFailureRequest{
		ResolverWorkFailureRequest: request}}, request.GetAttempt(), "resolver_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetResolverWorkResultAck(), nil
}

func (exchange *nativeExchange) RecoverResolver(ctx context.Context, request *agentpb.ResolverWorkRecoveryRequest) (*agentpb.ResolverWorkResultAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ResolverWorkRecoveryRequest{
		ResolverWorkRecoveryRequest: request}}, request.GetAttempt(), "resolver_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetResolverWorkResultAck(), nil
}

// Bound each typed payload independently; the wrapper and identity have a
// small fixed allowance, never the executable Corefile/Compose input budget.
func resolverRequestLimit(message *agentpb.AgentMessage) int {
	switch {
	case message.GetResolverWorkCompletionRequest() != nil,
		message.GetResolverWorkCheckpointRequest() != nil,
		message.GetResolverWorkRecoveryRequest() != nil:
		return resolverexecution.MaximumReportBytes + 4096
	default:
		return 20 * 1024
	}
}
