package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) RecoverNetwork(ctx context.Context, request *agentpb.NetworkWorkRecoveryReportRequest) (*agentpb.NetworkWorkRecoveryReportAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_NetworkWorkRecoveryReportRequest{
		NetworkWorkRecoveryReportRequest: request}}, request.GetAttempt(), "network_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetNetworkWorkRecoveryReportAck(), nil
}

func (exchange *nativeExchange) FailNetwork(ctx context.Context, request *agentpb.NetworkWorkFailureRequest) (*agentpb.NetworkWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_NetworkWorkFailureRequest{
		NetworkWorkFailureRequest: request}}, request.GetAttempt(), "network_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetNetworkWorkFailureAck(), nil
}

func (exchange *nativeExchange) NetworkCheckpoint(ctx context.Context, request *agentpb.NetworkWorkCheckpointRequest) (*agentpb.NetworkWorkCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_NetworkWorkCheckpointRequest{
		NetworkWorkCheckpointRequest: request}}, request.GetAttempt(), "network_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetNetworkWorkCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteNetwork(ctx context.Context, request *agentpb.NetworkWorkCompletionRequest) (*agentpb.NetworkWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_NetworkWorkCompletionRequest{
		NetworkWorkCompletionRequest: request}}, request.GetAttempt(), "network_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetNetworkWorkCompletionAck(), nil
}
