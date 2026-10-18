package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) RemovalConfiguration(ctx context.Context, request *agentpb.ServiceRemovalConfigurationRequest) (*agentpb.ServiceRemovalConfigurationManifest, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceRemovalConfigurationRequest{ServiceRemovalConfigurationRequest: request}}, request.GetAttempt(), "removal_configuration", request.GetStageId())
	if err != nil {
		return nil, err
	}
	return response.GetServiceRemovalConfigurationManifest(), nil
}

func (exchange *nativeExchange) RemovalConfigurationFile(ctx context.Context, request *agentpb.ServiceRemovalConfigurationFileRequest) (*agentpb.ServiceRemovalConfigurationFileDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceRemovalConfigurationFileRequest{ServiceRemovalConfigurationFileRequest: request}}, request.GetAttempt(), "removal_configuration_file", request.GetStageId()+"/"+request.GetDestination())
	if err != nil {
		return nil, err
	}
	return response.GetServiceRemovalConfigurationFileDelivery(), nil
}

func (exchange *nativeExchange) RemovalRecoveryCheckpoint(ctx context.Context, request *agentpb.ServiceRemovalRecoveryCheckpointRequest) (*agentpb.ServiceRemovalRecoveryCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceRemovalRecoveryCheckpointRequest{
		ServiceRemovalRecoveryCheckpointRequest: request}}, request.GetAttempt(), "removal_recovery_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetServiceRemovalRecoveryCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteRemovalRecovery(ctx context.Context, request *agentpb.ServiceRemovalRecoveryCompletionRequest) (*agentpb.ServiceRemovalRecoveryCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceRemovalRecoveryCompletionRequest{
		ServiceRemovalRecoveryCompletionRequest: request}}, request.GetAttempt(), "removal_recovery_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetServiceRemovalRecoveryCompletionAck(), nil
}

func (exchange *nativeExchange) FailRemovalRecovery(ctx context.Context, request *agentpb.ServiceRemovalRecoveryFailureRequest) (*agentpb.ServiceRemovalRecoveryFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceRemovalRecoveryFailureRequest{
		ServiceRemovalRecoveryFailureRequest: request}}, request.GetAttempt(), "removal_recovery_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetServiceRemovalRecoveryFailureAck(), nil
}
