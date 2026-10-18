package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) ComponentConfiguration(ctx context.Context, request *agentpb.ComponentConfigurationRequest) (*agentpb.ComponentConfigurationManifest, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ComponentConfigurationRequest{
		ComponentConfigurationRequest: request}}, request.GetAttempt(), "component_configuration", request.GetStageId())
	if err != nil {
		return nil, err
	}
	return response.GetComponentConfigurationManifest(), nil
}

func (exchange *nativeExchange) ComponentConfigurationFile(ctx context.Context, request *agentpb.ComponentConfigurationFileRequest) (*agentpb.ComponentConfigurationFileDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ComponentConfigurationFileRequest{
		ComponentConfigurationFileRequest: request}}, request.GetAttempt(), "component_configuration_file", request.GetStageId()+"/"+request.GetDestination())
	if err != nil {
		return nil, err
	}
	return response.GetComponentConfigurationFileDelivery(), nil
}

func (exchange *nativeExchange) RecoverComponent(ctx context.Context, request *agentpb.ComponentWorkRecoveryReportRequest) (*agentpb.ComponentWorkRecoveryReportAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ComponentWorkRecoveryReportRequest{
		ComponentWorkRecoveryReportRequest: request}}, request.GetAttempt(), "component_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetComponentWorkRecoveryReportAck(), nil
}

func (exchange *nativeExchange) FailComponent(ctx context.Context, request *agentpb.ComponentWorkFailureRequest) (*agentpb.ComponentWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ComponentWorkFailureRequest{
		ComponentWorkFailureRequest: request}}, request.GetAttempt(), "component_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetComponentWorkFailureAck(), nil
}

func (exchange *nativeExchange) ComponentCheckpoint(ctx context.Context, request *agentpb.ComponentWorkCheckpointRequest) (*agentpb.ComponentWorkCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ComponentWorkCheckpointRequest{
		ComponentWorkCheckpointRequest: request}}, request.GetAttempt(), "component_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetComponentWorkCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteComponent(ctx context.Context, request *agentpb.ComponentWorkCompletionRequest) (*agentpb.ComponentWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ComponentWorkCompletionRequest{
		ComponentWorkCompletionRequest: request}}, request.GetAttempt(), "component_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetComponentWorkCompletionAck(), nil
}
