package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) BackingProvisionCheckpoint(ctx context.Context, request *agentpb.BackingProvisionCheckpointRequest) (*agentpb.BackingProvisionCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionCheckpointRequest{
		BackingProvisionCheckpointRequest: request}}, request.GetAttempt(), "backing_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteBackingProvision(ctx context.Context, request *agentpb.BackingProvisionCompletionRequest) (*agentpb.BackingProvisionCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionCompletionRequest{
		BackingProvisionCompletionRequest: request}}, request.GetAttempt(), "backing_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionCompletionAck(), nil
}

func (exchange *nativeExchange) FailBackingProvision(ctx context.Context, request *agentpb.BackingProvisionFailureRequest) (*agentpb.BackingProvisionFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionFailureRequest{
		BackingProvisionFailureRequest: request}}, request.GetAttempt(), "backing_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionFailureAck(), nil
}

func (exchange *nativeExchange) BackingProvisionConfiguration(ctx context.Context, request *agentpb.BackingProvisionConfigurationRequest) (*agentpb.BackingProvisionConfigurationManifest, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionConfigurationRequest{
		BackingProvisionConfigurationRequest: request}}, request.GetAttempt(), "backing_configuration", request.GetStageId())
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionConfigurationManifest(), nil
}

func (exchange *nativeExchange) BackingProvisionFile(ctx context.Context, request *agentpb.BackingProvisionFileRequest) (*agentpb.BackingProvisionFileDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionFileRequest{
		BackingProvisionFileRequest: request}}, request.GetAttempt(), "backing_file", request.GetStageId()+"/"+request.GetDestination())
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionFileDelivery(), nil
}

func (exchange *nativeExchange) BackingProvisionHook(ctx context.Context, request *agentpb.BackingProvisionHookRequest) (*agentpb.BackingProvisionHookDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionHookRequest{
		BackingProvisionHookRequest: request}}, request.GetAttempt(), "backing_hook", request.GetStageId())
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionHookDelivery(), nil
}

func (exchange *nativeExchange) RecoverBackingProvision(ctx context.Context, request *agentpb.BackingProvisionRecoveryRequest) (*agentpb.BackingProvisionRecoveryAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_BackingProvisionRecoveryRequest{
		BackingProvisionRecoveryRequest: request}}, request.GetAttempt(), "backing_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetBackingProvisionRecoveryAck(), nil
}
