package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) RecoverEntryConfiguration(ctx context.Context, request *agentpb.EntryConfigurationWorkRecoveryReportRequest) (*agentpb.EntryConfigurationWorkRecoveryReportAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationWorkRecoveryReportRequest{
		EntryConfigurationWorkRecoveryReportRequest: request}}, request.GetAttempt(), "entry_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetEntryConfigurationWorkRecoveryReportAck(), nil
}

func (exchange *nativeExchange) EntryConfiguration(ctx context.Context, request *agentpb.EntryConfigurationRequest) (*agentpb.EntryConfigurationManifest, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationRequest{
		EntryConfigurationRequest: request}}, request.GetAttempt(), "entry_configuration", request.GetStageId())
	if err != nil {
		return nil, err
	}
	return response.GetEntryConfigurationManifest(), nil
}

func (exchange *nativeExchange) EntryConfigurationFile(ctx context.Context, request *agentpb.EntryConfigurationFileRequest) (*agentpb.EntryConfigurationFileDelivery, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationFileRequest{
		EntryConfigurationFileRequest: request}}, request.GetAttempt(), "entry_configuration_file", request.GetStageId()+"/"+request.GetDestination())
	if err != nil {
		return nil, err
	}
	return response.GetEntryConfigurationFileDelivery(), nil
}

func (exchange *nativeExchange) FailEntryConfiguration(ctx context.Context, request *agentpb.EntryConfigurationWorkFailureRequest) (*agentpb.EntryConfigurationWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationWorkFailureRequest{
		EntryConfigurationWorkFailureRequest: request}}, request.GetAttempt(), "entry_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetEntryConfigurationWorkFailureAck(), nil
}

func (exchange *nativeExchange) EntryConfigurationCheckpoint(ctx context.Context, request *agentpb.EntryConfigurationWorkCheckpointRequest) (*agentpb.EntryConfigurationWorkCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationWorkCheckpointRequest{
		EntryConfigurationWorkCheckpointRequest: request}}, request.GetAttempt(), "entry_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetEntryConfigurationWorkCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteEntryConfiguration(ctx context.Context, request *agentpb.EntryConfigurationWorkCompletionRequest) (*agentpb.EntryConfigurationWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EntryConfigurationWorkCompletionRequest{
		EntryConfigurationWorkCompletionRequest: request}}, request.GetAttempt(), "entry_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetEntryConfigurationWorkCompletionAck(), nil
}
