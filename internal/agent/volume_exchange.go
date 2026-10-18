package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) RecoverVolume(ctx context.Context, request *agentpb.VolumeWorkRecoveryReportRequest) (*agentpb.VolumeWorkRecoveryReportAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_VolumeWorkRecoveryReportRequest{
		VolumeWorkRecoveryReportRequest: request}}, request.GetAttempt(), "volume_recovery", "")
	if err != nil {
		return nil, err
	}
	return response.GetVolumeWorkRecoveryReportAck(), nil
}

func (exchange *nativeExchange) FailVolume(ctx context.Context, request *agentpb.VolumeWorkFailureRequest) (*agentpb.VolumeWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_VolumeWorkFailureRequest{
		VolumeWorkFailureRequest: request}}, request.GetAttempt(), "volume_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetVolumeWorkFailureAck(), nil
}

func (exchange *nativeExchange) VolumeCheckpoint(ctx context.Context, request *agentpb.VolumeWorkCheckpointRequest) (*agentpb.VolumeWorkCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_VolumeWorkCheckpointRequest{
		VolumeWorkCheckpointRequest: request}}, request.GetAttempt(), "volume_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetVolumeWorkCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteVolume(ctx context.Context, request *agentpb.VolumeWorkCompletionRequest) (*agentpb.VolumeWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_VolumeWorkCompletionRequest{
		VolumeWorkCompletionRequest: request}}, request.GetAttempt(), "volume_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetVolumeWorkCompletionAck(), nil
}
