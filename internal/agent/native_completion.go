package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) FailScript(ctx context.Context, request *agentpb.ScriptWorkFailureRequest) (*agentpb.ScriptWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkFailureRequest{ScriptWorkFailureRequest: request}}, request.GetAttempt(), "script_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetScriptWorkFailureAck(), nil
}

func (exchange *nativeExchange) CompleteScriptRecovery(ctx context.Context, request *agentpb.ScriptRecoveryCompletionRequest) (*agentpb.ScriptRecoveryCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptRecoveryCompletionRequest{
		ScriptRecoveryCompletionRequest: request}}, request.GetAttempt(), "script_recovery_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetScriptRecoveryCompletionAck(), nil
}

func (exchange *nativeExchange) CompleteScript(ctx context.Context, request *agentpb.ScriptWorkCompletionRequest) (*agentpb.ScriptWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
		ScriptWorkCompletionRequest: request}}, request.GetAttempt(), "script_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetScriptWorkCompletionAck(), nil
}

func (exchange *nativeExchange) RecoveryCheckpoint(ctx context.Context, request *agentpb.ReleaseRecoveryCheckpointRequest) (*agentpb.ReleaseRecoveryCheckpointAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseRecoveryCheckpointRequest{
		ReleaseRecoveryCheckpointRequest: request}}, request.GetAttempt(), "release_recovery_checkpoint", "")
	if err != nil {
		return nil, err
	}
	return response.GetReleaseRecoveryCheckpointAck(), nil
}

func (exchange *nativeExchange) FailRelease(ctx context.Context, request *agentpb.ReleaseWorkFailureRequest) (*agentpb.ReleaseWorkFailureAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseWorkFailureRequest{
		ReleaseWorkFailureRequest: request}}, request.GetAttempt(), "release_failure", "")
	if err != nil {
		return nil, err
	}
	return response.GetReleaseWorkFailureAck(), nil
}

func (exchange *nativeExchange) CompleteRelease(ctx context.Context, request *agentpb.ReleaseWorkCompletionRequest) (*agentpb.ReleaseWorkCompletionAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ReleaseWorkCompletionRequest{
		ReleaseWorkCompletionRequest: request}}, request.GetAttempt(), "release_completion", "")
	if err != nil {
		return nil, err
	}
	return response.GetReleaseWorkCompletionAck(), nil
}
