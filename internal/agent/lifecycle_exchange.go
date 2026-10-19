package agent

import (
	"context"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) RecoverLifecycle(
	ctx context.Context,
	request *agentpb.ServiceLifecycleRecoveryReportRequest,
) (*agentpb.ServiceLifecycleRecoveryReportAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ServiceLifecycleRecoveryReportRequest{
			ServiceLifecycleRecoveryReportRequest: request}},
		request.GetAttempt(),
		"lifecycle_recovery",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetServiceLifecycleRecoveryReportAck(), nil
}

func (exchange *nativeExchange) LifecycleCheckpoint(
	ctx context.Context,
	request *agentpb.ServiceLifecycleCheckpointRequest,
) (*agentpb.ServiceLifecycleCheckpointAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_ServiceLifecycleCheckpointRequest{
				ServiceLifecycleCheckpointRequest: request,
			},
		},
		request.GetAttempt(),
		"lifecycle_checkpoint",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetServiceLifecycleCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteLifecycle(
	ctx context.Context,
	request *agentpb.ServiceLifecycleCompletionRequest,
) (*agentpb.ServiceLifecycleCompletionAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_ServiceLifecycleCompletionRequest{
				ServiceLifecycleCompletionRequest: request,
			},
		},
		request.GetAttempt(),
		"lifecycle_completion",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetServiceLifecycleCompletionAck(), nil
}

func (exchange *nativeExchange) FailLifecycle(
	ctx context.Context,
	request *agentpb.ServiceLifecycleFailureRequest,
) (*agentpb.ServiceLifecycleFailureAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_ServiceLifecycleFailureRequest{ServiceLifecycleFailureRequest: request},
		},
		request.GetAttempt(),
		"lifecycle_failure",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetServiceLifecycleFailureAck(), nil
}

func (exchange *nativeExchange) LifecycleHook(
	ctx context.Context,
	request *agentpb.ServiceLifecycleHookRequest,
) (*agentpb.ServiceLifecycleHookDelivery, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_ServiceLifecycleHookRequest{ServiceLifecycleHookRequest: request},
		},
		request.GetAttempt(),
		"lifecycle_hook",
		request.GetStageId(),
	)
	if err != nil {
		return nil, err
	}
	return response.GetServiceLifecycleHookDelivery(), nil
}
