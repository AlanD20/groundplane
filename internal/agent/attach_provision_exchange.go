package agent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) StoreProvisionFacts(
	ctx context.Context,
	input attachexecution.ProvisionInput,
	output backinghook.Output,
) error {
	request, err := attachexecution.EncodeProvisionFacts(input, output)
	if err != nil {
		return err
	}
	defer attachexecution.ClearProvisionFacts(request)
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	response, err := exchange.request(
		bounded,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_AttachProvisionFactsRequest{AttachProvisionFactsRequest: request},
		},
		request.GetAttempt(),
		"attach_provision_facts",
		"",
	)
	if err != nil {
		return err
	}
	return attachexecution.ValidateProvisionFactsAck(input, request, response.GetAttachProvisionFactsAck())
}

func (exchange *nativeExchange) AttachProvisionCheckpoint(
	ctx context.Context,
	request *agentpb.AttachProvisionCheckpointRequest,
) (*agentpb.AttachProvisionCheckpointAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_AttachProvisionCheckpointRequest{AttachProvisionCheckpointRequest: request},
		},
		request.GetAttempt(),
		"attach_provision_checkpoint",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetAttachProvisionCheckpointAck(), nil
}

func (exchange *nativeExchange) CompleteAttachProvision(
	ctx context.Context,
	request *agentpb.AttachProvisionCompletionRequest,
) (*agentpb.AttachProvisionCompletionAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_AttachProvisionCompletionRequest{AttachProvisionCompletionRequest: request},
		},
		request.GetAttempt(),
		"attach_provision_completion",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetAttachProvisionCompletionAck(), nil
}

func (exchange *nativeExchange) FailAttachProvision(
	ctx context.Context,
	request *agentpb.AttachProvisionFailureRequest,
) (*agentpb.AttachProvisionFailureAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_AttachProvisionFailureRequest{AttachProvisionFailureRequest: request},
		},
		request.GetAttempt(),
		"attach_provision_failure",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetAttachProvisionFailureAck(), nil
}

func (exchange *nativeExchange) ReportProvisionRecovery(
	ctx context.Context,
	request *agentpb.AttachProvisionRecoveryReportRequest,
) (*agentpb.AttachProvisionRecoveryReportAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{
			Payload: &agentpb.AgentMessage_AttachProvisionRecoveryReportRequest{
				AttachProvisionRecoveryReportRequest: request,
			},
		},
		request.GetAttempt(),
		"attach_provision_recovery",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetAttachProvisionRecoveryReportAck(), nil
}
