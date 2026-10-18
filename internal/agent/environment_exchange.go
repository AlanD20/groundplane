package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) ReportEnvironmentProvision(ctx context.Context, request *agentpb.EnvironmentProvisionResultRequest) (*agentpb.EnvironmentProvisionResultAck, error) {
	response, err := exchange.request(ctx, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_EnvironmentProvisionResultRequest{
		EnvironmentProvisionResultRequest: request}}, request.GetAttempt(), "environment_provision_result", "")
	if err != nil {
		return nil, err
	}
	return response.GetEnvironmentProvisionResultAck(), nil
}
