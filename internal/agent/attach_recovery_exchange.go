package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (exchange *nativeExchange) CompleteAttachRecovery(
	ctx context.Context,
	request *agentpb.AttachRecoveryCompletionRequest,
) (*agentpb.AttachRecoveryCompletionAck, error) {
	response, err := exchange.request(
		ctx,
		&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_AttachRecoveryCompletionRequest{
			AttachRecoveryCompletionRequest: request}},
		request.GetAttempt(),
		"attach_recovery_completion",
		"",
	)
	if err != nil {
		return nil, err
	}
	return response.GetAttachRecoveryCompletionAck(), nil
}
