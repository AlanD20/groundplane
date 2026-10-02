package agent

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (c *Client) handleBackupStagingDelivery(
	ctx context.Context,
	stream agentStream,
	message *agentpb.ControllerMessage,
) (bool, error) {
	plan, receipt := message.GetBackupStagingRecoveryPlan(), message.GetBackupStagingRecoveryAckReceipt()
	if plan == nil && receipt == nil {
		return false, nil
	}
	if err := executionplan.RejectUnknown(message); err != nil {
		return true, err
	}
	if c.staging == nil {
		return true, invalidAgentStaging()
	}
	if plan != nil {
		ack, err := c.staging.applyPlan(ctx, c.processGeneration, plan)
		if err != nil {
			return true, err
		}
		return true, stream.Send(
			&agentpb.AgentMessage{
				Payload: &agentpb.AgentMessage_BackupStagingRecoveryAck{BackupStagingRecoveryAck: ack},
			},
		)
	}
	if !bytes.Equal(receipt.ProcessGeneration, c.processGeneration[:]) {
		return true, invalidAgentStaging()
	}
	if err := c.staging.acceptReceipt(ctx, receipt); err != nil {
		return true, err
	}
	return true, c.sendReady(stream)
}
