package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupStagingSession struct{ ready bool }

func (s *Server) handleBackupStagingDelivery(
	stream agentpb.AgentChannel_ConnectServer,
	session *Session,
	agentID string,
	agentGeneration uint64,
	message *agentpb.AgentMessage,
	staging *backupStagingSession,
) (bool, error) {
	inventory, acknowledgement := message.GetBackupStagingInventory(), message.GetBackupStagingRecoveryAck()
	if inventory == nil && acknowledgement == nil {
		return false, nil
	}
	if err := executionplan.RejectUnknown(message); err != nil {
		return true, err
	}
	store, ok := s.tasks.(backupStagingStore)
	if !ok {
		return true, errs.New(errs.KindInternal, "Backup staging authority store is unavailable")
	}
	process, err := session.ProcessAuthentication()
	if err != nil {
		return true, err
	}
	if inventory != nil {
		if staging.ready {
			return true, errs.New(errs.KindStateConflict, "Backup startup inventory arrived after Ready")
		}
		plan, err := s.resolveBackupStagingPlan(stream.Context(), store, process, agentID, agentGeneration, inventory)
		if err != nil {
			return true, err
		}
		return true, stream.Send(
			&agentpb.ControllerMessage{
				Payload: &agentpb.ControllerMessage_BackupStagingRecoveryPlan{BackupStagingRecoveryPlan: plan},
			},
		)
	}
	receipt, err := store.ApplyBackupStagingDelivery(
		stream.Context(),
		agentID,
		agentGeneration,
		process.ProcessGeneration,
		acknowledgement,
	)
	if err != nil {
		return true, err
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_BackupStagingRecoveryAckReceipt{BackupStagingRecoveryAckReceipt: receipt}}); err != nil {
		return true, err
	}
	staging.ready = true
	return true, nil
}
