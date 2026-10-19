package agentchannel

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (delivery *assignmentPayloadDelivery) prepareBackupRecoveryPayloads(
	stream agentpb.AgentChannel_ConnectServer,
	assignment *agentpb.BackupRecoveryAssignment,
	authorization agentregistration.WorkAuthorization,
	send func(agentpb.AgentChannel_ConnectServer, agentregistration.WorkAuthorization, *agentpb.BackupRecoveryAssignment, uint32, *volumeManifestExchange) error,
) (func(), error) {
	owned, attempt, _, err := executionplan.OpenBackupRecoveryAssignment(assignment, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if owned.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION ||
		len(owned.Input.Steps) != 1 || owned.Input.Steps[0].GetRestore().GetVolume() == nil {
		return func() {}, nil
	}
	binding, err := backupvolumetransfer.BindRecoverySource(owned, authorization.Identity(), 0,
		agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_OLD)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(delivery.ctx, attempt.Deadline)
	exchange := &volumeManifestExchange{ctx: ctx, slots: map[string]*volumeManifestSlot{
		binding.TransferID: {binding: binding, frames: make(chan *agentpb.BackupVolumeManifestTransfer, 1),
			credits: make(chan *agentpb.BackupVolumeManifestAckCredit, 1)},
	}}
	return delivery.launchBackupPayloads(stream, ctx, cancel, owned.Attempt, exchange, nil,
		func(stream agentpb.AgentChannel_ConnectServer) error {
			return send(stream, authorization, owned, 0, exchange)
		})
}
