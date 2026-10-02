package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) commitBackupPoint(
	ctx context.Context,
	input backupruntime.BackupCheckpointInput,
	request *agentpb.BackupCheckpointRequest,
	current etcdstore.Versioned[backupruntime.BackupRunRecord],
	next backupruntime.BackupRunRecord,
	ordinal uint32,
) (*agentpb.BackupCheckpointAck, error) {
	point, sweep, err := backupruntime.PrepareBackupPointPublication(next, ordinal, next.UpdatedAt)
	if err != nil {
		return nil, err
	}
	var orphan *etcdstore.Versioned[backupruntime.BackupOrphanRecord]
	if current.Record.Sources[ordinal].State == backupruntime.BackupSourceAttemptOrphaned {
		stored, found, err := service.repository.GetBackupOrphan(ctx, point.ID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errs.New(errs.KindStateConflict, "backup source orphan ownership is missing")
		}
		orphan = &stored
	}
	_, updated, err := service.repository.CommitBackupRecoveryPoint(
		ctx,
		input,
		current,
		next,
		ordinal,
		point,
		orphan,
		sweep,
	)
	if err != nil {
		return nil, err
	}
	if err := service.repository.FinalizeBackupCapture(ctx, input); err != nil {
		return nil, err
	}
	return backupCheckpointAcknowledgement(request, updated.Revision)
}
