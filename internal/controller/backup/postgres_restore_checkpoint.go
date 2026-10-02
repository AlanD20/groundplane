package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *BackupCheckpointService) checkpointPostgresRestore(ctx context.Context,
	input backupruntime.BackupCheckpointInput,
	claim etcdstore.Versioned[taskassignments.TaskAssignmentRecord], plan *agentpb.ExecutionPlan,
) (*agentpb.BackupCheckpointAck, error) {
	current, err := service.repository.GetBackupRestore(ctx, input.TaskID)
	if err != nil {
		return nil, err
	}
	if err := service.validatePostgresCheckpointAdvance(ctx, claim, plan,
		current.Record.OperationID, current.ReadRevision, input.Request); err != nil {
		return nil, err
	}
	at := service.now().UTC()
	if !at.After(current.Record.UpdatedAt) {
		at = current.Record.UpdatedAt.Add(time.Nanosecond)
	}
	revision, err := service.repository.CheckpointPostgresRestore(ctx, input, current, at)
	if err != nil {
		return nil, err
	}
	return backupCheckpointAcknowledgement(input.Request, revision)
}
