package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadStagingCapture binds source-file recovery to the resource run at the
// same revision as work progress, including after the execution has settled.
func (repository *InputRepository) LoadStagingCapture(ctx context.Context, source StagingSource) (backupruntime.BackupRunRecord, keyvalue.Condition, error) {
	var zero backupruntime.BackupRunRecord
	input := source.Input
	if input == nil || input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE ||
		uint64(source.Ordinal) >= uint64(len(input.Steps)) {
		return zero, keyvalue.Condition{}, conflict("Source staging is not a captured Backup")
	}
	key := backupruntime.BackupRunKey(source.View.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{key}, source.View.ReadRevision)
	if err != nil {
		return zero, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return zero, keyvalue.Condition{}, conflict("Backup source run is unavailable")
	}
	run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
	if err != nil {
		return zero, keyvalue.Condition{}, err
	}
	if run.TaskID != source.View.Task.Task.ID || run.EnvironmentID != input.EnvironmentId || len(run.Sources) != len(input.Steps) {
		return zero, keyvalue.Condition{}, conflict("Backup source run differs from captured work")
	}
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	for ordinal, step := range input.Steps {
		if run.Sources[ordinal].Ordinal != uint32(ordinal) {
			return zero, keyvalue.Condition{}, conflict("Backup source run order differs from captured work")
		}
		if err := backupplanning.ValidateBackupRunStepAuthority(run, run.Sources[ordinal], step, scope); err != nil {
			return zero, keyvalue.Condition{}, err
		}
	}
	return run, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}
